import asyncio
import inspect
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from binance import AsyncClient
from binance.exceptions import BinanceAPIException
from sqlalchemy import select

from app.database import AsyncSessionLocal, Order as DBOrder, Trade
from app.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class AsyncTradingBot:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.client: Optional[AsyncClient] = None
        self.rate_limiter = RateLimiter(requests_per_second=10)
        self._listen_key: Optional[str] = None
        self._sync_task_running = False

    async def _ensure_client(self) -> AsyncClient:
        if self.client is None:
            self.client = await AsyncClient.create(
                api_key=self.api_key,
                api_secret=self.api_secret,
                testnet=self.testnet,
            )
        return self.client

    async def _request(self, api_call: Callable[..., Any] | Any, *args, **kwargs):
        """Run one Binance API request through the shared async rate limiter."""
        await self.rate_limiter.acquire()
        try:
            result = api_call(*args, **kwargs) if callable(api_call) else api_call
            if inspect.isawaitable(result):
                return await result
            return result
        except BinanceAPIException as exc:
            if exc.code == 429:
                retry_after = int(getattr(exc, "headers", {}).get("Retry-After", 5))
                logger.warning("Rate limit hit, retrying after %ss", retry_after)
                await asyncio.sleep(retry_after)
                await self.rate_limiter.acquire()
                result = api_call(*args, **kwargs) if callable(api_call) else api_call
                if inspect.isawaitable(result):
                    return await result
                return result
            raise

    async def get_account_info(self) -> Dict:
        client = await self._ensure_client()
        return await self._request(client.futures_account)

    async def get_positions(self, symbol: str = None) -> List[Dict]:
        client = await self._ensure_client()
        params = {"symbol": symbol} if symbol else {}
        positions = await self._request(client.futures_position_information, **params)
        return positions or []

    async def get_symbol_info(self, symbol: str) -> Optional[Dict]:
        client = await self._ensure_client()
        info = await self._request(client.futures_exchange_info)
        for item in info.get("symbols", []):
            if item.get("symbol") == symbol.upper():
                return item
        return None

    async def validate_quantity(self, symbol: str, quantity: float) -> Tuple[bool, str]:
        sym_info = await self.get_symbol_info(symbol)
        if not sym_info:
            return False, f"Symbol {symbol} not found"
        for filt in sym_info.get("filters", []):
            if filt.get("filterType") == "LOT_SIZE":
                step = float(filt["stepSize"])
                min_q = float(filt["minQty"])
                max_q = float(filt["maxQty"])
                if quantity < min_q:
                    return False, f"Below min {min_q}"
                if quantity > max_q:
                    return False, f"Above max {max_q}"
                if round(quantity / step, 8) % 1 != 0:
                    return False, f"Not multiple of step {step}"
        return True, ""

    async def place_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        return await self._place_order(
            symbol=symbol,
            side=side,
            order_type="MARKET",
            quantity=quantity,
        )

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        time_in_force: str = "GTC",
    ) -> Optional[Dict]:
        return await self._place_order(
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            timeInForce=time_in_force,
        )

    async def place_stop_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_price: float,
        time_in_force: str = "GTC",
    ) -> Optional[Dict]:
        return await self._place_order(
            symbol=symbol,
            side=side,
            order_type="STOP",
            quantity=quantity,
            price=price,
            stopPrice=stop_price,
            timeInForce=time_in_force,
        )

    async def place_trailing_stop_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        callback_rate: float,
        activation_price: float = None,
    ) -> Optional[Dict]:
        params = {"callbackRate": callback_rate}
        if activation_price is not None:
            params["activationPrice"] = activation_price
        return await self._place_order(
            symbol=symbol,
            side=side,
            order_type="TRAILING_STOP_MARKET",
            quantity=quantity,
            **params,
        )

    async def place_oco_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_price: float,
        stop_limit_price: float,
    ) -> Optional[Dict]:
        """Binance USD-M Futures has no true OCO endpoint.

        This returns two linked client-side orders: a take-profit limit and a
        stop-limit. The websocket/order-sync layer can see both, but Binance
        itself will not atomically cancel the sibling if one fills.
        """
        limit_order = await self.place_limit_order(symbol, side, quantity, price)
        stop_order = await self.place_stop_limit_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=stop_limit_price,
            stop_price=stop_price,
        )
        return {
            "status": "CLIENT_SIDE_OCO_CREATED",
            "note": "Binance Futures does not support native OCO orders.",
            "orders": [order for order in (limit_order, stop_order) if order],
        }

    async def _place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        **params,
    ) -> Optional[Dict]:
        valid, err = await self.validate_quantity(symbol, quantity)
        if not valid:
            logger.error(err)
            return None

        client = await self._ensure_client()
        request_params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type,
            "quantity": quantity,
            **params,
        }
        order = await self._request(client.futures_create_order, **request_params)
        await self._save_order(order, request_params)
        await self.sync_order_status(symbol.upper(), str(order["orderId"]))
        return order

    async def _save_order(self, order: Dict, request_params: Dict) -> None:
        async with AsyncSessionLocal() as db:
            order_id = str(order["orderId"])
            existing = await db.scalar(select(DBOrder).where(DBOrder.order_id == order_id))
            if existing:
                existing.status = order.get("status", existing.status)
                existing.price = self._float(order.get("price") or request_params.get("price"))
            else:
                db.add(
                    DBOrder(
                        order_id=order_id,
                        symbol=order.get("symbol", request_params["symbol"]),
                        side=order.get("side", request_params["side"]),
                        order_type=order.get("type", request_params["type"]),
                        quantity=self._float(order.get("origQty") or request_params["quantity"]),
                        price=self._float(order.get("price") or request_params.get("price")),
                        status=order.get("status", "NEW"),
                        created_at=datetime.utcnow(),
                    )
                )
            await db.commit()

    async def get_order_status(self, symbol: str, order_id: str) -> Dict:
        client = await self._ensure_client()
        return await self._request(client.futures_get_order, symbol=symbol.upper(), orderId=order_id)

    async def get_open_orders(self, symbol: str = None) -> List[Dict]:
        client = await self._ensure_client()
        params = {"symbol": symbol.upper()} if symbol else {}
        return await self._request(client.futures_get_open_orders, **params) or []

    async def cancel_order(self, symbol: str, order_id: str) -> Dict:
        client = await self._ensure_client()
        result = await self._request(
            client.futures_cancel_order,
            symbol=symbol.upper(),
            orderId=order_id,
        )
        await self._update_order_status(str(order_id), result.get("status", "CANCELED"))
        return result

    async def get_historical_orders(self, limit: int = 100) -> List[DBOrder]:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(DBOrder).order_by(DBOrder.created_at.desc()).limit(limit)
            )
            return list(result.scalars().all())

    async def sync_order_status(self, symbol: str, order_id: str) -> Optional[Dict]:
        order = await self.get_order_status(symbol, order_id)
        status = order.get("status")
        if status:
            await self._update_order_status(str(order_id), status)
        if status == "FILLED":
            await self._save_trades_for_order(symbol, order_id, order)
        return order

    async def sync_open_order_states(self) -> None:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(DBOrder).where(DBOrder.status.in_(["NEW", "PARTIALLY_FILLED"]))
            )
            open_orders = list(result.scalars().all())

        for order in open_orders:
            try:
                await self.sync_order_status(order.symbol, order.order_id)
            except Exception:
                logger.exception("Failed to sync order %s", order.order_id)

    async def periodic_order_sync(self, interval_seconds: int = 180) -> None:
        self._sync_task_running = True
        while self._sync_task_running:
            try:
                await self.sync_open_order_states()
            except Exception:
                logger.exception("Periodic order status sync failed")
            await asyncio.sleep(interval_seconds)

    async def handle_order_trade_update(self, payload: Dict) -> None:
        order = payload.get("o", payload)
        order_id = str(order.get("i") or order.get("orderId"))
        symbol = order.get("s") or order.get("symbol")
        status = order.get("X") or order.get("status")
        if not order_id or not symbol:
            return
        if status:
            await self._update_order_status(order_id, status)
        if status == "FILLED":
            await self._save_trade_from_ws_order(order)

    async def _update_order_status(self, order_id: str, status: str) -> None:
        async with AsyncSessionLocal() as db:
            db_order = await db.scalar(select(DBOrder).where(DBOrder.order_id == str(order_id)))
            if db_order:
                db_order.status = status
                await db.commit()

    async def _save_trades_for_order(self, symbol: str, order_id: str, order: Dict) -> None:
        client = await self._ensure_client()
        trades = await self._request(
            client.futures_account_trades,
            symbol=symbol.upper(),
            orderId=order_id,
        )
        if not trades:
            await self._save_trade_snapshot(order)
            return

        async with AsyncSessionLocal() as db:
            for trade in trades:
                exists = await self._trade_exists(
                    db,
                    symbol=symbol,
                    timestamp=self._dt_from_ms(trade.get("time")),
                    quantity=self._float(trade.get("qty")),
                    price=self._float(trade.get("price")),
                )
                if exists:
                    continue
                db.add(
                    Trade(
                        symbol=symbol.upper(),
                        side=trade.get("side", order.get("side", "")),
                        quantity=self._float(trade.get("qty")),
                        price=self._float(trade.get("price")),
                        realized_pnl=self._float(trade.get("realizedPnl")),
                        commission=self._float(trade.get("commission")),
                        timestamp=self._dt_from_ms(trade.get("time")),
                    )
                )
            await db.commit()

    async def _save_trade_from_ws_order(self, order: Dict) -> None:
        timestamp = self._dt_from_ms(order.get("T") or order.get("E"))
        async with AsyncSessionLocal() as db:
            exists = await self._trade_exists(
                db,
                symbol=order.get("s", ""),
                timestamp=timestamp,
                quantity=self._float(order.get("z") or order.get("l")),
                price=self._float(order.get("ap") or order.get("L") or order.get("p")),
            )
            if exists:
                return
            db.add(
                Trade(
                    symbol=order.get("s", ""),
                    side=order.get("S", ""),
                    quantity=self._float(order.get("z") or order.get("l")),
                    price=self._float(order.get("ap") or order.get("L") or order.get("p")),
                    realized_pnl=self._float(order.get("rp")),
                    commission=self._float(order.get("n")),
                    timestamp=timestamp,
                )
            )
            await db.commit()

    async def _save_trade_snapshot(self, order: Dict) -> None:
        async with AsyncSessionLocal() as db:
            db.add(
                Trade(
                    symbol=order.get("symbol", ""),
                    side=order.get("side", ""),
                    quantity=self._float(order.get("executedQty") or order.get("origQty")),
                    price=self._float(order.get("avgPrice") or order.get("price")),
                    realized_pnl=0.0,
                    commission=0.0,
                    timestamp=datetime.utcnow(),
                )
            )
            await db.commit()

    async def get_listen_key(self) -> str:
        client = await self._ensure_client()
        if not self._listen_key:
            self._listen_key = await self._request(client.futures_stream_get_listen_key)
        return self._listen_key

    async def keep_alive_listen_key(self) -> None:
        while True:
            await asyncio.sleep(30 * 60)
            if self._listen_key:
                try:
                    client = await self._ensure_client()
                    await self._request(client.futures_stream_keepalive, self._listen_key)
                    logger.info("ListenKey keepalive sent")
                except Exception:
                    logger.exception("Failed to keep listenKey alive")

    @staticmethod
    async def _trade_exists(db, symbol: str, timestamp: datetime, quantity: float, price: float) -> bool:
        result = await db.execute(
            select(Trade).where(
                Trade.symbol == symbol.upper(),
                Trade.timestamp == timestamp,
                Trade.quantity == quantity,
                Trade.price == price,
            )
        )
        return result.scalar_one_or_none() is not None

    @staticmethod
    def _float(value, default: float = 0.0) -> float:
        if value in (None, ""):
            return default
        return float(value)

    @staticmethod
    def _dt_from_ms(value) -> datetime:
        if not value:
            return datetime.utcnow()
        return datetime.utcfromtimestamp(int(value) / 1000)
