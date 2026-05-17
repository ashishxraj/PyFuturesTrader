import asyncio
import logging
import time
from typing import Dict, Set

from fastapi import WebSocket
from starlette.websockets import WebSocketState
from binance import BinanceSocketManager

from app.bot import AsyncTradingBot

logger = logging.getLogger(__name__)


class ResilientWebSocketManager:
    def __init__(self, bot: AsyncTradingBot):
        self.bot = bot
        self.active_connections: Set[WebSocket] = set()
        self.tasks: Dict[str, asyncio.Task] = {}
        self._closing = False

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        self.stop_streams_for(websocket)

    async def send_json(self, websocket: WebSocket, message: dict):
        if websocket.application_state != WebSocketState.CONNECTED:
            self.disconnect(websocket)
            return
        try:
            await websocket.send_json(message)
        except Exception:
            logger.exception("Failed to send websocket message")
            self.disconnect(websocket)

    async def start_user_stream(self, websocket: WebSocket):
        await self._run_stream(
            websocket=websocket,
            stream_key=f"user_data:{id(websocket)}",
            stream_factory=lambda bm: bm.futures_user_socket(),
            formatter=self._format_user_data,
        )

    async def start_ticker_stream(self, symbol: str, websocket: WebSocket):
        symbol = symbol.upper()
        await self._run_stream(
            websocket=websocket,
            stream_key=f"ticker:{symbol}:{id(websocket)}",
            stream_factory=lambda bm: bm.symbol_ticker_futures_socket(symbol),
            formatter=lambda msg: self._format_ticker(symbol, msg),
        )

    async def start_kline_stream(self, symbol: str, interval: str, websocket: WebSocket):
        symbol = symbol.upper()
        await self._run_stream(
            websocket=websocket,
            stream_key=f"kline:{symbol}:{interval}:{id(websocket)}",
            stream_factory=lambda bm: bm.kline_futures_socket(symbol, interval),
            formatter=lambda msg: self._format_kline(symbol, interval, msg),
        )

    async def start_depth_stream(self, symbol: str, websocket: WebSocket):
        symbol = symbol.upper()
        await self._run_stream(
            websocket=websocket,
            stream_key=f"depth:{symbol}:{id(websocket)}",
            stream_factory=lambda bm: bm.futures_depth_socket(symbol),
            formatter=lambda msg: self._format_depth(symbol, msg),
        )

    def create_stream_task(self, stream_type: str, websocket: WebSocket, symbol: str = "", interval: str = "1m"):
        symbol = symbol.upper()
        key = self._task_key(stream_type, websocket, symbol, interval)
        if key in self.tasks and not self.tasks[key].done():
            return

        if stream_type == "ticker":
            task = asyncio.create_task(self.start_ticker_stream(symbol, websocket))
        elif stream_type == "kline":
            task = asyncio.create_task(self.start_kline_stream(symbol, interval, websocket))
        elif stream_type == "depth":
            task = asyncio.create_task(self.start_depth_stream(symbol, websocket))
        elif stream_type == "user_data":
            task = asyncio.create_task(self.start_user_stream(websocket))
        else:
            raise ValueError(f"Unknown stream type: {stream_type}")

        self.tasks[key] = task
        task.add_done_callback(lambda _: self.tasks.pop(key, None))

    def stop_stream(self, stream_type: str, websocket: WebSocket, symbol: str = "", interval: str = "1m"):
        key = self._task_key(stream_type, websocket, symbol.upper(), interval)
        task = self.tasks.pop(key, None)
        if task:
            task.cancel()

    def stop_streams_for(self, websocket: WebSocket):
        suffix = f":{id(websocket)}"
        for key, task in list(self.tasks.items()):
            if key.endswith(suffix) or f":{id(websocket)}:" in key:
                task.cancel()
                self.tasks.pop(key, None)

    async def close_all_streams(self):
        self._closing = True
        for task in list(self.tasks.values()):
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.tasks.clear()
        for websocket in list(self.active_connections):
            try:
                await websocket.close()
            except Exception:
                pass
        self.active_connections.clear()

    async def heartbeat(self, interval_seconds: int = 30):
        while not self._closing:
            await asyncio.sleep(interval_seconds)
            for websocket in list(self.active_connections):
                await self.send_json(
                    websocket,
                    {"type": "heartbeat", "timestamp": int(time.time() * 1000)},
                )

    async def _run_stream(self, websocket: WebSocket, stream_key: str, stream_factory, formatter):
        reconnect_delay = 1
        while not self._closing and websocket in self.active_connections:
            try:
                client = await self.bot._ensure_client()
                bm = BinanceSocketManager(client, user_timeout=60)
                async with stream_factory(bm) as stream:
                    reconnect_delay = 1
                    while websocket in self.active_connections:
                        msg = await stream.recv()
                        if msg and msg.get("e") == "error":
                            raise RuntimeError(msg.get("m", "Binance stream error"))
                        formatted = formatter(msg)
                        if formatted:
                            await self.send_json(websocket, formatted)
                            if formatted.get("event_type") == "ORDER_TRADE_UPDATE":
                                await self.bot.handle_order_trade_update(formatted["data"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("%s disconnected: %s", stream_key, exc)
                await self.send_json(
                    websocket,
                    {"type": "stream_reconnect", "stream": stream_key, "delay": reconnect_delay},
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    @staticmethod
    def _format_user_data(msg: dict) -> dict:
        return {
            "type": "user_data",
            "event_type": msg.get("e"),
            "event_time": msg.get("E"),
            "data": msg,
        }

    @staticmethod
    def _format_ticker(symbol: str, msg: dict) -> dict:
        return {
            "type": "ticker",
            "symbol": msg.get("s", symbol),
            "price": float(msg.get("c", 0) or 0),
            "price_change": float(msg.get("p", 0) or 0),
            "price_change_percent": float(msg.get("P", 0) or 0),
            "high": float(msg.get("h", 0) or 0),
            "low": float(msg.get("l", 0) or 0),
            "volume": float(msg.get("v", 0) or 0),
            "quote_volume": float(msg.get("q", 0) or 0),
            "timestamp": msg.get("E", int(time.time() * 1000)),
        }

    @staticmethod
    def _format_kline(symbol: str, interval: str, msg: dict) -> dict:
        kline = msg.get("k", {})
        return {
            "type": "kline",
            "symbol": kline.get("s", symbol),
            "interval": kline.get("i", interval),
            "open": float(kline.get("o", 0) or 0),
            "high": float(kline.get("h", 0) or 0),
            "low": float(kline.get("l", 0) or 0),
            "close": float(kline.get("c", 0) or 0),
            "volume": float(kline.get("v", 0) or 0),
            "is_closed": bool(kline.get("x", False)),
            "start_time": kline.get("t"),
            "end_time": kline.get("T"),
        }

    @staticmethod
    def _format_depth(symbol: str, msg: dict) -> dict:
        return {
            "type": "depth",
            "symbol": msg.get("s", symbol),
            "event_time": msg.get("E"),
            "bids": [[float(price), float(qty)] for price, qty in msg.get("b", [])],
            "asks": [[float(price), float(qty)] for price, qty in msg.get("a", [])],
        }

    @staticmethod
    def _task_key(stream_type: str, websocket: WebSocket, symbol: str = "", interval: str = "1m") -> str:
        if stream_type == "kline":
            return f"{stream_type}:{symbol}:{interval}:{id(websocket)}"
        if symbol:
            return f"{stream_type}:{symbol}:{id(websocket)}"
        return f"{stream_type}:{id(websocket)}"
