from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.bot import AsyncTradingBot
from app.dependencies import get_bot

router = APIRouter()


class OrderRequest(BaseModel):
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    stop_limit_price: Optional[float] = None
    callback_rate: Optional[float] = None
    activation_price: Optional[float] = None


class CancelRequest(BaseModel):
    symbol: str
    order_id: str


def serialize_order(order) -> dict:
    return {
        "id": order.id,
        "order_id": order.order_id,
        "symbol": order.symbol,
        "side": order.side,
        "order_type": order.order_type,
        "quantity": order.quantity,
        "price": order.price,
        "status": order.status,
        "created_at": order.created_at.isoformat() if order.created_at else None,
    }


@router.post("/place")
async def place_order(order: OrderRequest, bot: AsyncTradingBot = Depends(get_bot)):
    order_type = order.order_type.upper()
    if order_type == "MARKET":
        result = await bot.place_market_order(order.symbol, order.side, order.quantity)
    elif order_type == "LIMIT":
        if order.price is None:
            raise HTTPException(status_code=400, detail="Price required for limit orders")
        result = await bot.place_limit_order(order.symbol, order.side, order.quantity, order.price)
    elif order_type in {"STOP", "STOP_LIMIT"}:
        if order.price is None or order.stop_price is None:
            raise HTTPException(status_code=400, detail="Price and stop_price required")
        result = await bot.place_stop_limit_order(
            order.symbol,
            order.side,
            order.quantity,
            order.price,
            order.stop_price,
        )
    elif order_type in {"TRAILING_STOP", "TRAILING_STOP_MARKET"}:
        if order.callback_rate is None:
            raise HTTPException(status_code=400, detail="callback_rate required")
        result = await bot.place_trailing_stop_order(
            order.symbol,
            order.side,
            order.quantity,
            order.callback_rate,
            order.activation_price,
        )
    elif order_type == "OCO":
        if order.price is None or order.stop_price is None or order.stop_limit_price is None:
            raise HTTPException(
                status_code=400,
                detail="price, stop_price, and stop_limit_price required for client-side OCO",
            )
        result = await bot.place_oco_order(
            order.symbol,
            order.side,
            order.quantity,
            order.price,
            order.stop_price,
            order.stop_limit_price,
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported order type {order.order_type}")

    if not result:
        raise HTTPException(status_code=400, detail="Order rejected")
    return {"status": "success", "order": result}


@router.get("/open")
async def get_open_orders(symbol: Optional[str] = None, bot: AsyncTradingBot = Depends(get_bot)):
    return {"orders": await bot.get_open_orders(symbol)}


@router.post("/place_with_risk")
async def place_order_with_risk(
    symbol: str,
    side: str,
    risk_percent: float,
    entry_price: float,
    stop_loss_price: float,
    order_type: str = "MARKET",
    bot: AsyncTradingBot = Depends(get_bot),
):
    quantity = await bot.calculate_position_size(symbol, side, risk_percent, entry_price, stop_loss_price)
    if not quantity:
        raise HTTPException(status_code=400, detail="Could not calculate quantity (check balance or stop price)")
    if order_type.upper() == "MARKET":
        result = await bot.place_market_order(symbol, side, quantity)
    elif order_type.upper() == "LIMIT":
        # you'd need price from request – simplified
        raise HTTPException(status_code=400, detail="Limit order requires price")
    else:
        raise HTTPException(status_code=400, detail="Only MARKET supported for risk orders")
    return {"status": "success", "order": result, "calculated_quantity": quantity}


@router.post("/cancel")
async def cancel_order(payload: CancelRequest, bot: AsyncTradingBot = Depends(get_bot)):
    return {"status": "success", "order": await bot.cancel_order(payload.symbol, payload.order_id)}


@router.get("/history")
async def order_history(limit: int = 100, bot: AsyncTradingBot = Depends(get_bot)):
    orders = await bot.get_historical_orders(limit)
    return {"orders": [serialize_order(order) for order in orders]}
