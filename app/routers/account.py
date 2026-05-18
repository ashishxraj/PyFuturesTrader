from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import func, select

from app.bot import AsyncTradingBot
from app.database import AsyncSessionLocal, Trade
from app.dependencies import get_bot

router = APIRouter()


@router.get("/balance")
async def get_balance(bot: AsyncTradingBot = Depends(get_bot)):
    return {"balance": await bot.get_account_info()}


@router.get("/positions")
async def get_positions(symbol: Optional[str] = None, bot: AsyncTradingBot = Depends(get_bot)):
    return {"positions": await bot.get_positions(symbol)}


@router.get("/portfolio")
async def get_portfolio(bot: AsyncTradingBot = Depends(get_bot)):
    account = await bot.get_account_info()
    positions = await bot.get_positions()

    async with AsyncSessionLocal() as db:
        realized = await db.scalar(select(func.coalesce(func.sum(Trade.realized_pnl), 0.0)))
        commission = await db.scalar(select(func.coalesce(func.sum(Trade.commission), 0.0)))

    unrealized = sum(float(item.get("unRealizedProfit", 0) or 0) for item in positions)
    return {
        "wallet_balance": float(account.get("totalWalletBalance", 0) or 0),
        "available_balance": float(account.get("availableBalance", 0) or 0),
        "realized_pnl": float(realized or 0),
        "unrealized_pnl": unrealized,
        "commission": float(commission or 0),
        "net_pnl": float(realized or 0) + unrealized - float(commission or 0),
    }
