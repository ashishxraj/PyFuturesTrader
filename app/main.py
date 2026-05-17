import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.auth import verify_token
from app.bot import AsyncTradingBot
from app.database import init_db
from app.routers import account, auth, orders, ws
from app.websocket_manager import ResilientWebSocketManager

from dotenv import load_dotenv
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    bot = AsyncTradingBot(
        api_key=os.getenv("BINANCE_API_KEY") or os.getenv("binance_api_key", ""),
        api_secret=os.getenv("BINANCE_SECRET") or os.getenv("binance_secret_key", ""),
        testnet=os.getenv("BINANCE_TESTNET", "true").lower() == "true",
    )
    app.state.bot = bot
    app.state.ws_manager = ResilientWebSocketManager(bot)
    app.state.background_tasks = [
        asyncio.create_task(bot.keep_alive_listen_key()),
        asyncio.create_task(bot.periodic_order_sync()),
        asyncio.create_task(app.state.ws_manager.heartbeat()),
    ]

    try:
        yield
    finally:
        for task in app.state.background_tasks:
            task.cancel()
        await asyncio.gather(*app.state.background_tasks, return_exceptions=True)
        await app.state.ws_manager.close_all_streams()
        if bot.client:
            await bot.client.close_connection()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth")
app.include_router(orders.router, prefix="/api/orders", dependencies=[Depends(verify_token)])
app.include_router(account.router, prefix="/api/account", dependencies=[Depends(verify_token)])
app.include_router(ws.router, prefix="/ws")

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
async def index():
    return FileResponse("app/static/index.html")
