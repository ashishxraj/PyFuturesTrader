import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import structlog

from app.auth import verify_token
from app.bot import AsyncTradingBot
from app.database import init_db
from app.routers import account, auth, orders, ws
from app.websocket_manager import ResilientWebSocketManager
from dotenv import load_dotenv

load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    bot = AsyncTradingBot(
        api_key=os.getenv("binance_api_key") or "",
        api_secret=os.getenv("binance_secret_key") or "",
        testnet=os.getenv("binance_testnet", "true").lower() == "true",
    )
    app.state.bot = bot
    app.state.ws_manager = ResilientWebSocketManager(bot)

    # Start background tasks
    tasks = [
        asyncio.create_task(bot.keep_alive_listen_key()),
        asyncio.create_task(bot.periodic_order_sync()),
        asyncio.create_task(app.state.ws_manager.heartbeat()),
    ]
    app.state.background_tasks = tasks

    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await app.state.ws_manager.close_all_streams()
        if bot.client:
            await bot.client.close_connection()


app = FastAPI(lifespan=lifespan)

# CORS – more restrictive for production
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(auth.router, prefix="/api/auth")
app.include_router(orders.router, prefix="/api/orders", dependencies=[Depends(verify_token)])
app.include_router(account.router, prefix="/api/account", dependencies=[Depends(verify_token)])
app.include_router(ws.router, prefix="/ws")

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
async def index():
    return FileResponse("app/static/index.html")


@app.get("/health")
async def health():
    """Healthcheck endpoint."""
    bot_ok = app.state.bot.client is not None
    return JSONResponse(
        content={"status": "ok", "bot_client_connected": bot_ok},
        status_code=200 if bot_ok else 503,
    )