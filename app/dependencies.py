from starlette.requests import HTTPConnection

from app.bot import AsyncTradingBot
from app.websocket_manager import ResilientWebSocketManager


def get_bot(connection: HTTPConnection) -> AsyncTradingBot:
    return connection.app.state.bot


def get_ws_manager(connection: HTTPConnection) -> ResilientWebSocketManager:
    return connection.app.state.ws_manager
