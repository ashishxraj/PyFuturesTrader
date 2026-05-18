import asyncio
import time

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from app.auth import decode_token
from app.dependencies import get_ws_manager
from app.websocket_manager import ResilientWebSocketManager

router = APIRouter()


async def authenticate_ws(websocket: WebSocket, token: str | None) -> bool:
    if not token:
        return True
    try:
        decode_token(token)
        return True
    except Exception:
        await websocket.close(code=1008)
        return False


@router.websocket("/trade")
async def trade_websocket(
    websocket: WebSocket,
    token: str | None = Query(default=None),
    manager: ResilientWebSocketManager = Depends(get_ws_manager),
):
    if not await authenticate_ws(websocket, token):
        return
    await manager.connect(websocket)
    await manager.send_json(
        websocket,
        {"type": "connection", "status": "connected", "timestamp": int(time.time() * 1000)},
    )
    manager.create_stream_task("user_data", websocket)

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            stream_type = data.get("type")
            symbol = data.get("symbol", "").upper()
            interval = data.get("interval", "1m")

            if action == "subscribe":
                if stream_type in {"ticker", "kline", "depth"} and not symbol:
                    await manager.send_json(websocket, {"type": "error", "message": "symbol required"})
                    continue
                manager.create_stream_task(stream_type, websocket, symbol, interval)
                await manager.send_json(
                    websocket,
                    {"type": "subscribed", "stream": stream_type, "symbol": symbol, "interval": interval},
                )
            elif action == "unsubscribe":
                manager.stop_stream(stream_type, websocket, symbol, interval)
                await manager.send_json(
                    websocket,
                    {"type": "unsubscribed", "stream": stream_type, "symbol": symbol, "interval": interval},
                )
            elif action == "ping":
                await manager.send_json(websocket, {"type": "pong", "timestamp": int(time.time() * 1000)})
            else:
                await manager.send_json(websocket, {"type": "error", "message": "Unknown action"})
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


@router.websocket("/ticker/{symbol}")
async def ticker_websocket(
    websocket: WebSocket,
    symbol: str,
    token: str | None = Query(default=None),
    manager: ResilientWebSocketManager = Depends(get_ws_manager),
):
    if not await authenticate_ws(websocket, token):
        return
    await manager.connect(websocket)
    manager.create_stream_task("ticker", websocket, symbol)
    try:
        while websocket.application_state == WebSocketState.CONNECTED:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


@router.websocket("/kline/{symbol}/{interval}")
async def kline_websocket(
    websocket: WebSocket,
    symbol: str,
    interval: str,
    token: str | None = Query(default=None),
    manager: ResilientWebSocketManager = Depends(get_ws_manager),
):
    if not await authenticate_ws(websocket, token):
        return
    await manager.connect(websocket)
    manager.create_stream_task("kline", websocket, symbol, interval)
    try:
        while websocket.application_state == WebSocketState.CONNECTED:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)


@router.websocket("/depth/{symbol}")
async def depth_websocket(
    websocket: WebSocket,
    symbol: str,
    token: str | None = Query(default=None),
    manager: ResilientWebSocketManager = Depends(get_ws_manager),
):
    if not await authenticate_ws(websocket, token):
        return
    await manager.connect(websocket)
    manager.create_stream_task("depth", websocket, symbol)
    try:
        while websocket.application_state == WebSocketState.CONNECTED:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)
