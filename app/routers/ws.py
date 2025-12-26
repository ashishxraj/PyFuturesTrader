# app/routers/ws.py
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import List, Dict, Optional
import json
import asyncio
import logging
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
from datetime import datetime
import time
import os


logger = logging.getLogger("WebSocketManager")


router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.binance_client: Optional[AsyncClient] = None
        self.bm: Optional[BinanceSocketManager] = None
        self.socket_tasks = {}
        self.symbol_subscriptions = {}
        self.running_streams = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket connection. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            self.disconnect(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to connection: {e}")
                disconnected.append(connection)
        
        for connection in disconnected:
            self.disconnect(connection)

    async def initialize_binance_client(self):
        """Initialize the Binance async client"""
        if self.binance_client is None:
            try:
                # Get credentials from environment variables
                api_key = os.getenv("binance_api_key", "your_testnet_api_key")
                api_secret = os.getenv("binance_secret_key", "your_testnet_api_secret")
                
                self.binance_client = await AsyncClient.create(
                    api_key=api_key, 
                    api_secret=api_secret, 
                    testnet=True
                )
                self.bm = BinanceSocketManager(self.binance_client, user_timeout=60)
                logger.info("Binance WebSocket client initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Binance client: {e}")
                raise

    async def close_binance_client(self):
        """Close the Binance client"""
        if self.binance_client:
            await self.binance_client.close_connection()
            self.binance_client = None
            self.bm = None
            logger.info("Binance WebSocket client closed")

    async def handle_ticker_stream(self, symbol: str, websocket: WebSocket):
        """Handle individual ticker stream for a symbol"""
        stream_key = f"ticker_{symbol}_{id(websocket)}"
        if stream_key in self.running_streams:
            return
            
        try:
            await self.initialize_binance_client()
            self.running_streams.add(stream_key)
            
            # Use the correct method name for futures ticker socket
            async with self.bm.symbol_ticker_socket(symbol) as stream:
                logger.info(f"Started ticker stream for {symbol}")
                while stream_key in self.running_streams:
                    try:
                        res = await stream.recv()
                        if res and res.get('e') != 'error':
                            ticker_data = {
                                "type": "ticker",
                                "symbol": res['s'],
                                "price": float(res['c']),
                                "price_change": float(res['p']),
                                "price_change_percent": float(res['P']),
                                "high": float(res['h']),
                                "low": float(res['l']),
                                "volume": float(res['v']),
                                "quote_volume": float(res['q']),
                                "timestamp": res['E'],
                                "event_time": datetime.fromtimestamp(res['E'] / 1000).isoformat()
                            }
                            await self.send_personal_message(ticker_data, websocket)
                        elif res and res.get('e') == 'error':
                            logger.error(f"Socket error for {symbol}: {res}")
                            break
                    except Exception as e:
                        logger.error(f"Error in ticker stream for {symbol}: {e}")
                        break
        except BinanceAPIException as e:
            logger.error(f"Binance API exception in ticker stream for {symbol}: {e}")
            error_msg = {"type": "error", "message": f"API error for {symbol}: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        except Exception as e:
            logger.error(f"Failed to handle ticker stream for {symbol}: {e}")
            error_msg = {"type": "error", "message": f"Failed to subscribe to {symbol}: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        finally:
            self.running_streams.discard(stream_key)

    async def handle_mini_ticker_stream(self, websocket: WebSocket):
        """Handle mini ticker stream for all symbols"""
        stream_key = f"mini_ticker_{id(websocket)}"
        if stream_key in self.running_streams:
            return
            
        try:
            await self.initialize_binance_client()
            self.running_streams.add(stream_key)
            
            # Use the updated method name
            async with self.bm.all_mini_ticker_socket() as stream:
                logger.info("Started mini ticker stream for all symbols")
                while stream_key in self.running_streams:
                    try:
                        res = await stream.recv()
                        if res and res.get('e') != 'error':
                            if isinstance(res, list):
                                for item in res:
                                    mini_ticker_data = self.format_mini_ticker_data(item)
                                    await self.send_personal_message(mini_ticker_data, websocket)
                            else:
                                mini_ticker_data = self.format_mini_ticker_data(res)
                                await self.send_personal_message(mini_ticker_data, websocket)
                        elif res and res.get('e') == 'error':
                            logger.error(f"Socket error in mini ticker: {res}")
                            break
                    except Exception as e:
                        logger.error(f"Error in mini ticker stream: {e}")
                        break
                        
        except BinanceAPIException as e:
            logger.error(f"Binance API exception in mini ticker stream: {e}")
            error_msg = {"type": "error", "message": f"API error in mini ticker: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        except Exception as e:
            logger.error(f"Failed to handle mini ticker stream: {e}")
            # Fallback to individual symbols
            await self.fallback_mini_ticker(websocket)
        finally:
            self.running_streams.discard(stream_key)

    def format_mini_ticker_data(self, res):
        """Format mini ticker data"""
        try:
            return {
                "type": "mini_ticker",
                "symbol": res['s'],
                "price": float(res['c']),
                "open": float(res['o']),
                "high": float(res['h']),
                "low": float(res['l']),
                "volume": float(res['v']),
                "quote_volume": float(res['q']),
                "timestamp": res['E'],
                "event_time": datetime.fromtimestamp(res['E'] / 1000).isoformat()
            }
        except (KeyError, ValueError) as e:
            logger.error(f"Error formatting mini ticker data: {e}")
            return {"type": "error", "message": "Invalid data format"}

    async def fallback_mini_ticker(self, websocket: WebSocket):
        """Fallback method for when mini ticker is not available"""
        top_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'SOLUSDT']
        
        for symbol in top_symbols:
            asyncio.create_task(self.handle_ticker_stream(symbol, websocket))

    async def handle_user_data_stream(self, websocket: WebSocket):
        """Handle user data stream (account updates, order updates)"""
        stream_key = f"user_data_{id(websocket)}"
        if stream_key in self.running_streams:
            return
            
        try:
            await self.initialize_binance_client()
            self.running_streams.add(stream_key)
            
            async with self.bm.user_socket() as stream:
                logger.info("Started user data stream")
                while stream_key in self.running_streams:
                    try:
                        res = await stream.recv()
                        if res and res.get('e') != 'error':
                            user_data = {
                                "type": "user_data",
                                "event_type": res['e'],
                                "event_time": res['E'],
                                "data": res
                            }
                            await self.send_personal_message(user_data, websocket)
                        elif res and res.get('e') == 'error':
                            logger.error(f"Socket error in user data: {res}")
                            break
                    except Exception as e:
                        logger.error(f"Error in user data stream: {e}")
                        break
        except BinanceAPIException as e:
            logger.error(f"Binance API exception in user data stream: {e}")
            error_msg = {"type": "error", "message": f"API error in user data: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        except Exception as e:
            logger.error(f"Failed to handle user data stream: {e}")
            error_msg = {"type": "error", "message": f"Failed to start user data stream: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        finally:
            self.running_streams.discard(stream_key)

    async def handle_kline_stream(self, symbol: str, interval: str, websocket: WebSocket):
        """Handle kline/candlestick stream"""
        stream_key = f"kline_{symbol}_{interval}_{id(websocket)}"
        if stream_key in self.running_streams:
            return
            
        try:
            await self.initialize_binance_client()
            self.running_streams.add(stream_key)
            
            async with self.bm.kline_socket(symbol, interval) as stream:
                logger.info(f"Started kline stream for {symbol} with interval {interval}")
                while stream_key in self.running_streams:
                    try:
                        res = await stream.recv()
                        if res and res.get('e') != 'error':
                            kline = res['k']
                            kline_data = {
                                "type": "kline",
                                "symbol": kline['s'],
                                "interval": kline['i'],
                                "open": float(kline['o']),
                                "high": float(kline['h']),
                                "low": float(kline['l']),
                                "close": float(kline['c']),
                                "volume": float(kline['v']),
                                "is_closed": kline['x'],
                                "event_time": kline['T'],
                                "start_time": kline['t'],
                                "end_time": kline['T']
                            }
                            await self.send_personal_message(kline_data, websocket)
                        elif res and res.get('e') == 'error':
                            logger.error(f"Socket error for {symbol} kline: {res}")
                            break
                    except Exception as e:
                        logger.error(f"Error in kline stream for {symbol}: {e}")
                        break
        except BinanceAPIException as e:
            logger.error(f"Binance API exception in kline stream for {symbol}: {e}")
            error_msg = {"type": "error", "message": f"API error for {symbol} kline: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        except Exception as e:
            logger.error(f"Failed to handle kline stream for {symbol}: {e}")
            error_msg = {"type": "error", "message": f"Failed to subscribe to kline for {symbol}: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        finally:
            self.running_streams.discard(stream_key)

    async def handle_depth_stream(self, symbol: str, websocket: WebSocket):
        """Handle order book depth stream"""
        stream_key = f"depth_{symbol}_{id(websocket)}"
        if stream_key in self.running_streams:
            return
            
        try:
            await self.initialize_binance_client()
            self.running_streams.add(stream_key)
            
            async with self.bm.depth_socket(symbol) as stream:
                logger.info(f"Started depth stream for {symbol}")
                while stream_key in self.running_streams:
                    try:
                        res = await stream.recv()
                        if res and res.get('e') != 'error':
                            depth_data = {
                                "type": "depth",
                                "symbol": res['s'],
                                "event_time": res['E'],
                                "bids": [[float(price), float(quantity)] for price, quantity in res['b']],
                                "asks": [[float(price), float(quantity)] for price, quantity in res['a']]
                            }
                            await self.send_personal_message(depth_data, websocket)
                        elif res and res.get('e') == 'error':
                            logger.error(f"Socket error for {symbol} depth: {res}")
                            break
                    except Exception as e:
                        logger.error(f"Error in depth stream for {symbol}: {e}")
                        break
        except BinanceAPIException as e:
            logger.error(f"Binance API exception in depth stream for {symbol}: {e}")
            error_msg = {"type": "error", "message": f"API error for {symbol} depth: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        except Exception as e:
            logger.error(f"Failed to handle depth stream for {symbol}: {e}")
            error_msg = {"type": "error", "message": f"Failed to subscribe to depth for {symbol}: {str(e)}"}
            await self.send_personal_message(error_msg, websocket)
        finally:
            self.running_streams.discard(stream_key)

    def stop_stream(self, websocket: WebSocket, stream_type: str = None, symbol: str = None):
        """Stop specific streams for a websocket"""
        if stream_type and symbol:
            stream_key = f"{stream_type}_{symbol}_{id(websocket)}"
            self.running_streams.discard(stream_key)
        else:
            # Stop all streams for this websocket
            websocket_id = id(websocket)
            keys_to_remove = [key for key in self.running_streams if key.endswith(f"_{websocket_id}")]
            for key in keys_to_remove:
                self.running_streams.discard(key)


manager = ConnectionManager()


@router.websocket("/ticker/{symbol}")
async def symbol_ticker_websocket(websocket: WebSocket, symbol: str):
    """WebSocket endpoint for individual symbol ticker"""
    await manager.connect(websocket)
    try:
        await manager.handle_ticker_stream(symbol.upper(), websocket)
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for ticker {symbol}")
    except Exception as e:
        logger.error(f"Error in symbol ticker WebSocket: {e}")
    finally:
        manager.stop_stream(websocket, "ticker", symbol.upper())
        manager.disconnect(websocket)


@router.websocket("/mini_ticker")
async def mini_ticker_websocket(websocket: WebSocket):
    """WebSocket endpoint for all symbols mini ticker"""
    await manager.connect(websocket)
    try:
        await manager.handle_mini_ticker_stream(websocket)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for mini ticker")
    except Exception as e:
        logger.error(f"Error in mini ticker WebSocket: {e}")
    finally:
        manager.stop_stream(websocket, "mini_ticker")
        manager.disconnect(websocket)


@router.websocket("/user_data")
async def user_data_websocket(websocket: WebSocket):
    """WebSocket endpoint for user data (account and order updates)"""
    await manager.connect(websocket)
    try:
        await manager.handle_user_data_stream(websocket)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for user data")
    except Exception as e:
        logger.error(f"Error in user data WebSocket: {e}")
    finally:
        manager.stop_stream(websocket, "user_data")
        manager.disconnect(websocket)


@router.websocket("/kline/{symbol}/{interval}")
async def kline_websocket(websocket: WebSocket, symbol: str, interval: str):
    """WebSocket endpoint for kline/candlestick data"""
    await manager.connect(websocket)
    try:
        await manager.handle_kline_stream(symbol.upper(), interval, websocket)
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for kline {symbol}")
    except Exception as e:
        logger.error(f"Error in kline WebSocket: {e}")
    finally:
        manager.stop_stream(websocket, "kline", f"{symbol.upper()}_{interval}")
        manager.disconnect(websocket)


@router.websocket("/depth/{symbol}")
async def depth_websocket(websocket: WebSocket, symbol: str):
    """WebSocket endpoint for order book depth data"""
    await manager.connect(websocket)
    try:
        await manager.handle_depth_stream(symbol.upper(), websocket)
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for depth {symbol}")
    except Exception as e:
        logger.error(f"Error in depth WebSocket: {e}")
    finally:
        manager.stop_stream(websocket, "depth", symbol.upper())
        manager.disconnect(websocket)


@router.websocket("/trade")
async def trade_websocket(websocket: WebSocket):
    """Main WebSocket endpoint that handles multiple types of subscriptions"""
    await manager.connect(websocket)
    try:
        # Send connection confirmation
        await websocket.send_json({
            "type": "connection",
            "status": "connected",
            "timestamp": int(time.time() * 1000)
        })
        
        while True:
            try:
                # Receive subscription requests from client with timeout
                data = await asyncio.wait_for(websocket.receive_json(), timeout=30.0)
                
                action = data.get("action")
                if action == "subscribe":
                    stream_type = data.get("type")
                    symbol = data.get("symbol", "").upper()
                    interval = data.get("interval", "1m")
                    
                    if stream_type == "ticker" and symbol:
                        asyncio.create_task(manager.handle_ticker_stream(symbol, websocket))
                    elif stream_type == "kline" and symbol:
                        asyncio.create_task(manager.handle_kline_stream(symbol, interval, websocket))
                    elif stream_type == "depth" and symbol:
                        asyncio.create_task(manager.handle_depth_stream(symbol, websocket))
                    elif stream_type == "user_data":
                        asyncio.create_task(manager.handle_user_data_stream(websocket))
                    elif stream_type == "mini_ticker":
                        asyncio.create_task(manager.handle_mini_ticker_stream(websocket))
                    else:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Invalid subscription request: {data}"
                        })
                
                elif action == "unsubscribe":
                    stream_type = data.get("type")
                    symbol = data.get("symbol", "").upper()
                    manager.stop_stream(websocket, stream_type, symbol)
                
                elif action == "ping":
                    await websocket.send_json({
                        "type": "pong",
                        "timestamp": int(time.time() * 1000)
                    })
                    
            except asyncio.TimeoutError:
                # Send heartbeat
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": int(time.time() * 1000)
                })
                continue
                
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for trade endpoint")
    except Exception as e:
        logger.error(f"Error in trade WebSocket: {e}")
    finally:
        manager.stop_stream(websocket)
        manager.disconnect(websocket)


@router.on_event("shutdown")
async def shutdown_event():
    """Clean up WebSocket connections on shutdown"""
    await manager.close_binance_client()
    logger.info("WebSocket manager shutdown complete")
