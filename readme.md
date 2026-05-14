# PyFuturesTrader - Trading Bot

![PyFuturesTrader](https://img.shields.io/badge/PyFuturesTrader-blue)
![Python](https://img.shields.io/badge/Python-3.8%2B-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104%2B-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

An algorithmic trading platform for Binance Futures with real-time market data, risk management, and a clean, modern web interface.

## Features

### Core Trading Features
- **Multiple Order Types**: Market, Limit, Stop-Limit, and Trailing Stop orders
- **Real-time Market Data**: Live price feeds via WebSocket connections
- **Risk Management**: Position sizing based on account risk percentage
- **Portfolio Management**: Track positions, P&L, and account balance
- **Advanced Order Types**: Support for OCO (One-Cancels-Other) style orders

### Web Interface
- **Real-time Charts**: Interactive price charts with live updates
- **Responsive Design**: Modern UI built with Tailwind CSS
- **Order Management**: Place, monitor, and cancel orders directly from UI
- **Account Overview**: Real-time balance and position tracking
- **WebSocket Integration**: Live updates without page refreshes

### Security & Reliability
- **API Key Management**: Secure handling of Binance credentials
- **Rate Limiting**: Built-in protection against API rate limits
- **Error Handling**: Comprehensive logging and error recovery
- **Testnet Support**: Safe testing environment with Binance Testnet

## Prerequisites

- Python 3.8 or higher
- Binance Testnet account
- Modern web browser

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/ashishxraj/PyFuturesTrader.git
cd PyFuturesTrader
```

### 2. Set Up Virtual Environment
```bash
python -m venv venv
# On macOS / Linux
source venv/bin/activate
# On Windows
venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure API Keys
Create a `.env` file in the root directory:
```env
binance_api_key=your_testnet_api_key_here
binance_secret_key=your_testnet_api_secret_here
```

To get API keys:
1. Visit [Binance Testnet](https://testnet.binancefuture.com)
2. Register an account
3. Go to API Management
4. Create new API key with trading permissions
5. Copy the keys to your `.env` file

## 🚀 Quick Start

### 1. Start the Application
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Access the Web Interface
Open your browser and navigate to:
http://localhost:8000

### 3. Start Trading
1. **Place an Order**:
   - Select symbol (e.g., BTCUSDT)
   - Choose order type (Market/Limit/Stop-Limit)
   - Enter quantity or risk percentage
   - Click "Place Order"

2. **Monitor Positions**:
   - View open positions in real-time
   - Track unrealized P&L
   - Monitor account balance

3. **Manage Orders**:
   - View all open orders
   - Cancel orders directly from UI
   - Get real-time order updates

## Project Structure

```
cryptotrader-pro/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application entry point
│   ├── bot.py               # EnhancedTradingBot class
│   ├── test-api.py          # standalone script for testing api
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── orders.py        # Order management endpoints
│   │   ├── account.py       # Account information endpoints
│   │   └── ws.py            # WebSocket connections
│   └── static/
│       ├── index.html       # Main web interface
│       └── js/
│           └── app.js       # Frontend JavaScript
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container configuration
├── .env.example             # Environment variables template
└── README.md                # This file
```

## Testing

### 1. Test Connection
```bash
python test-api.py.py
```

### 2. Test Trading (Testnet)
1. Ensure you're using Testnet API keys
2. Place a small market order (e.g., 0.001 BTC)
3. Verify order execution and account updates
4. Monitor positions and P&L

## API Endpoints

### REST API
- `GET /api/account/balance` - Get account balance
- `GET /api/account/positions` - Get open positions
- `GET /api/orders/open` - Get open orders
- `POST /api/orders/place` - Place new order
- `POST /api/orders/cancel` - Cancel existing order

### WebSocket Endpoints
- `ws://localhost:8000/ws/trade` - Main trading WebSocket
- `ws://localhost:8000/ws/mini_ticker` - Mini ticker stream
- `ws://localhost:8000/ws/user_data` - User data stream
- `ws://localhost:8000/ws/kline/{symbol}/{interval}` - Candlestick data
- `ws://localhost:8000/ws/depth/{symbol}` - Order book depth

## 🐳 Docker Deployment

### Build Docker Image
```bash
docker build -t PyFuturesTrader .
```

### Run Container
```bash
docker run -d -p 8000:8000 --env-file .env PyFuturesTrader
```

## 🔧 Configuration

### Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `binance_api_key` | Binance Testnet API Key | Required |
| `binance_secret_key` | Binance Testnet API Secret | Required |
| `TESTNET` | Use Binance Testnet | `True` |

### Trading Parameters
- **Risk Percentage**: Configurable per order (1-5% recommended)
- **Order Types**: Market, Limit, Stop-Limit, Trailing Stop
- **Time-in-Force**: GTC (Good Till Cancelled) by default
- **Position Sizing**: Automatic calculation based on risk

## Monitoring & Logging

### Log Files
- `trading_bot.log` - Application logs
- Real-time console output

### What's Logged
- API requests and responses
- Order placement and execution
- WebSocket connections
- Error messages and exceptions

<!-- ##  Risk Management

### Built-in Protections
1. **Position Sizing**: Limits based on account balance
2. **Quantity Validation**: Ensures valid lot sizes
3. **Rate Limiting**: Prevents API throttling
4. **Error Recovery**: Automatic reconnection attempts

### Recommended Practices
1. **Start Small**: Begin with test orders on Testnet
2. **Set Stop Losses**: Always use stop orders for risk management
3. **Monitor Margin**: Keep margin usage below 50%
4. **Regular Backups**: Export trade history regularly -->

## Troubleshooting

### Common Issues

1. **API Key Errors**
   - Ensure you're using Testnet keys, not Mainnet
   - Check IP whitelist settings
   - Verify API key has trading permissions

2. **WebSocket Connection Issues**
   - Check firewall settings
   - Verify Binance WebSocket service status
   - Check network connectivity

3. **Order Rejection**
   - Verify symbol is trading on Futures
   - Check minimum quantity requirements
   - Ensure sufficient balance

### Getting Help
- Check the logs: `trading_bot.log`
- Review error messages in the web interface
- Test API connectivity with `test_connection.py`

<!-- ## 🔮 Roadmap

### Planned Features
- [ ] Strategy backtesting engine
- [ ] Paper trading mode
- [ ] Advanced charting with indicators
- [ ] Multi-exchange support
- [ ] Telegram/Email notifications
- [ ] Performance analytics dashboard
- [ ] Mobile-responsive design improvements
- [ ] API documentation with Swagger -->

## In Progress
- [x] WebSocket real-time data
- [x] Advanced order types
- [x] Risk management features
- [x] Modern web interface

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a new feature branch from `develop` (`git checkout -b feature/your-feature-name`)
3. Commit changes (`git commit -m 'Add <feature description>'`)
4. Push to branch (`git push origin feature/your-feature-name`)
5. Open a Pull Request to the `develop` branch

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Code formatting
black app/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

<!-- ## ⚠️ Disclaimer

**This software is for educational and demonstration purposes only.**

- Trade at your own risk
- Cryptocurrency trading involves substantial risk
- Past performance is not indicative of future results
- Always test thoroughly on Testnet before using real funds
- The developers are not responsible for any financial losses -->

## Acknowledgments

- [Binance API](https://binance-docs.github.io/apidocs/) for their comprehensive API
- [python-binance](https://github.com/sammchardy/python-binance) library
- [FastAPI](https://fastapi.tiangolo.com/) for the web framework
- [Tailwind CSS](https://tailwindcss.com/) for styling

## Support

For support, please:
1. Check the [Issues](https://github.com/ashishxraj/PyFuturesTrader/issues) page
2. Create a new issue with detailed description
3. Include logs and steps to reproduce

---