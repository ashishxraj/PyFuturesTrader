import pytest
from unittest.mock import AsyncMock, patch
from app.bot import AsyncTradingBot

@pytest.mark.asyncio
async def test_validate_quantity():
    bot = AsyncTradingBot("key", "secret")
    with patch.object(bot, 'get_symbol_info', return_value={
        'filters': [{'filterType': 'LOT_SIZE', 'stepSize': '0.001', 'minQty': '0.001', 'maxQty': '100'}]
    }):
        valid, err = await bot.validate_quantity("BTCUSDT", 0.002)
        assert valid is True
        valid, err = await bot.validate_quantity("BTCUSDT", 0.0005)
        assert valid is False