import asyncio
import time

class RateLimiter:
    def __init__(self, requests_per_second: float):
        self.rate = requests_per_second
        self.period = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            sleep_time = self.last_request_time + self.period - now
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            self.last_request_time = time.monotonic()