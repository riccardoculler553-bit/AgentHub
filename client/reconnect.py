"""ReconnectManager: exponential backoff with jitter.

Never a fixed interval - a fleet of devices that all reconnect on the same
fixed 3s timer would stampede the server after every restart.
"""

import asyncio
import random

BASE_DELAYS = [1, 2, 4, 8, 16, 30]
MAX_DELAY = 30.0


class ReconnectManager:
    def __init__(self, max_delay: float = MAX_DELAY) -> None:
        self.max_delay = max_delay
        self.attempt = 0

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        """Pure delay computation (unit-testable): exponential series + jitter."""
        index = min(self.attempt, len(BASE_DELAYS) - 1)
        base = float(BASE_DELAYS[index])
        delay = min(base, self.max_delay)
        jitter = random.uniform(0, delay * 0.25)
        self.attempt += 1
        return delay + jitter

    async def wait(self) -> float:
        delay = self.next_delay()
        await asyncio.sleep(delay)
        return delay
