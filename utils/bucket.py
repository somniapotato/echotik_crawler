
import time


class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_time = time.time()

    def consume(self, amount=1):
        now = time.time()
        elapsed = now - self.last_time
        self.tokens += elapsed * self.rate
        self.tokens = min(self.tokens, self.capacity)
        self.last_time = now
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False
