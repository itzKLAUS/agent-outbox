"""Synchronous dispatch boundary for trusted adapters."""

from collections.abc import Callable
from typing import Any

from .store import Lease, Outbox, _integer


class RetryableError(Exception):
    """Adapter reports an ambiguous, retryable transport failure."""


class Worker:
    def __init__(self, outbox: Outbox, name: str, adapter: Callable[[Lease], Any]):
        self.outbox = outbox
        self.name = name
        self.adapter = adapter

    def step(self, *, lease_ms: int = 30_000, delay_ms: int = 1_000) -> bool:
        """Execute one claimed intent. Return False if no intent is ready.

        RetryableError and other ordinary exceptions are conservatively ambiguous;
        unknown external success can only be retried in retry_safe mode. Process
        exits/cancellation leave the lease for recovery. Adapter results are hashed,
        never persisted in full. A result that cannot be hashed leaves the lease
        for recovery because the external side effect might already have happened.
        """
        _integer(delay_ms, "delay_ms")
        lease = self.outbox.claim(self.name, lease_ms=lease_ms)
        if lease is None:
            return False
        try:
            result = self.adapter(lease)
        except RetryableError:
            self.outbox.retry(lease, delay_ms=delay_ms)
        except Exception:
            self.outbox.retry(lease, delay_ms=delay_ms)
            raise
        else:
            self.outbox.succeed(lease, result)
        return True
