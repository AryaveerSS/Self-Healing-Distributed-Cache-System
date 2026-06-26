"""
Circuit Breaker — Phase 6

Prevents cascading failures by stopping requests to unhealthy nodes.

States:
- CLOSED: Normal operation, requests go through
- OPEN: Too many failures, requests rejected immediately (fail-fast)
- HALF_OPEN: Testing if node recovered, one probe request allowed
"""

import time
from typing import Dict, Optional
from enum import Enum
import logging


logger = logging.getLogger(__name__)


class BreakerState(str, Enum):
    """Circuit breaker state machine."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing — reject immediately
    HALF_OPEN = "half_open"  # Testing recovery


class NodeCircuit:
    """Per-node circuit state."""

    __slots__ = (
        "node_id",
        "state",
        "failure_count",
        "last_failure_time",
        "last_success_time",
    )

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.state = BreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = time.time()
        self.last_success_time = time.time()


class CircuitBreaker:
    """
    Per-node circuit breaker.

    Usage:
        cb = CircuitBreaker(failure_threshold=5, timeout_s=30)

        if cb.is_available(node_id):
            response = await call_node(node_id)
            cb.record_success(node_id)
        else:
            raise error immediately   # fail-fast, no timeout wait
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_s: int = 30,
    ):
        self.failure_threshold = failure_threshold
        self.timeout_s = timeout_s
        self.states: Dict[str, NodeCircuit] = {}

    def _get(self, node_id: str) -> NodeCircuit:
        if node_id not in self.states:
            self.states[node_id] = NodeCircuit(node_id)
        return self.states[node_id]

    def record_success(self, node_id: str) -> None:
        c = self._get(node_id)
        c.last_success_time = time.time()
        if c.state in (BreakerState.HALF_OPEN, BreakerState.OPEN):
            logger.info(f"Circuit CLOSED for {node_id} (recovered)")
        c.state = BreakerState.CLOSED
        c.failure_count = 0

    def record_failure(self, node_id: str) -> None:
        c = self._get(node_id)
        c.failure_count += 1
        c.last_failure_time = time.time()

        if c.failure_count >= self.failure_threshold and c.state != BreakerState.OPEN:
            logger.warning(f"Circuit OPEN for {node_id} ({c.failure_count} failures)")
            c.state = BreakerState.OPEN

    def is_available(self, node_id: str) -> bool:
        c = self._get(node_id)

        if c.state == BreakerState.CLOSED:
            return True

        if c.state == BreakerState.OPEN:
            # Check if cooldown expired → try HALF_OPEN
            if (time.time() - c.last_failure_time) >= self.timeout_s:
                logger.info(f"Circuit HALF_OPEN for {node_id}")
                c.state = BreakerState.HALF_OPEN
                c.failure_count = 0
                return True
            return False

        # HALF_OPEN — allow one probe
        return True

    def get_status(self, node_id: str) -> dict:
        c = self._get(node_id)
        return {
            "node_id": node_id,
            "state": c.state,
            "failures": c.failure_count,
            "last_failure_time": c.last_failure_time,
        }
