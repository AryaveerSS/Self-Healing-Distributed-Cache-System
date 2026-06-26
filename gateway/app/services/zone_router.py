"""
Zone Router — Phase 8: Geo-Aware Routing

Intelligently routes read requests to nearest replica based on client zone.

Patterns:
1. Writes ALWAYS go to primary (consistency)
2. Reads check if replica in same zone → serve from nearest
3. Track RTT (Round-Trip Time) to all nodes
4. After write, pin reads to primary for 500ms (read-your-writes)
"""

import time
from typing import List, Dict, Optional
import logging


logger = logging.getLogger(__name__)


class ZoneRouter:
    """
    Smart request routing based on geography and consistency requirements.
    """

    # How long to pin reads to primary after a write (ms)
    READ_YOUR_WRITES_PIN_MS = 500

    def __init__(self):
        """Initialize zone router."""
        # node_id -> last RTT in milliseconds
        self.latency_map: Dict[str, float] = {}

        # key -> (primary_node, timestamp) for read-your-writes
        # Clean up entries older than PIN_MS
        self.recent_writes: Dict[str, tuple[str, float]] = {}

    def update_latency(self, node_id: str, latency_ms: float) -> None:
        """Update RTT latency for a node (from periodic pings)."""
        # Exponential moving average: smooth out spikes
        if node_id not in self.latency_map:
            self.latency_map[node_id] = latency_ms
        else:
            # EMA: new = 0.7 * old + 0.3 * measured
            alpha = 0.3
            self.latency_map[node_id] = (
                alpha * latency_ms + (1 - alpha) * self.latency_map[node_id]
            )

    def pick_replica(
        self,
        replicas: List[str],
        client_zone: Optional[str],
        primary: str,
    ) -> str:
        """
        Pick best replica for a read request.

        Strategy:
        1. If client in same zone as a replica → use it
        2. If all replicas are far away → use lowest latency
        3. Default → use primary

        Args:
            replicas: List of replica node IDs (first is primary)
            client_zone: Client's zone (e.g., "asia-south")
            primary: Primary node ID

        Returns:
            Best node ID to read from
        """
        if not replicas:
            return primary

        if not client_zone:
            # No zone info → use latency-based
            return self._pick_by_latency(replicas)

        # TODO: In production, map node_id -> zone via cluster_manager
        # For now, use latency as proxy

        # Try to find replica in same zone
        # (This requires zone info in node metadata)

        return self._pick_by_latency(replicas)

    def _pick_by_latency(self, nodes: List[str]) -> str:
        """Pick node with lowest latency."""
        if not nodes:
            return None

        # Sort by latency (lowest first)
        # Default latency is 100ms if unknown
        sorted_nodes = sorted(nodes, key=lambda n: self.latency_map.get(n, 100.0))

        return sorted_nodes[0]

    def pin_write(
        self,
        key: str,
        primary: str,
        client_zone: str,
    ) -> None:
        """
        Record a write to enforce read-your-writes consistency.

        For 500ms after a write, all reads for this key must go to
        the primary node that handled the write.

        This solves: Client writes, then immediately reads old version
        (because replica hasn't replicated yet).
        """
        self.recent_writes[key] = (primary, time.time())
        logger.debug(f"Pinned {key} to {primary} for read-your-writes")

    def is_pinned_to_primary(self, key: str, primary: str) -> bool:
        """
        Check if key is still pinned to primary for read-your-writes.

        Returns True if write was recent enough.
        """
        if key not in self.recent_writes:
            return False

        pinned_node, write_time = self.recent_writes[key]
        if pinned_node != primary:
            return False  # Different primary

        elapsed_ms = (time.time() - write_time) * 1000
        if elapsed_ms > self.READ_YOUR_WRITES_PIN_MS:
            # Pin expired, clean up
            del self.recent_writes[key]
            return False

        return True

    def cleanup_expired_pins(self) -> int:
        """
        Clean up expired write pins.

        Called periodically to free memory.
        """
        now = time.time()
        expired = [
            k
            for k, (_, ts) in self.recent_writes.items()
            if (now - ts) * 1000 > self.READ_YOUR_WRITES_PIN_MS
        ]

        for k in expired:
            del self.recent_writes[k]

        if expired:
            logger.debug(f"Cleaned up {len(expired)} expired write pins")

        return len(expired)
