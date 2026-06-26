"""
Cluster Manager — Phase 3-4

Core responsibilities:
1. Track all nodes and their health status
2. Maintain consistent hash ring
3. Detect node failures via heartbeat timeout
4. Trigger failover and rebalancing
5. Route requests to appropriate nodes
"""

import time
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging

from app.hash_ring import HashRing
from app.models.models import NodeStatus, NodeInfo


logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Node health states for state machine."""

    ALIVE = "alive"
    SUSPECT = "suspect"  # missed one or two heartbeats
    DEAD = "dead"  # missed 3+ heartbeats
    RECOVERED = "recovered"  # was dead, now responding again


@dataclass
class NodeMetadata:
    """In-memory state for a single node."""

    node_id: str
    host: str
    port: int
    zone: str
    status: HealthStatus = HealthStatus.ALIVE
    last_heartbeat: float = field(default_factory=time.time)
    memory_bytes: int = 0
    cache_size: int = 0
    cache_capacity: int = 0
    hit_ratio: float = 0.0
    failed_attempts: int = 0  # consecutive heartbeat misses
    uptime_s: int = 0


class ClusterManager:
    """
    Manages the distributed cluster state.

    Key design patterns:
    - Single source of truth for cluster topology
    - Async-friendly for concurrent operations
    - Thread-safe via lock (production: use distributed consensus)
    """

    # Heartbeat failure thresholds
    HEARTBEAT_TIMEOUT_S = 15  # mark as SUSPECT after this
    DEAD_THRESHOLD = 3  # mark as DEAD after N missed heartbeats

    def __init__(
        self,
        heartbeat_timeout_s: int = HEARTBEAT_TIMEOUT_S,
        replication_factor: int = 3,
    ):
        """Initialize cluster manager."""
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.replication_factor = replication_factor

        self.nodes: Dict[str, NodeMetadata] = {}
        self.hash_ring: Optional[HashRing] = None

        self.metrics_log: List[Dict] = []  # for debugging
        self.event_log: List[Dict] = []  # cluster events

    # =========================================================
    # Node Registration
    # =========================================================

    def register_node(
        self,
        node_id: str,
        host: str,
        port: int,
        zone: str,
        cache_capacity: int,
    ) -> None:
        """
        Register a new node with the cluster.

        Called once when node starts (from /register endpoint).
        """
        if node_id in self.nodes:
            logger.warning(f"Node {node_id} already registered, updating...")

        self.nodes[node_id] = NodeMetadata(
            node_id=node_id,
            host=host,
            port=port,
            zone=zone,
        )

        # Rebuild hash ring with new node list
        self._rebuild_hash_ring()

        self._log_event(
            "node_registered",
            {
                "node_id": node_id,
                "zone": zone,
                "total_nodes": len(self.nodes),
            },
        )

        logger.info(f"Registered node {node_id} in zone {zone}")

    def deregister_node(self, node_id: str) -> None:
        """
        Remove a node from cluster (graceful shutdown).
        """
        if node_id not in self.nodes:
            return

        del self.nodes[node_id]
        self._rebuild_hash_ring()

        self._log_event(
            "node_deregistered",
            {
                "node_id": node_id,
                "total_nodes": len(self.nodes),
            },
        )

        logger.info(f"Deregistered node {node_id}")

    # =========================================================
    # Heartbeat Processing
    # =========================================================

    def process_heartbeat(
        self,
        node_id: str,
        metrics: Dict,
    ) -> None:
        """
        Process incoming heartbeat from a cache node.

        Called by the heartbeat endpoint when a node POSTs health info.
        Updates node state and potentially triggers failover.
        """
        if node_id not in self.nodes:
            logger.warning(f"Heartbeat from unknown node {node_id}")
            return

        node = self.nodes[node_id]
        now = time.time()

        # Update metrics
        node.last_heartbeat = now
        node.memory_bytes = metrics.get("memory_bytes", 0)
        node.cache_size = metrics.get("cache_size", 0)
        node.hit_ratio = metrics.get("hit_ratio", 0.0)
        node.uptime_s = metrics.get("uptime_s", 0)
        node.failed_attempts = 0  # reset counter

        # State transition
        old_status = node.status
        if node.status == HealthStatus.DEAD:
            node.status = HealthStatus.RECOVERED
            logger.warning(f"Node {node_id} recovered!")
            self._log_event("node_recovered", {"node_id": node_id})
        elif node.status == HealthStatus.SUSPECT:
            node.status = HealthStatus.ALIVE
            logger.info(f"Node {node_id} returned to ALIVE")

        # Log if status changed
        if old_status != node.status:
            self._rebuild_hash_ring()  # topology changed

    def check_heartbeat_timeouts(self) -> List[str]:
        """
        Scan all nodes for heartbeat timeouts.
        Called every 3 seconds by the background loop in main.py.
        Returns list of nodes that just became DEAD.
        """
        now = time.time()
        newly_dead: List[str] = []

        for node_id, node in self.nodes.items():
            if node.status == HealthStatus.DEAD:
                continue

            time_since_heartbeat = now - node.last_heartbeat

            if time_since_heartbeat > self.heartbeat_timeout_s:
                old_status = node.status  # capture BEFORE modifying
                node.failed_attempts += 1

                if node.failed_attempts >= self.DEAD_THRESHOLD:
                    node.status = HealthStatus.DEAD
                    newly_dead.append(node_id)
                    logger.error(
                        f"Node {node_id} marked DEAD "
                        f"(no heartbeat for {time_since_heartbeat:.0f}s)"
                    )
                    self._log_event(
                        "node_dead",
                        {
                            "node_id": node_id,
                            "time_since_heartbeat": round(time_since_heartbeat, 1),
                        },
                    )
                    self._rebuild_hash_ring()

                elif old_status == HealthStatus.ALIVE:
                    node.status = HealthStatus.SUSPECT
                    logger.warning(
                        f"Node {node_id} SUSPECT "
                        f"(missed {node.failed_attempts} heartbeats)"
                    )
                    self._log_event(
                        "node_suspect",
                        {
                            "node_id": node_id,
                            "missed_count": node.failed_attempts,
                        },
                    )

        return newly_dead

    # =========================================================
    # Hash Ring Management
    # =========================================================

    def _rebuild_hash_ring(self) -> None:
        """Rebuild hash ring with alive nodes only."""
        alive_nodes = [
            nid for nid, n in self.nodes.items() if n.status == HealthStatus.ALIVE
        ]

        if not alive_nodes:
            self.hash_ring = None
            logger.error("No ALIVE nodes in cluster!")
            return

        self.hash_ring = HashRing(alive_nodes, virtual_nodes=160)
        logger.debug(f"Rebuilt hash ring with {len(alive_nodes)} nodes")

    def get_node_for_key(self, key: str) -> Optional[str]:
        """Get primary node responsible for a key."""
        if not self.hash_ring:
            return None
        return self.hash_ring.get_node(key)

    def get_replicas_for_key(self, key: str) -> List[str]:
        """Get primary + replica nodes for a key."""
        if not self.hash_ring:
            return []
        return self.hash_ring.get_replicas(key, self.replication_factor)

    # =========================================================
    # Node Info Queries
    # =========================================================

    def get_node_info(self, node_id: str) -> Optional[NodeInfo]:
        """Get info about a specific node."""
        if node_id not in self.nodes:
            return None

        n = self.nodes[node_id]
        return NodeInfo(
            node_id=n.node_id,
            host=n.host,
            port=n.port,
            zone=n.zone,
            status=n.status,
            cache_size=n.cache_size,
            cache_capacity=n.cache_capacity,
            hit_ratio=n.hit_ratio,
            memory_bytes=n.memory_bytes,
            uptime_s=n.uptime_s,
            last_heartbeat=n.last_heartbeat,
        )

    def get_all_nodes(self) -> List[NodeInfo]:
        """Get info about all nodes."""
        return [self.get_node_info(node_id) for node_id in sorted(self.nodes.keys())]

    def get_cluster_stats(self) -> Dict:
        """Get aggregate cluster statistics."""
        nodes = self.nodes.values()
        alive_count = sum(1 for n in nodes if n.status == HealthStatus.ALIVE)

        total_size = sum(n.cache_size for n in nodes)
        total_capacity = sum(n.cache_capacity for n in nodes)
        avg_hit_ratio = sum(n.hit_ratio for n in nodes) / len(nodes) if nodes else 0.0

        return {
            "total_nodes": len(self.nodes),
            "alive_nodes": alive_count,
            "dead_nodes": sum(1 for n in nodes if n.status == HealthStatus.DEAD),
            "suspect_nodes": sum(1 for n in nodes if n.status == HealthStatus.SUSPECT),
            "total_cache_size": total_size,
            "total_capacity": total_capacity,
            "avg_hit_ratio": avg_hit_ratio,
            "replication_factor": self.replication_factor,
        }

    # =========================================================
    # Event Logging (for dashboard)
    # =========================================================

    def _log_event(self, event_type: str, data: Dict) -> None:
        """Log a cluster event."""
        event = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            **data,
        }
        self.event_log.append(event)

        # Keep only last 1000 events
        if len(self.event_log) > 1000:
            self.event_log.pop(0)

    def get_event_log(self, limit: int = 100) -> List[Dict]:
        """Get recent cluster events."""
        return self.event_log[-limit:]
