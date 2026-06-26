"""
Heartbeat API — Phase 4

Endpoint for cache nodes to send health information to the cluster manager.
This is the primary mechanism for failure detection.

Design: Push model (nodes send heartbeats) instead of pull (gateway polls).
Why? Scales better, nodes are aware of their own state, reduces gateway load.
"""

from fastapi import APIRouter, Request, HTTPException
import logging

from app.models.models import HeartbeatPayload
from app.services.cluster_manager import ClusterManager

router = APIRouter(tags=["heartbeat"])

# Global singleton — shared with main.py
# In production, use dependency injection
cluster_manager: ClusterManager = None


def set_cluster_manager(cm: ClusterManager) -> None:
    """Initialize the cluster manager (called from main.py)."""
    global cluster_manager
    cluster_manager = cm


logger = logging.getLogger(__name__)


@router.post("/heartbeat")
async def heartbeat(payload: HeartbeatPayload) -> dict:
    """
    Receive heartbeat from a cache node.
    Auto-registers the node if not yet known.
    """
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Cluster manager not initialized")

    # Auto-register unknown nodes — C++ nodes don't call /register explicitly
    if payload.node_id not in cluster_manager.nodes:
        cluster_manager.register_node(
            node_id=payload.node_id,
            host=payload.host,
            port=payload.port,
            zone=payload.zone,
            cache_capacity=payload.cache_capacity,
        )
        logger.info(f"Auto-registered node {payload.node_id} via heartbeat")

    cluster_manager.process_heartbeat(
        node_id=payload.node_id,
        metrics={
            "memory_bytes": payload.memory_bytes,
            "cache_size": payload.cache_size,
            "hit_ratio": payload.hit_ratio,
            "uptime_s": payload.uptime_s,
        },
    )

    logger.debug(f"Heartbeat from {payload.node_id} (zone={payload.zone})")

    return {"status": "received", "node_id": payload.node_id}


@router.post("/register")
async def register_node(data: dict) -> dict:
    """
    Register a new node with the cluster.

    Cache nodes call this once at startup to introduce themselves.
    """
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Cluster manager not initialized")

    required = ["node_id", "host", "port", "zone", "cache_capacity"]
    for field in required:
        if field not in data:
            raise HTTPException(
                status_code=400, detail=f"Missing required field: {field}"
            )

    cluster_manager.register_node(
        node_id=data["node_id"],
        host=data["host"],
        port=data["port"],
        zone=data["zone"],
        cache_capacity=data["cache_capacity"],
    )

    logger.info(f"Registered node {data['node_id']}")

    return {
        "status": "registered",
        "node_id": data["node_id"],
        "cluster_size": len(cluster_manager.nodes),
    }
