"""
Cluster Management API — Phase 3

Endpoints for:
- Querying cluster topology
- Cluster statistics
- Event log (for debugging)
- Admin operations (rebalance, failover, etc)
"""

from fastapi import APIRouter, HTTPException
import logging

from app.models.models import (
    ClusterStatsResponse,
    NodeInfo,
    HealthResponse,
)
from app.services.cluster_manager import ClusterManager

router = APIRouter(prefix="/api/cluster", tags=["cluster"])
cluster_manager: ClusterManager = None


def set_cluster_manager(cm: ClusterManager) -> None:
    """Initialize from main.py."""
    global cluster_manager
    cluster_manager = cm


logger = logging.getLogger(__name__)


@router.get("/nodes", response_model=list[NodeInfo])
async def list_nodes() -> list[NodeInfo]:
    """List all nodes in the cluster."""
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Not initialized")

    return cluster_manager.get_all_nodes()


@router.get("/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str) -> NodeInfo:
    """Get info about a specific node."""
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Not initialized")

    info = cluster_manager.get_node_info(node_id)
    if not info:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")

    return info


@router.get("/stats", response_model=ClusterStatsResponse)
async def cluster_stats() -> dict:
    """Get cluster-wide statistics."""
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Not initialized")

    return cluster_manager.get_cluster_stats()


@router.get("/health", response_model=HealthResponse)
async def health() -> dict:
    """Health check for the gateway itself."""
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Not initialized")

    stats = cluster_manager.get_cluster_stats()

    return {
        "status": "healthy" if stats["alive_nodes"] > 0 else "degraded",
        "cluster_size": stats["total_nodes"],
        "alive_nodes": stats["alive_nodes"],
        "uptime_s": 0,  # TODO: track gateway uptime
    }


@router.get("/events")
async def event_log(limit: int = 100) -> dict:
    """Get recent cluster events."""
    if not cluster_manager:
        raise HTTPException(status_code=503, detail="Not initialized")

    events = cluster_manager.get_event_log(limit)
    return {
        "count": len(events),
        "events": events,
    }


@router.post("/rebalance")
async def trigger_rebalance(force: bool = False) -> dict:
    """
    Trigger manual rebalancing (Phase 7).

    Forces a rebalance even if no topology change detected.
    Useful after major network partition healing.
    """
    # TODO: Implement rebalancer

    return {
        "status": "queued",
        "message": "Rebalance started (Phase 7 - not implemented yet)",
    }


@router.post("/failover/{node_id}")
async def manual_failover(node_id: str) -> dict:
    """
    Manually trigger failover for a node (Phase 6).

    Treats node as DEAD and triggers replica promotion.
    For testing and admin recovery operations.
    """
    # TODO: Implement failover logic

    return {
        "status": "initiated",
        "node_id": node_id,
        "message": "Failover triggered (Phase 6 - not implemented yet)",
    }
