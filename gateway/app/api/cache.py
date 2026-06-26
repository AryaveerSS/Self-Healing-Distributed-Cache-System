"""
Cache API — Phases 5-8

Main API for clients to GET/PUT/DELETE keys.

Responsibilities:
1. Route request to appropriate cache node via hash ring
2. Handle replication (write to primary + replicas)
3. Handle read-your-writes consistency
4. Handle geo-routing (read from nearest replica)
5. Handle circuit breaker (skip failed nodes)
"""

from fastapi import APIRouter, HTTPException, Header, Request
from typing import Optional
import httpx
import time
import logging

from app.models.models import (
    CachePutRequest,
    CacheGetResponse,
    CachePutResponse,
    CacheDeleteResponse,
)
from app.services.cluster_manager import ClusterManager
from app.services.circuit_breaker import CircuitBreaker
from app.services.zone_router import ZoneRouter

router = APIRouter(prefix="/api/cache", tags=["cache"])

cluster_manager: ClusterManager = None
circuit_breaker: CircuitBreaker = None
zone_router: ZoneRouter = None

logger = logging.getLogger(__name__)


def initialize(
    cm: ClusterManager,
    cb: CircuitBreaker,
    zr: ZoneRouter,
) -> None:
    """Initialize dependencies."""
    global cluster_manager, circuit_breaker, zone_router
    cluster_manager = cm
    circuit_breaker = cb
    zone_router = zr


# =========================================================
# Helper: Call remote cache node
# =========================================================


async def call_node(
    node_id: str,
    method: str,
    path: str,
    body: Optional[dict] = None,
    timeout_s: float = 5.0,
) -> httpx.Response:
    """
    Call a cache node via HTTP.

    Raises:
        HTTPException if node unreachable or times out
    """
    node_info = cluster_manager.get_node_info(node_id)
    if not node_info:
        raise HTTPException(status_code=503, detail=f"Node {node_id} not found")

    url = f"http://{node_info.host}:{node_info.port}{path}"

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            if method == "GET":
                resp = await client.get(url)
            elif method == "PUT":
                resp = await client.put(url, json=body)
            elif method == "DELETE":
                resp = await client.delete(url)
            elif method == "POST":
                resp = await client.post(url, json=body)
            else:
                raise ValueError(f"Unknown method {method}")

            # Track for circuit breaker
            if resp.status_code >= 500:
                circuit_breaker.record_failure(node_id)
            else:
                circuit_breaker.record_success(node_id)

            return resp

    except (httpx.ConnectError, httpx.TimeoutException) as e:
        circuit_breaker.record_failure(node_id)
        logger.error(f"Failed to call {node_id}: {e}")
        raise HTTPException(status_code=503, detail=f"Node {node_id} unreachable")


# =========================================================
# GET /cache/{key}
# =========================================================


@router.get("/{key}", response_model=CacheGetResponse)
async def get_key(
    key: str,
    x_client_zone: Optional[str] = Header(None),
) -> dict:
    """
    GET key from cache.

    Algorithm:
    1. Find all replicas for the key
    2. If client in same zone as a replica → read from it (geo-aware)
    3. Else read from primary
    4. If primary was recently written → serve from primary (read-your-writes)

    Headers:
        X-Client-Zone: Client's zone (e.g., "asia-south")
    """
    if not cluster_manager or not zone_router or not circuit_breaker:
        raise HTTPException(status_code=503, detail="Not initialized")

    # Find replicas
    replicas = cluster_manager.get_replicas_for_key(key)
    if not replicas:
        raise HTTPException(status_code=503, detail="No nodes available")

    primary = replicas[0]

    # Check circuit breaker — skip dead nodes
    available_replicas = [n for n in replicas if circuit_breaker.is_available(n)]

    if not available_replicas:
        raise HTTPException(status_code=503, detail="All replicas unavailable")

    # Pick which replica to read from
    target_node = zone_router.pick_replica(
        replicas=available_replicas,
        client_zone=x_client_zone,
        primary=primary,
    )

    # Call the node
    try:
        resp = await call_node(target_node, "GET", f"/cache/{key}")

        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Key not found")

        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Cache node error")

        return resp.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET {key} failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# =========================================================
# PUT /cache/{key}
# =========================================================


@router.put("/{key}", response_model=CachePutResponse)
async def put_key(
    key: str,
    request: CachePutRequest,
    x_client_zone: Optional[str] = Header(None),
) -> dict:
    """
    PUT key into cache.

    Algorithm:
    1. Write to primary node (waits for ack)
    2. Asynchronously replicate to replica nodes
    3. Record write timestamp for read-your-writes consistency
    4. Return immediately (doesn't wait for replicas)

    This is asynchronous replication = higher throughput, eventual consistency.
    """
    if not cluster_manager or not circuit_breaker:
        raise HTTPException(status_code=503, detail="Not initialized")

    # Find primary
    primary = cluster_manager.get_node_for_key(key)
    if not primary:
        raise HTTPException(status_code=503, detail="No nodes available")

    # Check circuit breaker
    if not circuit_breaker.is_available(primary):
        raise HTTPException(status_code=503, detail="Primary node unavailable")

    # Write to primary (blocking)
    try:
        resp = await call_node(
            primary,
            "PUT",
            f"/cache/{key}",
            body={"value": request.value, "ttl": request.ttl or 0},
        )

        if resp.status_code not in [200, 201]:
            raise HTTPException(status_code=resp.status_code, detail="Write failed")

        # TODO: Background task to replicate to other replicas
        # For now, just acknowledge the write

        result = resp.json()

        # Record write timestamp for read-your-writes consistency
        zone_router.pin_write(key, primary, x_client_zone or "default")

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PUT {key} failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# =========================================================
# DELETE /cache/{key}
# =========================================================


@router.delete("/{key}", response_model=CacheDeleteResponse)
async def delete_key(key: str) -> dict:
    """
    DELETE key from cache.

    Deletes from primary, and later from all replicas.
    """
    if not cluster_manager or not circuit_breaker:
        raise HTTPException(status_code=503, detail="Not initialized")

    primary = cluster_manager.get_node_for_key(key)
    if not primary:
        raise HTTPException(status_code=503, detail="No nodes available")

    if not circuit_breaker.is_available(primary):
        raise HTTPException(status_code=503, detail="Primary node unavailable")

    try:
        resp = await call_node(primary, "DELETE", f"/cache/{key}")

        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Key not found")

        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Delete failed")

        # TODO: Delete from replicas async

        return resp.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DELETE {key} failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
