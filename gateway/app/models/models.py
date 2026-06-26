"""
Data Models — Pydantic schemas for all API contracts.

Using Pydantic ensures:
- Type safety
- Automatic validation
- OpenAPI schema generation
- Serialization/deserialization
"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from enum import Enum


class NodeStatus(str, Enum):
    """Node health states."""

    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    RECOVERED = "recovered"


# =========================================================
# Node Models
# =========================================================


class NodeInfo(BaseModel):
    """Information about a cache node."""

    node_id: str
    host: str
    port: int
    zone: str
    status: NodeStatus
    cache_size: int
    cache_capacity: int
    hit_ratio: float
    memory_bytes: int
    uptime_s: int
    last_heartbeat: float


class HeartbeatPayload(BaseModel):
    """Heartbeat POST from cache node to gateway."""

    node_id: str
    host: str
    port: int
    zone: str
    status: str
    cache_size: int
    cache_capacity: int
    hit_ratio: float
    memory_bytes: int
    uptime_s: int
    timestamp: int  # milliseconds


class NodeRegistration(BaseModel):
    """Register a new node with cluster manager."""

    node_id: str
    host: str
    port: int
    zone: str
    cache_capacity: int


# =========================================================
# Cache Models
# =========================================================


class CacheGetRequest(BaseModel):
    """GET request body (if needed for metadata)."""

    zone: Optional[str] = None  # client's zone for geo-routing


class CachePutRequest(BaseModel):
    """PUT request body."""

    value: str = Field(..., description="Value to cache")
    ttl: Optional[int] = Field(default=0, description="TTL in seconds (0 = no expiry)")


class CacheGetResponse(BaseModel):
    """Response from GET."""

    key: str
    value: str
    node_id: str
    zone: str


class CachePutResponse(BaseModel):
    """Response from PUT."""

    status: str  # "created" or "updated"
    key: str
    node_id: str


class CacheDeleteResponse(BaseModel):
    """Response from DELETE."""

    status: str  # "deleted"
    key: str


class CacheErrorResponse(BaseModel):
    """Error response."""

    error: str
    key: Optional[str] = None


# =========================================================
# Replication Models (Phase 5)
# =========================================================


class SnapshotEntry(BaseModel):
    """Single entry in a snapshot."""

    key: str
    value: str
    ttl: int  # remaining TTL in seconds


class SnapshotResponse(BaseModel):
    """Full snapshot response from cache node."""

    node_id: str
    entries: List[SnapshotEntry]


class BulkLoadRequest(BaseModel):
    """Bulk load request for key migration."""

    entries: List[SnapshotEntry]


class BulkLoadResponse(BaseModel):
    """Response from bulk load."""

    status: str
    count: int


# =========================================================
# Cluster Info Models (Phase 3)
# =========================================================


class ClusterStatsResponse(BaseModel):
    """Cluster-wide statistics."""

    total_nodes: int
    alive_nodes: int
    dead_nodes: int
    suspect_nodes: int
    total_cache_size: int
    total_capacity: int
    avg_hit_ratio: float
    replication_factor: int


class ClusterEventLog(BaseModel):
    """Cluster event entry."""

    timestamp: str
    event_type: str
    data: Dict[str, Any]


class HealthResponse(BaseModel):
    """Gateway health check response."""

    status: str
    cluster_size: int
    alive_nodes: int
    uptime_s: int


# =========================================================
# Rebalancing Models (Phase 7)
# =========================================================


class RebalanceRequest(BaseModel):
    """Trigger rebalancing."""

    force: Optional[bool] = False


class RebalanceStatus(BaseModel):
    """Status of ongoing rebalance."""

    status: str  # "pending", "in_progress", "completed"
    moved_keys: int
    remaining_keys: int
    affected_nodes: List[str]


# =========================================================
# Circuit Breaker Models (Phase 6)
# =========================================================


class CircuitBreakerStatus(BaseModel):
    """Status of circuit breaker for a node."""

    node_id: str
    status: str  # "closed", "open", "half_open"
    failures: int
    last_failure_time: Optional[float] = None
    recovery_time: Optional[float] = None


# =========================================================
# Metrics Models (Phase 9)
# =========================================================


class NodeMetrics(BaseModel):
    """Detailed metrics from a cache node."""

    node_id: str
    zone: str
    hits: int
    misses: int
    evictions: int
    expirations: int
    total_puts: int
    total_deletes: int
    hit_ratio: float
    cache_size: int
    cache_capacity: int
    memory_bytes: int
    total_requests: int


class LatencyMetrics(BaseModel):
    """Latency percentiles."""

    p50_ms: float
    p95_ms: float
    p99_ms: float
    p999_ms: float


class RequestMetrics(BaseModel):
    """Request rate metrics."""

    requests_per_second: float
    average_latency_ms: float
    max_latency_ms: float
