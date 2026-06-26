"""
Gateway Main — FastAPI Application

Orchestrates all microservices:
- Cluster Manager: tracks node health
- Hash Ring: consistent hashing
- Circuit Breaker: fault tolerance
- Zone Router: geo-aware routing
- Rebalancer: key migration
- Bloom Filter: negative lookup optimization
"""

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from app.config import settings
from app.services.cluster_manager import ClusterManager
from app.services.circuit_breaker import CircuitBreaker
from app.services.zone_router import ZoneRouter
from app.services.rebalancer import Rebalancer
from app.services.bloom_filter import BloomFilter

# API routers
from app.api import heartbeat, cluster, cache

# Setup logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Global singletons
cluster_manager: ClusterManager = None
circuit_breaker: CircuitBreaker = None
zone_router: ZoneRouter = None
rebalancer: Rebalancer = None
bloom_filter: BloomFilter = None


async def startup_event():
    """Initialize all services on startup."""
    global cluster_manager, circuit_breaker, zone_router, rebalancer, bloom_filter

    logger.info("Initializing gateway services...")

    cluster_manager = ClusterManager(
        heartbeat_timeout_s=settings.heartbeat_timeout_s,
        replication_factor=settings.replication_factor,
    )

    circuit_breaker = CircuitBreaker(
        failure_threshold=settings.circuit_breaker_failure_threshold,
        timeout_s=settings.circuit_breaker_timeout_s,
    )

    zone_router = ZoneRouter()
    rebalancer = Rebalancer()
    bloom_filter = BloomFilter(
        size_bytes=settings.bloom_filter_size_bytes,
        num_hashes=7,
    )

    # Wire up dependencies
    heartbeat.set_cluster_manager(cluster_manager)
    cluster.set_cluster_manager(cluster_manager)
    cache.initialize(cluster_manager, circuit_breaker, zone_router)

    logger.info("Gateway initialized successfully")

    # Start background failure detection loop
    asyncio.create_task(_failure_detection_loop())


async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Gateway shutting down...")


async def _failure_detection_loop():
    """
    Background task: scan for dead nodes every 3 seconds.

    This is the heart of self-healing. Without this loop running,
    nodes that stop sending heartbeats are never detected as dead.

    Algorithm:
      - Every 3s, iterate all nodes
      - If (now - last_heartbeat) > heartbeat_timeout_s → increment miss counter
      - If miss counter >= DEAD_THRESHOLD → mark DEAD, open circuit breaker
      - If node recovers (sends heartbeat) → mark ALIVE, close circuit breaker
    """
    await asyncio.sleep(5)  # Let gateway fully start before checking

    while True:
        try:
            if cluster_manager:
                newly_dead = cluster_manager.check_heartbeat_timeouts()

                for node_id in newly_dead:
                    # Open circuit breaker immediately for dead node
                    for _ in range(circuit_breaker.failure_threshold):
                        circuit_breaker.record_failure(node_id)
                    logger.error(
                        f"[FAILOVER] Node {node_id} is DEAD — "
                        f"circuit breaker opened, traffic rerouted"
                    )

        except Exception as e:
            logger.error(f"Failure detection loop error: {e}")

        await asyncio.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    await startup_event()
    yield
    await shutdown_event()


# Create FastAPI app with lifespan
app = FastAPI(
    title="Distributed Cache Cluster Manager",
    version="1.0.0",
    description="Production-grade distributed cache with consistent hashing, failover, and geo-routing",
    lifespan=lifespan,
)

# Allow the dashboard (any localhost origin) to call the gateway
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers — each router defines its own prefix
app.include_router(heartbeat.router)
app.include_router(cluster.router)
app.include_router(cache.router)


# =========================================================
# Health & Status Endpoints
# =========================================================


@app.get("/")
async def root():
    """Root endpoint."""
    global cluster_manager

    stats = cluster_manager.get_cluster_stats() if cluster_manager else {}

    return {
        "service": "Distributed Cache Gateway",
        "version": "1.0.0",
        "status": "running",
        "cluster": stats,
    }


@app.get("/status")
async def status():
    """Detailed status."""
    global cluster_manager, circuit_breaker

    if not cluster_manager:
        return {"status": "initializing"}

    stats = cluster_manager.get_cluster_stats()

    return {
        "status": "healthy" if stats["alive_nodes"] > 0 else "degraded",
        "cluster_stats": stats,
        "circuit_breakers": len(circuit_breaker.states),
    }


# =========================================================
# Background Tasks
# =========================================================


@app.post("/api/health-check")
async def trigger_health_check(background_tasks: BackgroundTasks):
    """
    Manually trigger health check (normally runs every N seconds).
    """
    global cluster_manager

    if not cluster_manager:
        return {"status": "error", "message": "Not initialized"}

    def check_timeouts():
        newly_dead = cluster_manager.check_heartbeat_timeouts()
        if newly_dead:
            logger.warning(f"Detected {len(newly_dead)} dead nodes: {newly_dead}")

    background_tasks.add_task(check_timeouts)

    return {"status": "health-check queued"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
