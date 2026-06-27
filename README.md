# Distributed Self-Healing Cache System

A production-grade distributed caching platform demonstrating advanced distributed systems concepts. Built for **SDE interviews at Google, Microsoft, Amazon, Cisco, and similar companies**.

## What This Project Teaches

Every component maps to real-world systems:

| Feature | Real-World Equivalent |
|---------|---------------------|
| Consistent Hashing | DynamoDB, Cassandra, Chord DHT |
| LRU Eviction | Redis, Memcached |
| Heartbeat Detection | Kafka, Consul, ZooKeeper |
| Automatic Failover | Redis Sentinel, MongoDB Replica Sets |
| Write-Ahead Log | PostgreSQL, RocksDB |
| Bloom Filter | Cassandra, HBase, Bigtable |
| Circuit Breaker | Netflix Hystrix, AWS SDK |
| Geo-Routing | Cloudflare, Route 53 |
| Replication | Every distributed database |

## Architecture

```
                     Clients (curl, React)
                            |
                    ┌───────▼────────┐
                    │  FastAPI Gateway│ ← Single entry point
                    │                │
                    │ • Hash Ring     │ ← Consistent hashing
                    │ • Cluster Mgr   │ ← Node tracking
                    │ • Rebalancer    │ ← Key migration
                    │ • Circuit Break │ ← Fault tolerance
                    │ • Zone Router   │ ← Geo-routing
                    │ • Bloom Filter  │ ← Negative lookups
                    └────────┬────────┘
              ┌─────────────┼──────────────┐
              ▼             ▼              ▼
        ┌─────────┐   ┌─────────┐   ┌─────────┐
        │ Node 1  │   │ Node 2  │   │ Node 3  │
        │ C++ LRU │   │ C++ LRU │   │ C++ LRU │
        │ asia    │   │ us-east │   │ europe  │
        └─────────┘   └─────────┘   └─────────┘
              │             │              │
              └─────────────┼──────────────┘
                            ▼
                    ┌──────────────────┐
                    │ React Dashboard  │
                    │ Real-time metrics│
                    │ Node status      │
                    │ Event log        │
                    └──────────────────┘
```

## Phases & Implementation Status

| Phase | Component | Status | LOC |
|-------|-----------|--------|-----|
| 1 | C++ LRU Cache Node | ✅ Complete | 800 |
| 2 | Consistent Hash Ring | ✅ Complete | 200 |
| 3-4 | Cluster Manager | ✅ Complete | 300 |
| 5 | Replication | ✅ Framework | 100 |
| 6 | Circuit Breaker | ✅ Complete | 150 |
| 7 | Key Rebalancer | ✅ Complete | 250 |
| 8 | Geo-Routing + Bloom Filter | ✅ Complete | 200 |
| 9 | React Dashboard | ✅ Complete | 400 |
| 10 | Benchmarks + Chaos Tests | ✅ Complete | 400 |

**Total: ~2,800 lines of production-quality code**

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Node.js 18+ (for dashboard development)
- Python 3.11+ (for benchmarking)
- C++ compiler (for building cache-node)

### Run Everything

```bash
# Clone and setup
git clone <repo>
cd self_healing_cache

# Start the entire cluster (3 nodes + gateway + dashboard)
docker-compose up -d

# Wait for services to be healthy
docker-compose ps

# Access services
# Gateway API:  http://localhost:8000
# Dashboard:    http://localhost:3000
# Nodes:        http://localhost:8001, 8002, 8003
```

### Manual Testing

```bash
# PUT a key (Write to primary)
curl -X PUT http://localhost:8000/api/cache/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": "hello world", "ttl": 300}'

# GET the key (Read from replica if available)
curl http://localhost:8000/api/cache/mykey

# DELETE the key
curl -X DELETE http://localhost:8000/api/cache/mykey

# Cluster status
curl http://localhost:8000/api/cluster/stats
```

## Key Features Explained

### 1. Consistent Hashing (Phase 2)

Why not `hash(key) % N`? When nodes are added/removed, ALL keys rehash to new locations (expensive).

**Consistent hashing:**
- Keys and nodes both placed on a circular ring via hashing
- Adding/removing a node only migrates ~1/N keys
- Virtual nodes ensure uniform distribution even with different capacities

```python
ring = HashRing(nodes=["node-1", "node-2", "node-3"])
owner = ring.get_node("user:123")  # O(log N) lookup
replicas = ring.get_replicas("user:123", replication_factor=3)
```

### 2. LRU Cache (Phase 1)

Each node runs this C++ cache:
- Doubly-linked list + hash map = O(1) operations
- TTL support with lazy expiration (on access) + periodic purge (30s sweep)
- Thread-safe via mutex

```cpp
LRUCache cache(capacity=10000, default_ttl_s=300);
cache.put("key", "value", ttl_seconds=60);
auto value = cache.get("key");  // O(1) + moves to front
cache.remove("key");
```

### 3. Heartbeat-Based Failure Detection (Phase 4)

Nodes send heartbeats every 5 seconds. Gateway tracks them:
- 1 missed heartbeat: `SUSPECT`
- 3 missed heartbeats: `DEAD`
- Recovered node: `ALIVE`

```
Timeline:
T=0:   Heartbeat received     → ALIVE
T=5:   Heartbeat received     → ALIVE
T=10:  TIMEOUT (no heartbeat) → SUSPECT (1 miss)
T=15:  TIMEOUT (no heartbeat) → SUSPECT (2 misses)
T=20:  TIMEOUT (no heartbeat) → DEAD    (3 misses) ← Failover triggered
```

### 4. Automatic Failover (Phase 6)

When primary dies:
1. Replicas continue serving reads
2. Highest replica promoted to primary
3. New replica allocated from survivors
4. All happening **without human intervention**

### 5. Circuit Breaker (Phase 6)

Prevents cascading failures:
- **CLOSED**: Normal, forward requests
- **OPEN**: Too many failures, reject immediately (fail-fast)
- **HALF_OPEN**: Testing recovery, allow one probe

```python
cb = CircuitBreaker(failure_threshold=5, timeout_s=30)
if cb.is_available(node_id):
    response = await call_node(node_id)  # Safe
    cb.record_success(node_id)
else:
    return error_immediately()  # Fail-fast, no timeout
```

### 6. Geo-Aware Routing (Phase 8)

Clients send `X-Client-Zone` header. Gateway:
- **Writes**: Always go to primary (consistency)
- **Reads**: Served from nearest replica in same zone (low latency)
- **Read-your-writes**: After write, pin reads to primary for 500ms

```
Client in asia-south:
  PUT key → primary (us-east) ← cross-datacenter write
  GET key → replica in asia-south ← low latency read
  (for 500ms, reads go to primary to avoid stale data)
```

### 7. Bloom Filter (Phase 8)

Before routing GET request, check bloom filter:
- Key definitely absent? Return 404 immediately (no RPC)
- Saves RTT for ~20-30% of reads in miss-heavy workloads

```python
bf = BloomFilter(size_bytes=1_000_000)
bf.add("key1")
if not bf.might_exist("key2"):
    return 404  # Definitely absent, no RPC needed
```

### 8. Key Rebalancing (Phase 7)

When topology changes:
1. Build new hash ring with remaining nodes
2. Compute key ownership changes
3. Snapshot keys from old owner
4. Bulk-load to new owner
5. Clean up optional

Only affected keys migrate, others stay in place.

## Performance Characteristics

### Expected Latencies (Single datacenter)
- Cache hit: **0.5-2ms**
- Cache miss (to node): **1-3ms**
- Write (primary): **1-5ms**
- Cross-datacenter write: **50-200ms**

### Throughput
- Single node: **10,000-50,000 req/s** (depending on value size)
- 3-node cluster: **30,000-150,000 req/s**

### Replication Overhead
- Async replication: **~5-10% latency increase**
- Replica lag: **10-100ms** (depending on load)

## Interview Talking Points

### Consistent Hashing
- **Question**: How do you add a node without rehashing all keys?
- **Answer**: Consistent hashing ensures only ~1/N keys migrate
- **Code walkthrough**: Show `hash_ring.py` virtual node logic

### Failure Detection
- **Question**: How do you know when a node is dead?
- **Answer**: Heartbeat timeouts with exponential backoff
- **Trade-off**: Detect failures in 15 seconds vs false positives

### Rebalancing Safety
- **Question**: What if a rebalance fails halfway?
- **Answer**: Idempotent operations (write-ahead log), can retry safely

### Geo-Routing
- **Question**: How do you ensure read-your-writes consistency?
- **Answer**: Pin reads to primary for 500ms after write

### Bloom Filters
- **Question**: Why use a Bloom filter in a cache?
- **Answer**: Eliminate ~20% of cache misses (no RPC for known absences)

## Development

### Build C++ Cache Node
```bash
cd cache-node
mkdir build && cd build
cmake ..
make
./cache_node
```

### Run Gateway Locally
```bash
cd gateway
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m uvicorn app.main:app --reload
```

### Run Dashboard
```bash
cd dashboard
npm install
npm run dev  # http://localhost:3000
```

### Run Benchmarks
```bash
cd benchmarks
pip install httpx
python benchmark.py      # Throughput + latency tests
python chaos_test.py     # Resilience tests
```

## Production Considerations

**Not included** (out of scope for interview project):
- Persistence (RocksDB backend)
- Distributed consensus (Raft/Paxos)
- Advanced monitoring (Prometheus/Grafana)
- Authentication/Authorization
- TLS encryption
- Load balancing
- Sharding across multiple clusters

These are intentionally left as "Phase 11, 12, ..." to keep scope manageable while proving understanding.

## Code Quality

- **Type hints**: Full Python type hints for IDE support
- **Documentation**: Docstrings explain algorithms & trade-offs
- **Error handling**: Graceful degradation, no panics
- **Logging**: Structured logs for debugging
- **Testing**: Unit tests for hash ring, benchmarks for performance

## Interview Strategy

**Time Allocation (1 hour):**
1. High-level walkthrough (5 min)
2. Hash ring deep-dive (10 min) — *show code*
3. Failure detection explanation (10 min) — *show cluster_manager.py*
4. Rebalancing algorithm (10 min) — *show rebalancer.py*
5. Live demo (15 min) — *docker-compose + curl + dashboard*
6. Q&A on trade-offs (10 min)

**Talking Points:**
- "This is inspired by how DynamoDB/Cassandra/Redis Cluster actually work"
- "Trade-off: async replication (fast) vs strongly consistent (slow)"
- "Why heartbeats instead of health checks: push > pull at scale"
- "Virtual nodes solve the 'unlucky hashing' problem"
- "Bloom filters reduce network RTT for cache misses"

## License

MIT — Free to use, modify, and redistribute

## Author

Built as a portfolio project demonstrating deep understanding of distributed systems concepts essential for senior SDE roles.
