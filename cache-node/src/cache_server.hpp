#pragma once

#include "lru_cache.hpp"
#include <string>
#include <thread>
#include <atomic>
#include <functional>
#include <chrono>

// ============================================================
// CacheServer
// ============================================================
// Wraps LRUCache and exposes it over HTTP using cpp-httplib.
//
// Each cache node is an independent process running this server.
// The FastAPI gateway talks to these servers via REST.
//
// Endpoints exposed:
//   GET    /cache/{key}           → get value
//   PUT    /cache/{key}           → set value (body: JSON)
//   DELETE /cache/{key}           → delete key
//   GET    /health                → heartbeat endpoint
//   GET    /metrics               → cache statistics
//   GET    /keys                  → list all keys (for rebalancing)
//   POST   /bulk-load             → load migrated keys
//   GET    /snapshot              → full cache dump (for replication)
//   POST   /purge-expired         → trigger manual GC
//
// Design Decision — Why cpp-httplib?
//   Header-only, zero dependencies, simple API.
//   Production alternative: Boost.Beast, Drogon, uWebSockets.
//   For this project, httplib keeps the build trivial.
// ============================================================

namespace cache {

struct NodeConfig {
    std::string node_id;         // e.g. "node-1"
    std::string host;            // e.g. "0.0.0.0"
    int         port;            // e.g. 8001
    std::string zone;            // e.g. "asia-south" for geo-routing
    size_t      cache_capacity;  // max keys
    uint32_t    default_ttl_s;   // 0 = no TTL
    std::string cluster_manager_url; // e.g. "http://gateway:8000"
    uint32_t    heartbeat_interval_s = 5;
    uint32_t    purge_interval_s     = 30;
};

class CacheServer {
public:
    explicit CacheServer(NodeConfig config);
    ~CacheServer();

    // Start listening — blocks until stop() is called
    void start();

    // Signal the server to stop
    void stop();

    // Check if server is running
    bool is_running() const;

private:
    // -------------------------------------------------------
    // Route handlers
    // -------------------------------------------------------
    void handle_get    (const std::string& key,  /* response */ void* res);
    void handle_put    (const std::string& key,  const std::string& body, void* res);
    void handle_delete (const std::string& key,  void* res);
    void handle_health (void* res);
    void handle_metrics(void* res);
    void handle_keys   (void* res);
    void handle_snapshot(void* res);
    void handle_bulk_load(const std::string& body, void* res);

    // -------------------------------------------------------
    // Background threads
    // -------------------------------------------------------

    // Sends POST /heartbeat to cluster manager every N seconds
    // Payload: {node_id, host, port, zone, metrics}
    void heartbeat_loop();

    // Calls purge_expired() every N seconds
    void purge_loop();

    // -------------------------------------------------------
    // JSON helpers
    // -------------------------------------------------------
    std::string metrics_to_json() const;
    std::string snapshot_to_json() const;

    // -------------------------------------------------------
    // Members
    // -------------------------------------------------------
    NodeConfig   config_;
    LRUCache     cache_;
    std::atomic<bool> running_{false};

    std::thread heartbeat_thread_;
    std::thread purge_thread_;

    // Uptime tracking
    std::chrono::steady_clock::time_point start_time_;

    // Request counter for req/sec metric
    std::atomic<uint64_t> request_count_{0};
};

} // namespace cache
