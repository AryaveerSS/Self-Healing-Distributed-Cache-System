#include "cache_server.hpp"

// cpp-httplib — header-only HTTP server/client
// Download: https://github.com/yhirose/cpp-httplib
// Place httplib.h in cache-node/third_party/
#include "../third_party/httplib.h"

#include <nlohmann/json.hpp>
#include <iostream>
#include <sstream>
#include <chrono>

using json = nlohmann::json;

namespace cache {

// ============================================================
// Constructor
// ============================================================
CacheServer::CacheServer(NodeConfig config)
    : config_(std::move(config))
    , cache_(config_.cache_capacity, config_.default_ttl_s)
    , start_time_(std::chrono::steady_clock::now())
{
    std::cout << "[" << config_.node_id << "] "
              << "Cache node initialised. "
              << "Capacity=" << config_.cache_capacity
              << " Zone=" << config_.zone
              << " Port=" << config_.port
              << "\n";
}

CacheServer::~CacheServer() {
    stop();
}

// ============================================================
// Start — registers routes and begins listening
// ============================================================
void CacheServer::start() {
    running_ = true;

    // Launch background threads
    heartbeat_thread_ = std::thread(&CacheServer::heartbeat_loop, this);
    purge_thread_     = std::thread(&CacheServer::purge_loop,     this);

    // -------------------------------------------------------
    // Build HTTP server and register routes
    // -------------------------------------------------------
    httplib::Server svr;

    // ---------- GET /cache/:key ----------
    svr.Get(R"(/cache/(.+))", [this](const httplib::Request& req,
                                      httplib::Response& res) {
        ++request_count_;
        const std::string& key = req.matches[1];
        handle_get(key, &res);
    });

    // ---------- PUT /cache/:key ----------
    svr.Put(R"(/cache/(.+))", [this](const httplib::Request& req,
                                      httplib::Response& res) {
        ++request_count_;
        const std::string& key = req.matches[1];
        handle_put(key, req.body, &res);
    });

    // ---------- DELETE /cache/:key ----------
    svr.Delete(R"(/cache/(.+))", [this](const httplib::Request& req,
                                         httplib::Response& res) {
        ++request_count_;
        const std::string& key = req.matches[1];
        handle_delete(key, &res);
    });

    // ---------- GET /health ----------
    svr.Get("/health", [this](const httplib::Request&,
                               httplib::Response& res) {
        handle_health(&res);
    });

    // ---------- GET /metrics ----------
    svr.Get("/metrics", [this](const httplib::Request&,
                                httplib::Response& res) {
        handle_metrics(&res);
    });

    // ---------- GET /keys ----------
    svr.Get("/keys", [this](const httplib::Request&,
                             httplib::Response& res) {
        handle_keys(&res);
    });

    // ---------- GET /snapshot ----------
    // Full dump: used by replication and rebalancing
    svr.Get("/snapshot", [this](const httplib::Request&,
                                 httplib::Response& res) {
        handle_snapshot(&res);
    });

    // ---------- POST /bulk-load ----------
    // Receives migrated keys from another node or the cluster manager
    svr.Post("/bulk-load", [this](const httplib::Request& req,
                                   httplib::Response& res) {
        handle_bulk_load(req.body, &res);
    });

    // ---------- POST /purge-expired ----------
    svr.Post("/purge-expired", [this](const httplib::Request&,
                                       httplib::Response& res) {
        size_t removed = cache_.purge_expired();
        json resp = {{"purged", removed}};
        res.set_content(resp.dump(), "application/json");
    });

    std::cout << "[" << config_.node_id << "] "
              << "Listening on " << config_.host
              << ":" << config_.port << "\n";

    // Blocks here until svr.stop() is called
    svr.listen(config_.host.c_str(), config_.port);
}

// ============================================================
// Stop
// ============================================================
void CacheServer::stop() {
    if (!running_.exchange(false)) return;

    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    if (purge_thread_.joinable())     purge_thread_.join();
}

bool CacheServer::is_running() const {
    return running_.load();
}

// ============================================================
// Handler: GET /cache/:key
// ============================================================
// Returns:
//   200 {"key": "...", "value": "...", "node_id": "..."}
//   404 {"error": "key not found"}
// ============================================================
void CacheServer::handle_get(const std::string& key, void* res_ptr) {
    auto* res = static_cast<httplib::Response*>(res_ptr);
    auto  val = cache_.get(key);

    if (val.has_value()) {
        json body = {
            {"key",     key},
            {"value",   val.value()},
            {"node_id", config_.node_id},
            {"zone",    config_.zone}
        };
        res->set_content(body.dump(), "application/json");
        res->status = 200;
    } else {
        json body = {{"error", "key not found"}, {"key", key}};
        res->set_content(body.dump(), "application/json");
        res->status = 404;
    }
}

// ============================================================
// Handler: PUT /cache/:key
// ============================================================
// Request body JSON:
//   {"value": "...", "ttl": 60}   ← ttl optional
//
// Returns:
//   201 {"status": "created", "key": "..."}
//   200 {"status": "updated", "key": "..."}
//   400 {"error": "invalid JSON"}
// ============================================================
void CacheServer::handle_put(const std::string& key,
                              const std::string& body,
                              void* res_ptr)
{
    auto* res = static_cast<httplib::Response*>(res_ptr);

    json req_body;
    try {
        req_body = json::parse(body);
    } catch (...) {
        json err = {{"error", "invalid JSON body"}};
        res->set_content(err.dump(), "application/json");
        res->status = 400;
        return;
    }

    if (!req_body.contains("value")) {
        json err = {{"error", "missing 'value' field"}};
        res->set_content(err.dump(), "application/json");
        res->status = 400;
        return;
    }

    std::string value    = req_body["value"].get<std::string>();
    uint32_t    ttl      = req_body.value("ttl", 0);
    bool        existed  = cache_.contains(key);

    cache_.put(key, value, ttl);

    json resp = {
        {"status",  existed ? "updated" : "created"},
        {"key",     key},
        {"node_id", config_.node_id}
    };
    res->set_content(resp.dump(), "application/json");
    res->status = existed ? 200 : 201;
}

// ============================================================
// Handler: DELETE /cache/:key
// ============================================================
void CacheServer::handle_delete(const std::string& key, void* res_ptr) {
    auto* res = static_cast<httplib::Response*>(res_ptr);
    bool removed = cache_.remove(key);

    if (removed) {
        json body = {{"status", "deleted"}, {"key", key}};
        res->set_content(body.dump(), "application/json");
        res->status = 200;
    } else {
        json body = {{"error", "key not found"}, {"key", key}};
        res->set_content(body.dump(), "application/json");
        res->status = 404;
    }
}

// ============================================================
// Handler: GET /health
// ============================================================
// Used by cluster manager for heartbeat verification.
// Returns lightweight payload — called every N seconds.
// ============================================================
void CacheServer::handle_health(void* res_ptr) {
    auto* res = static_cast<httplib::Response*>(res_ptr);

    auto now     = std::chrono::steady_clock::now();
    auto uptime  = std::chrono::duration_cast<std::chrono::seconds>(
                       now - start_time_).count();
    auto metrics = cache_.metrics();

    json body = {
        {"status",        "alive"},
        {"node_id",       config_.node_id},
        {"zone",          config_.zone},
        {"port",          config_.port},
        {"uptime_s",      uptime},
        {"cache_size",    cache_.size()},
        {"cache_capacity",cache_.capacity()},
        {"hit_ratio",     metrics.hit_ratio()},
        {"memory_bytes",  cache_.estimated_memory_bytes()},
        {"timestamp",     std::chrono::duration_cast<std::chrono::milliseconds>(
                              now.time_since_epoch()).count()}
    };
    res->set_content(body.dump(), "application/json");
    res->status = 200;
}

// ============================================================
// Handler: GET /metrics
// ============================================================
void CacheServer::handle_metrics(void* res_ptr) {
    auto* res = static_cast<httplib::Response*>(res_ptr);
    res->set_content(metrics_to_json(), "application/json");
    res->status = 200;
}

std::string CacheServer::metrics_to_json() const {
    auto m = cache_.metrics();
    json body = {
        {"node_id",        config_.node_id},
        {"zone",           config_.zone},
        {"hits",           m.hits},
        {"misses",         m.misses},
        {"evictions",      m.evictions},
        {"expirations",    m.expirations},
        {"total_puts",     m.total_puts},
        {"total_deletes",  m.total_dels},
        {"hit_ratio",      m.hit_ratio()},
        {"cache_size",     cache_.size()},
        {"cache_capacity", cache_.capacity()},
        {"memory_bytes",   cache_.estimated_memory_bytes()},
        {"total_requests", request_count_.load()}
    };
    return body.dump();
}

// ============================================================
// Handler: GET /keys
// ============================================================
void CacheServer::handle_keys(void* res_ptr) {
    auto* res  = static_cast<httplib::Response*>(res_ptr);
    auto  keys = cache_.keys();
    json  body = {{"node_id", config_.node_id}, {"keys", keys}};
    res->set_content(body.dump(), "application/json");
    res->status = 200;
}

// ============================================================
// Handler: GET /snapshot
// ============================================================
// Returns full cache contents for replication/rebalancing.
// Format: [{key, value, ttl_remaining_s}, ...]
// ============================================================
void CacheServer::handle_snapshot(void* res_ptr) {
    auto* res  = static_cast<httplib::Response*>(res_ptr);
    auto  snap = cache_.snapshot();

    json entries = json::array();
    for (auto& [k, v, ttl] : snap) {
        entries.push_back({{"key", k}, {"value", v}, {"ttl", ttl}});
    }
    json body = {{"node_id", config_.node_id}, {"entries", entries}};
    res->set_content(body.dump(), "application/json");
    res->status = 200;
}

// ============================================================
// Handler: POST /bulk-load
// ============================================================
// Body: {"entries": [{"key": "...", "value": "...", "ttl": 60}, ...]}
// Used during key migration when nodes join/leave/recover.
// ============================================================
void CacheServer::handle_bulk_load(const std::string& body, void* res_ptr) {
    auto* res = static_cast<httplib::Response*>(res_ptr);

    json req;
    try {
        req = json::parse(body);
    } catch (...) {
        res->set_content(
            json{{"error", "invalid JSON"}}.dump(), "application/json");
        res->status = 400;
        return;
    }

    std::vector<std::tuple<std::string, std::string, uint32_t>> entries;
    for (auto& e : req["entries"]) {
        entries.emplace_back(
            e["key"].get<std::string>(),
            e["value"].get<std::string>(),
            e.value("ttl", 0)
        );
    }

    cache_.bulk_load(entries);

    json resp = {{"status", "loaded"}, {"count", entries.size()}};
    res->set_content(resp.dump(), "application/json");
    res->status = 200;
}

// ============================================================
// Background: Heartbeat Loop
// ============================================================
// Periodically POSTs health info to the cluster manager.
// If the cluster manager doesn't hear from us within 2x the
// interval, it marks this node DEAD and triggers failover.
//
// Why push instead of pull?
//   Pull: manager polls every node → simpler but N×polling load
//   Push: node sends → manager is passive, scales better
//   We use push (same as Kafka brokers, Consul agents)
// ============================================================
void CacheServer::heartbeat_loop() {
    httplib::Client client(config_.cluster_manager_url);
    client.set_connection_timeout(2);
    client.set_read_timeout(2);

    while (running_) {
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.heartbeat_interval_s));

        if (!running_) break;

        auto  m     = cache_.metrics();
        auto  now   = std::chrono::steady_clock::now();
        auto  uptime= std::chrono::duration_cast<std::chrono::seconds>(
                          now - start_time_).count();

        json payload = {
            {"node_id",       config_.node_id},
            {"host",          config_.host},
            {"port",          config_.port},
            {"zone",          config_.zone},
            {"status",        "alive"},
            {"cache_size",    cache_.size()},
            {"cache_capacity",cache_.capacity()},
            {"hit_ratio",     m.hit_ratio()},
            {"memory_bytes",  cache_.estimated_memory_bytes()},
            {"uptime_s",      uptime},
            {"timestamp",     std::chrono::duration_cast<std::chrono::milliseconds>(
                                  now.time_since_epoch()).count()}
        };

        auto res = client.Post(
            "/internal/heartbeat",
            payload.dump(),
            "application/json"
        );

        if (!res || res->status != 200) {
            std::cerr << "[" << config_.node_id << "] "
                      << "Heartbeat failed — cluster manager unreachable\n";
        }
    }
}

// ============================================================
// Background: Purge Loop
// ============================================================
// Periodic TTL expiration sweep — keeps memory clean even
// for keys that are never accessed again (lazy expiry alone
// wouldn't collect those).
// ============================================================
void CacheServer::purge_loop() {
    while (running_) {
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.purge_interval_s));

        if (!running_) break;

        size_t removed = cache_.purge_expired();
        if (removed > 0) {
            std::cout << "[" << config_.node_id << "] "
                      << "Purged " << removed << " expired keys\n";
        }
    }
}

} // namespace cache
