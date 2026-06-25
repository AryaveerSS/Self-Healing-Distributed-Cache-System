#include "cache_server.hpp"
#include <iostream>
#include <cstdlib>
#include <stdexcept>

// ============================================================
// main.cpp — Cache Node Entry Point
// ============================================================
// Configuration is read from environment variables so each
// Docker container gets its own identity without code changes.
//
// Environment Variables:
//   NODE_ID          (required) e.g. "node-1"
//   NODE_HOST        (default: "0.0.0.0")
//   NODE_PORT        (required) e.g. "8001"
//   NODE_ZONE        (default: "default") e.g. "asia-south"
//   CACHE_CAPACITY   (default: "1000")
//   DEFAULT_TTL_S    (default: "0" = no TTL)
//   CLUSTER_MANAGER  (default: "http://gateway:8000")
//   HEARTBEAT_S      (default: "5")
//   PURGE_INTERVAL_S (default: "30")
// ============================================================

static std::string env_or(const char* name, const std::string& fallback) {
    const char* val = std::getenv(name);
    return val ? std::string(val) : fallback;
}

static int env_int(const char* name, int fallback) {
    const char* val = std::getenv(name);
    if (!val) return fallback;
    try { return std::stoi(val); }
    catch (...) { return fallback; }
}

int main() {
    cache::NodeConfig config;

    config.node_id     = env_or ("NODE_ID",         "node-1");
    config.host        = env_or ("NODE_HOST",        "0.0.0.0");
    config.port        = env_int("NODE_PORT",        8001);
    config.zone        = env_or ("NODE_ZONE",        "default");
    config.cache_capacity     = static_cast<size_t>(
                                    env_int("CACHE_CAPACITY", 1000));
    config.default_ttl_s      = static_cast<uint32_t>(
                                    env_int("DEFAULT_TTL_S", 0));
    config.cluster_manager_url = env_or("CLUSTER_MANAGER",
                                         "http://gateway:8000");
    config.heartbeat_interval_s = static_cast<uint32_t>(
                                    env_int("HEARTBEAT_S", 5));
    config.purge_interval_s     = static_cast<uint32_t>(
                                    env_int("PURGE_INTERVAL_S", 30));

    std::cout << "========================================\n";
    std::cout << " Distributed Cache Node\n";
    std::cout << "========================================\n";
    std::cout << " ID       : " << config.node_id   << "\n";
    std::cout << " Zone     : " << config.zone       << "\n";
    std::cout << " Port     : " << config.port       << "\n";
    std::cout << " Capacity : " << config.cache_capacity << "\n";
    std::cout << " TTL      : " << config.default_ttl_s
              << (config.default_ttl_s == 0 ? " (disabled)" : "s") << "\n";
    std::cout << " Manager  : " << config.cluster_manager_url << "\n";
    std::cout << "========================================\n";

    try {
        cache::CacheServer server(config);
        server.start();  // blocks until killed
    } catch (const std::exception& ex) {
        std::cerr << "[FATAL] " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
