#pragma once

#include <unordered_map>
#include <string>
#include <mutex>
#include <chrono>
#include <optional>

// ============================================================
// LRU Cache — Core Data Structure
// ============================================================
// Design Decisions:
//   - Doubly Linked List for O(1) move-to-front and eviction
//   - unordered_map for O(1) key lookup to list node
//   - Optional TTL per key (lazy expiration on access)
//   - Thread-safe via std::mutex (one lock per node)
//
// Why NOT use std::list?
//   std::list iterators stay valid after splice — we exploit
//   this to move nodes without re-allocating memory.
//
// Trade-off:
//   Single mutex = simple but serializes all ops.
//   Production alternative: striped locking or RCU.
// ============================================================

namespace cache {

// Represents the expiry time point for a cache entry
using TimePoint = std::chrono::steady_clock::time_point;

struct CacheEntry {
    std::string key;
    std::string value;
    std::optional<TimePoint> expires_at;  // nullopt = no TTL
};

// Doubly Linked List Node
struct ListNode {
    CacheEntry entry;
    ListNode* prev;
    ListNode* next;

    explicit ListNode(CacheEntry e)
        : entry(std::move(e)), prev(nullptr), next(nullptr) {}
};

// ============================================================
// Metrics tracked per cache node
// ============================================================
struct CacheMetrics {
    uint64_t hits        = 0;
    uint64_t misses      = 0;
    uint64_t evictions   = 0;
    uint64_t expirations = 0;
    uint64_t total_puts  = 0;
    uint64_t total_dels  = 0;

    double hit_ratio() const {
        uint64_t total = hits + misses;
        return total == 0 ? 0.0 : static_cast<double>(hits) / total;
    }
};

// ============================================================
// LRUCache class
// ============================================================
class LRUCache {
public:
    // capacity     : max number of keys this node holds
    // default_ttl_s: optional default TTL in seconds (0 = no TTL)
    explicit LRUCache(size_t capacity, uint32_t default_ttl_s = 0);
    ~LRUCache();

    // Disable copy — cache owns raw pointers
    LRUCache(const LRUCache&)            = delete;
    LRUCache& operator=(const LRUCache&) = delete;

    // -------------------------------------------------------
    // Core Operations — all O(1)
    // -------------------------------------------------------

    // GET: returns value if key exists and is not expired
    // Moves accessed node to front of list (most recently used)
    std::optional<std::string> get(const std::string& key);

    // PUT: insert or update key-value pair
    // ttl_seconds = 0 means use default_ttl (or no TTL if default is 0)
    // If cache is full, evicts least recently used entry
    void put(const std::string& key,
             const std::string& value,
             uint32_t ttl_seconds = 0);

    // DELETE: remove key if present, returns true if removed
    bool remove(const std::string& key);

    // -------------------------------------------------------
    // Utility
    // -------------------------------------------------------

    size_t size() const;
    size_t capacity() const;
    bool   empty() const;
    bool   contains(const std::string& key) const;

    // Returns all currently valid (non-expired) keys
    std::vector<std::string> keys() const;

    // Purge all expired keys — called periodically by server
    size_t purge_expired();

    // Snapshot all entries for rebalancing/replication transfer
    // Returns {key, value, remaining_ttl_seconds} tuples
    std::vector<std::tuple<std::string, std::string, uint32_t>> snapshot() const;

    // Bulk load — used when keys are migrated from another node
    void bulk_load(const std::vector<std::tuple<std::string,
                                                 std::string,
                                                 uint32_t>>& entries);

    // Clear entire cache
    void clear();

    // Metrics — returns a copy (thread safe)
    CacheMetrics metrics() const;

    // Memory estimate in bytes (rough)
    size_t estimated_memory_bytes() const;

private:
    // -------------------------------------------------------
    // Internal helpers — caller must hold mutex_
    // -------------------------------------------------------

    // Detach node from its current position in the list
    void detach(ListNode* node);

    // Insert node right after the dummy head (= most recent)
    void attach_front(ListNode* node);

    // Evict the least recently used node (just before dummy tail)
    void evict_lru();

    // Check if a cache entry is expired right now
    bool is_expired(const CacheEntry& entry) const;

    // Build a TimePoint from now + ttl_seconds
    TimePoint make_expiry(uint32_t ttl_seconds) const;

    // -------------------------------------------------------
    // Data members
    // -------------------------------------------------------
    size_t   capacity_;
    uint32_t default_ttl_s_;

    // Dummy sentinel nodes — simplify edge cases
    // head_.next = most recently used
    // tail_.prev = least recently used
    ListNode head_;   // dummy head
    ListNode tail_;   // dummy tail

    // key → pointer into linked list
    std::unordered_map<std::string, ListNode*> map_;

    mutable std::mutex mutex_;
    CacheMetrics       metrics_;
};

} // namespace cache
