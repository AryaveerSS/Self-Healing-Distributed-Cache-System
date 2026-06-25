#include "lru_cache.hpp"
#include <stdexcept>
#include <algorithm>

namespace cache {

// ============================================================
// Constructor / Destructor
// ============================================================

LRUCache::LRUCache(size_t capacity, uint32_t default_ttl_s)
    : capacity_(capacity)
    , default_ttl_s_(default_ttl_s)
    , head_(CacheEntry{"__head__", "", std::nullopt})
    , tail_(CacheEntry{"__tail__", "", std::nullopt})
{
    if (capacity_ == 0)
        throw std::invalid_argument("LRUCache capacity must be > 0");

    // Link sentinels
    head_.next = &tail_;
    tail_.prev = &head_;
}

LRUCache::~LRUCache() {
    // Walk the list and free all nodes
    ListNode* curr = head_.next;
    while (curr != &tail_) {
        ListNode* next = curr->next;
        delete curr;
        curr = next;
    }
}

// ============================================================
// GET — O(1)
// ============================================================
// Steps:
//   1. Look up key in hash map
//   2. If missing  → miss, return nullopt
//   3. If expired  → remove it, return nullopt
//   4. Move node to front (most recently used)
//   5. Return value
// ============================================================
std::optional<std::string> LRUCache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = map_.find(key);
    if (it == map_.end()) {
        ++metrics_.misses;
        return std::nullopt;
    }

    ListNode* node = it->second;

    // Lazy expiration: check TTL on access
    if (is_expired(node->entry)) {
        detach(node);
        map_.erase(it);
        delete node;
        ++metrics_.misses;
        ++metrics_.expirations;
        return std::nullopt;
    }

    // Move to front = mark as most recently used
    detach(node);
    attach_front(node);

    ++metrics_.hits;
    return node->entry.value;
}

// ============================================================
// PUT — O(1)
// ============================================================
// Steps:
//   1. If key exists → update value, move to front
//   2. If key is new:
//        a. If at capacity → evict LRU (tail.prev)
//        b. Create new node, attach at front
//        c. Insert into map
// ============================================================
void LRUCache::put(const std::string& key,
                   const std::string& value,
                   uint32_t ttl_seconds)
{
    std::lock_guard<std::mutex> lock(mutex_);

    // Determine expiry
    uint32_t effective_ttl = ttl_seconds > 0 ? ttl_seconds : default_ttl_s_;
    std::optional<TimePoint> expiry = std::nullopt;
    if (effective_ttl > 0) {
        expiry = make_expiry(effective_ttl);
    }

    auto it = map_.find(key);
    if (it != map_.end()) {
        // Update existing node in-place
        ListNode* node = it->second;
        node->entry.value      = value;
        node->entry.expires_at = expiry;
        detach(node);
        attach_front(node);
        ++metrics_.total_puts;
        return;
    }

    // New key — evict if needed
    if (map_.size() >= capacity_) {
        evict_lru();
    }

    // Allocate and insert
    CacheEntry entry{key, value, expiry};
    ListNode* node = new ListNode(std::move(entry));
    attach_front(node);
    map_[key] = node;
    ++metrics_.total_puts;
}

// ============================================================
// DELETE — O(1)
// ============================================================
bool LRUCache::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = map_.find(key);
    if (it == map_.end()) return false;

    ListNode* node = it->second;
    detach(node);
    map_.erase(it);
    delete node;
    ++metrics_.total_dels;
    return true;
}

// ============================================================
// Utility
// ============================================================

size_t LRUCache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

size_t LRUCache::capacity() const {
    return capacity_;  // immutable after construction
}

bool LRUCache::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.empty();
}

bool LRUCache::contains(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    return !is_expired(it->second->entry);
}

std::vector<std::string> LRUCache::keys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    result.reserve(map_.size());
    for (auto& [k, node] : map_) {
        if (!is_expired(node->entry))
            result.push_back(k);
    }
    return result;
}

// ============================================================
// Purge Expired Keys — Periodic GC
// ============================================================
// Why two expiration strategies?
//   Lazy (on access): free, but stale keys occupy memory
//   Periodic purge:   small CPU cost, keeps memory clean
// Redis uses both — we do the same.
// ============================================================
size_t LRUCache::purge_expired() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;

    ListNode* curr = head_.next;
    while (curr != &tail_) {
        ListNode* next = curr->next;
        if (is_expired(curr->entry)) {
            map_.erase(curr->entry.key);
            detach(curr);
            delete curr;
            ++metrics_.expirations;
            ++count;
        }
        curr = next;
    }
    return count;
}

// ============================================================
// Snapshot — for replication and rebalancing
// ============================================================
// Returns {key, value, remaining_ttl_seconds}
// remaining_ttl = 0 means no TTL
// ============================================================
std::vector<std::tuple<std::string, std::string, uint32_t>>
LRUCache::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::tuple<std::string, std::string, uint32_t>> result;
    result.reserve(map_.size());

    auto now = std::chrono::steady_clock::now();

    ListNode* curr = head_.next;
    while (curr != &tail_) {
        const auto& entry = curr->entry;
        if (!is_expired(entry)) {
            uint32_t remaining = 0;
            if (entry.expires_at.has_value()) {
                auto diff = entry.expires_at.value() - now;
                auto secs = std::chrono::duration_cast<
                                std::chrono::seconds>(diff).count();
                remaining = secs > 0 ? static_cast<uint32_t>(secs) : 0;
            }
            result.emplace_back(entry.key, entry.value, remaining);
        }
        curr = curr->next;
    }
    return result;
}

// ============================================================
// Bulk Load — used during key migration
// ============================================================
void LRUCache::bulk_load(
    const std::vector<std::tuple<std::string, std::string, uint32_t>>& entries)
{
    for (auto& [key, value, ttl] : entries) {
        put(key, value, ttl);
    }
}

// ============================================================
// Clear
// ============================================================
void LRUCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    ListNode* curr = head_.next;
    while (curr != &tail_) {
        ListNode* next = curr->next;
        delete curr;
        curr = next;
    }
    head_.next = &tail_;
    tail_.prev = &head_;
    map_.clear();
}

// ============================================================
// Metrics
// ============================================================
CacheMetrics LRUCache::metrics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metrics_;
}

// ============================================================
// Memory Estimate
// ============================================================
// Very rough: sizeof node + avg string sizes
// In production you'd track exact heap usage per entry
// ============================================================
size_t LRUCache::estimated_memory_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total = sizeof(*this);
    ListNode* curr = head_.next;
    while (curr != &tail_) {
        total += sizeof(ListNode);
        total += curr->entry.key.capacity();
        total += curr->entry.value.capacity();
        curr = curr->next;
    }
    // unordered_map overhead: ~56 bytes per bucket roughly
    total += map_.bucket_count() * 8;
    return total;
}

// ============================================================
// Private Helpers
// ============================================================

void LRUCache::detach(ListNode* node) {
    // Remove node from wherever it is in the list
    // Sentinel nodes ensure prev/next are never nullptr
    node->prev->next = node->next;
    node->next->prev = node->prev;
    node->prev = nullptr;
    node->next = nullptr;
}

void LRUCache::attach_front(ListNode* node) {
    // Insert immediately after dummy head
    node->next       = head_.next;
    node->prev       = &head_;
    head_.next->prev = node;
    head_.next       = node;
}

void LRUCache::evict_lru() {
    // LRU node is just before the dummy tail
    ListNode* lru = tail_.prev;
    if (lru == &head_) return;  // empty cache

    map_.erase(lru->entry.key);
    detach(lru);
    delete lru;
    ++metrics_.evictions;
}

bool LRUCache::is_expired(const CacheEntry& entry) const {
    if (!entry.expires_at.has_value()) return false;
    return std::chrono::steady_clock::now() >= entry.expires_at.value();
}

LRUCache::TimePoint LRUCache::make_expiry(uint32_t ttl_seconds) const {
    return std::chrono::steady_clock::now()
         + std::chrono::seconds(ttl_seconds);
}

} // namespace cache
