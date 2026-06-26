"""
Bloom Filter — Phase 8

Probabilistic set membership test with zero false negatives.

Use case:
- Before routing a GET request to cache, check bloom filter
- If filter says key doesn't exist → return 404 immediately (no RPC)
- Saves network RTT for misses (common in read-heavy workloads)

Trade-off:
- False positives: might say key exists when it doesn't (will hit cache, get 404)
- False negatives: NEVER says key is absent when it exists (guaranteed)

Used in: Cassandra, HBase, Google Bigtable
"""

import hashlib
from typing import List
import logging


logger = logging.getLogger(__name__)


class BloomFilter:
    """
    Space-efficient probabilistic filter.

    False positive rate depends on:
    - Size of bit array (bytes)
    - Number of hash functions
    - Number of items added

    Formula: FPR = (1 - e^(-k*n/m))^k
    where k=hash functions, n=items, m=bits

    Typical: 1MB filter with 7 hash functions gives ~1-2% FPR
    """

    def __init__(self, size_bytes: int = 1000000, num_hashes: int = 7):
        """
        Initialize bloom filter.

        Args:
            size_bytes: Size of bit array in bytes (default 1MB)
            num_hashes: Number of hash functions (default 7)
        """
        self.size_bits = size_bytes * 8
        self.num_hashes = num_hashes
        self.bit_array = bytearray(size_bytes)

        logger.info(
            f"Bloom filter initialized: "
            f"{size_bytes} bytes, "
            f"{num_hashes} hash functions"
        )

    def _hashes(self, key: str) -> List[int]:
        """
        Generate N independent hash values for a key.

        Using different seed values ensures hash independence.
        """
        hashes = []
        for i in range(self.num_hashes):
            h = hashlib.sha256(f"{key}|{i}".encode()).digest()
            # Convert first 4 bytes to int, mod by array size
            hash_val = int.from_bytes(h[:4], "big") % self.size_bits
            hashes.append(hash_val)
        return hashes

    def add(self, key: str) -> None:
        """Add key to filter."""
        for bit_pos in self._hashes(key):
            byte_idx = bit_pos // 8
            bit_idx = bit_pos % 8
            self.bit_array[byte_idx] |= 1 << bit_idx

    def might_exist(self, key: str) -> bool:
        """
        Check if key might exist in set.

        Returns:
            False: definitely not in set (safe to skip RPC)
            True:  might be in set (need to check cache)
        """
        for bit_pos in self._hashes(key):
            byte_idx = bit_pos // 8
            bit_idx = bit_pos % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False  # Bit not set → definitely absent
        return True  # All bits set → probably present

    def clear(self) -> None:
        """Clear entire filter."""
        self.bit_array = bytearray(len(self.bit_array))

    def load_from_snapshot(self, keys: List[str]) -> None:
        """
        Rebuild filter from key list.

        Called after rebalancing or on startup.
        """
        self.clear()
        for key in keys:
            self.add(key)
        logger.debug(f"Loaded {len(keys)} keys into bloom filter")
