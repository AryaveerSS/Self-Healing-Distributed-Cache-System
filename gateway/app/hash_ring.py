"""
Consistent Hashing Ring — Phase 2

Implements consistent hashing with virtual nodes for uniform key distribution.
When nodes join/leave, only ~k/N keys are reassigned (k=total keys, N=nodes).

This is how DynamoDB, Cassandra, and Chord DHT implement data distribution.
"""

import hashlib
from typing import List, Tuple, Optional, Set
from bisect import bisect_right


class HashRing:
    """
    Consistent hash ring with virtual nodes.

    Key insights:
    - Each physical node maps to M virtual nodes on the ring
    - Virtual nodes ensure uniform distribution even with capacity differences
    - Add/remove nodes → only affected keys migrate
    """

    def __init__(self, nodes: List[str], virtual_nodes: int = 160):
        """
        Initialize ring with physical nodes.

        Args:
            nodes: List of node IDs (e.g., ["node-1", "node-2", "node-3"])
            virtual_nodes: Number of virtual replicas per physical node
                          160 is typical (gives smooth distribution)
        """
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}  # hash -> node_id
        self.sorted_keys: List[int] = []
        self.nodes: Set[str] = set()

        for node in nodes:
            self.add_node(node)

    @staticmethod
    def _hash(key: str) -> int:
        """
        Compute hash using MD5 for good distribution.
        Production systems often use MurmurHash3 or xxHash.
        MD5 is fine for interviews — deterministic, well-distributed.
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        """Add a physical node, creating M virtual nodes."""
        if node_id in self.nodes:
            return  # already exists

        self.nodes.add(node_id)

        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}#{i}"
            hash_val = self._hash(virtual_key)
            self.ring[hash_val] = node_id

        # Re-sort after adding all virtual nodes
        self.sorted_keys = sorted(self.ring.keys())

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual nodes."""
        if node_id not in self.nodes:
            return

        self.nodes.discard(node_id)

        # Remove all virtual nodes for this physical node
        hashes_to_remove = [h for h, n in self.ring.items() if n == node_id]
        for h in hashes_to_remove:
            del self.ring[h]

        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        """
        Find responsible node for a key.

        Algorithm:
        1. Hash the key to a point on the ring
        2. Walk clockwise to find first node
        3. That node is responsible

        Returns None if ring is empty.
        """
        if not self.ring:
            return None

        key_hash = self._hash(key)
        idx = bisect_right(self.sorted_keys, key_hash)

        # Wrap around if needed
        if idx == len(self.sorted_keys):
            idx = 0

        return self.ring[self.sorted_keys[idx]]

    def get_replicas(self, key: str, replication_factor: int = 3) -> List[str]:
        """
        Find all replica nodes for a key.

        Returns primary + replicas in order (first node is primary).

        Example:
            replication_factor=3 → returns [primary, replica1, replica2]

        Important: Replicas must be DIFFERENT physical nodes.
        We skip virtual nodes of the same physical node.
        """
        if not self.ring:
            return []

        key_hash = self._hash(key)
        idx = bisect_right(self.sorted_keys, key_hash)
        if idx == len(self.sorted_keys):
            idx = 0

        replicas: List[str] = []
        seen_physical_nodes: Set[str] = set()

        # Walk around ring until we have enough replicas
        # from different physical nodes
        attempts = 0
        while len(replicas) < replication_factor and attempts < len(self.ring):
            node = self.ring[self.sorted_keys[idx]]
            if node not in seen_physical_nodes:
                replicas.append(node)
                seen_physical_nodes.add(node)

            idx = (idx + 1) % len(self.sorted_keys)
            attempts += 1

        return replicas

    def get_keys_for_node(self, node_id: str) -> List[Tuple[str, int]]:
        """
        Get all (virtual_key, hash_value) pairs for a physical node.

        Used during rebalancing to know which key ranges a node owns.
        """
        result = []
        for hash_val, n in self.ring.items():
            if n == node_id:
                result.append((str(hash_val), hash_val))
        return result

    def get_all_nodes(self) -> List[str]:
        """Return all active physical nodes."""
        return sorted(list(self.nodes))

    def rebalance_after_node_change(self, old_ring: "HashRing") -> dict[str, List[str]]:
        """
        Compute key migrations needed after topology change.

        Returns: {
            "node_id": [keys to migrate to this node],
            ...
        }

        Used in Phase 7 (rebalancer).
        """
        migrations: dict[str, List[str]] = {}

        # For every possible key hash, see if its owner changed
        # In practice, you'd iterate over actual keys in the old ring
        # This is a skeleton for the interview question

        return migrations
