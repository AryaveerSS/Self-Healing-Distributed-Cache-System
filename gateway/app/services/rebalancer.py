"""
Key Rebalancer — Phase 7

When nodes join, leave, or recover:
1. Build new hash ring
2. Compute key ownership changes
3. Migrate keys from old owner to new owner
4. Update topology in cluster manager

This ensures data is spread uniformly and no data is lost.
"""

import asyncio
from typing import Dict, List, Set, Tuple
import logging
import httpx

from app.hash_ring import HashRing


logger = logging.getLogger(__name__)


class Rebalancer:
    """
    Orchestrates key migration during topology changes.

    Algorithm:
    1. When topology changes, build new ring
    2. For each key, check: new_owner == old_owner?
    3. If changed: snapshot from old owner, bulk-load to new owner
    4. Delete from old owner (optional, depends on policy)
    """

    def __init__(self):
        """Initialize rebalancer."""
        self.in_progress = False
        self.migrated_keys = 0
        self.affected_nodes: Set[str] = set()

    async def rebalance(
        self,
        old_ring: HashRing,
        new_ring: HashRing,
        node_endpoints: Dict[str, Tuple[str, int]],  # node_id -> (host, port)
        cluster_manager,
    ) -> Dict:
        """
        Execute full rebalance after topology change.

        Args:
            old_ring: Hash ring before topology change
            new_ring: Hash ring after topology change
            node_endpoints: Map of node_id -> (host, port)
            cluster_manager: Cluster manager instance

        Returns:
            {
                "status": "completed",
                "migrated_keys": N,
                "affected_nodes": [list],
            }
        """
        if self.in_progress:
            return {"status": "already_in_progress"}

        try:
            self.in_progress = True
            self.migrated_keys = 0
            self.affected_nodes = set()

            logger.info("Starting rebalance...")

            # Get all keys from all nodes
            all_keys = await self._get_all_keys(node_endpoints)
            logger.info(f"Found {len(all_keys)} total keys to check")

            # For each key, check if owner changed
            migrations = {}  # new_owner -> [keys]

            for key in all_keys:
                old_owner = old_ring.get_node(key)
                new_owner = new_ring.get_node(key)

                if old_owner == new_owner:
                    continue  # No migration needed

                if new_owner not in migrations:
                    migrations[new_owner] = []

                migrations[new_owner].append((key, old_owner))
                self.affected_nodes.add(old_owner)
                self.affected_nodes.add(new_owner)

            logger.info(
                f"Need to migrate {sum(len(v) for v in migrations.values())} "
                f"keys across {len(migrations)} nodes"
            )

            # Execute migrations in parallel
            await self._execute_migrations(migrations, node_endpoints)

            logger.info(f"Rebalance complete. Migrated {self.migrated_keys} keys.")

            return {
                "status": "completed",
                "migrated_keys": self.migrated_keys,
                "affected_nodes": list(self.affected_nodes),
            }

        finally:
            self.in_progress = False

    async def _get_all_keys(
        self,
        node_endpoints: Dict[str, Tuple[str, int]],
    ) -> Set[str]:
        """
        Fetch all keys from all nodes.

        Calls GET /keys on each node.
        """
        all_keys = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = [
                self._fetch_keys_from_node(client, node_id, host, port)
                for node_id, (host, port) in node_endpoints.items()
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch keys: {result}")
                else:
                    all_keys.update(result)

        return all_keys

    async def _fetch_keys_from_node(
        self,
        client: httpx.AsyncClient,
        node_id: str,
        host: str,
        port: int,
    ) -> Set[str]:
        """Fetch keys from a single node."""
        try:
            url = f"http://{host}:{port}/keys"
            resp = await client.get(url)

            if resp.status_code == 200:
                data = resp.json()
                return set(data.get("keys", []))
            else:
                logger.warning(f"Failed to get keys from {node_id}: {resp.status_code}")
                return set()

        except Exception as e:
            logger.error(f"Error fetching keys from {node_id}: {e}")
            return set()

    async def _execute_migrations(
        self,
        migrations: Dict[str, List[Tuple[str, str]]],
        node_endpoints: Dict[str, Tuple[str, int]],
    ) -> None:
        """
        Execute all key migrations.

        For each (new_owner, [(key, old_owner), ...]):
          1. Snapshot keys from old_owner
          2. Bulk-load to new_owner
          3. Delete from old_owner (optional)
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = []

            for new_owner, keys_and_owners in migrations.items():
                # Group by old_owner for efficiency
                by_old_owner: Dict[str, List[str]] = {}
                for key, old_owner in keys_and_owners:
                    if old_owner not in by_old_owner:
                        by_old_owner[old_owner] = []
                    by_old_owner[old_owner].append(key)

                # Create migration task for each old_owner
                for old_owner, keys in by_old_owner.items():
                    task = self._migrate_keys(
                        client,
                        old_owner,
                        new_owner,
                        keys,
                        node_endpoints,
                    )
                    tasks.append(task)

            # Execute all migrations in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Migration failed: {result}")
                else:
                    self.migrated_keys += result

    async def _migrate_keys(
        self,
        client: httpx.AsyncClient,
        old_owner: str,
        new_owner: str,
        keys: List[str],
        node_endpoints: Dict[str, Tuple[str, int]],
    ) -> int:
        """
        Migrate a batch of keys from old_owner to new_owner.
        """
        try:
            old_host, old_port = node_endpoints.get(old_owner, ("localhost", 8001))
            new_host, new_port = node_endpoints.get(new_owner, ("localhost", 8002))

            # Step 1: Get snapshot of keys from old owner
            snapshot_url = f"http://{old_host}:{old_port}/snapshot"
            snap_resp = await client.get(snapshot_url)

            if snap_resp.status_code != 200:
                logger.error(f"Failed to snapshot from {old_owner}")
                return 0

            snapshot = snap_resp.json()
            entries = snapshot.get("entries", [])

            # Filter to only the keys we need
            keys_set = set(keys)
            filtered = [e for e in entries if e["key"] in keys_set]

            if not filtered:
                return 0

            # Step 2: Bulk load to new owner
            load_url = f"http://{new_host}:{new_port}/bulk-load"
            load_resp = await client.post(load_url, json={"entries": filtered})

            if load_resp.status_code != 200:
                logger.error(f"Failed to load keys to {new_owner}")
                return 0

            logger.info(
                f"Migrated {len(filtered)} keys from {old_owner} to {new_owner}"
            )

            # Step 3: Optionally delete from old owner
            # (For now, leave in place for safety)

            return len(filtered)

        except Exception as e:
            logger.error(f"Migration error {old_owner}->{new_owner}: {e}")
            return 0
