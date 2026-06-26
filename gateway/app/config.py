from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Gateway configuration from environment variables."""

    # Network
    host: str = "0.0.0.0"
    port: int = 8000

    # Cluster
    heartbeat_timeout_s: int = 10  # mark SUSPECT after this many seconds
    heartbeat_check_interval_s: int = 2  # check timeouts every N seconds
    replication_factor: int = 3

    # Circuit breaker
    circuit_breaker_failure_threshold: int = 5  # open after N failures
    circuit_breaker_timeout_s: int = 30  # half-open probe after N seconds

    # Request timeout to cache nodes
    node_request_timeout_s: int = 5

    # Bloom filter (Phase 8)
    bloom_filter_size_bytes: int = 1000000  # 1MB default

    # TTL for read-after-write pinning
    read_after_write_pin_ms: int = 500

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
