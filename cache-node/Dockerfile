# ============================================================
# Stage 1 — Builder
# ============================================================
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libssl-dev \
    libgtest-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# -----------------------------------------------------------
# Download third-party headers
# cpp-httplib  — single header HTTP server/client
# nlohmann/json — single header JSON library
# -----------------------------------------------------------
RUN mkdir -p third_party && \
    curl -sSL https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h \
         -o third_party/httplib.h && \
    curl -sSL https://raw.githubusercontent.com/nlohmann/json/develop/single_include/nlohmann/json.hpp \
         -o third_party/nlohmann/json.hpp --create-dirs

# Copy source
COPY CMakeLists.txt .
COPY src/            src/
COPY tests/          tests/

# Build
RUN cmake -B build \
          -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_TESTS=ON && \
    cmake --build build --parallel $(nproc)

# Run unit tests during build
RUN cd build && ctest --output-on-failure

# ============================================================
# Stage 2 — Runtime (minimal image)
# ============================================================
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1001 cacheuser

WORKDIR /app
COPY --from=builder /build/build/cache_node .

USER cacheuser

# Default environment — override in docker-compose.yml
ENV NODE_ID=node-1          \
    NODE_HOST=0.0.0.0       \
    NODE_PORT=8001          \
    NODE_ZONE=default       \
    CACHE_CAPACITY=1000     \
    DEFAULT_TTL_S=0         \
    CLUSTER_MANAGER=http://gateway:8000 \
    HEARTBEAT_S=5           \
    PURGE_INTERVAL_S=30

EXPOSE 8001

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:${NODE_PORT}/health || exit 1

CMD ["./cache_node"]
