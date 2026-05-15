FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    libcurl4-openssl-dev \
    libssl-dev \
    libyaml-cpp-dev \
    libsasl2-dev \
    python3 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Build mongo-c-driver (libmongoc + libbson)
WORKDIR /deps
RUN wget -q https://github.com/mongodb/mongo-c-driver/releases/download/1.28.0/mongo-c-driver-1.28.0.tar.gz \
    && tar xzf mongo-c-driver-1.28.0.tar.gz \
    && cd mongo-c-driver-1.28.0 \
    && mkdir -p cmake_build && cd cmake_build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
    && make -j$(nproc) && make install

# Build mongo-cxx-driver (mongocxx + bsoncxx)
RUN wget -q https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.9.0/mongo-cxx-driver-r3.9.0.tar.gz \
    && tar xzf mongo-cxx-driver-r3.9.0.tar.gz \
    && cd mongo-cxx-driver-r3.9.0 \
    && mkdir -p cmake_build && cd cmake_build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_PREFIX_PATH=/usr/local \
    && make -j$(nproc) && make install

# Build mg-clickhouse
WORKDIR /build
COPY . .

RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH=/usr/local && \
    make -j$(nproc)

# --- Production runtime image ---
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4 \
    libssl3 \
    libyaml-cpp0.7 \
    libsasl2-2 \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd -r mgch && useradd -r -g mgch -d /var/lib/mg-clickhouse -s /sbin/nologin mgch

COPY --from=builder /usr/local/lib/ /usr/local/lib/
COPY --from=builder /build/build/mg_clickhouse /usr/local/bin/mg_clickhouse
COPY config/mg-clickhouse.yaml /etc/mg-clickhouse/mg-clickhouse.yaml

RUN ldconfig \
    && mkdir -p /var/lib/mg-clickhouse/resume_tokens \
    && mkdir -p /var/log/mg-clickhouse \
    && chown -R mgch:mgch /var/lib/mg-clickhouse /var/log/mg-clickhouse

USER mgch
EXPOSE 9090

# tini ensures proper signal forwarding for graceful shutdown
ENTRYPOINT ["tini", "--", "/usr/local/bin/mg_clickhouse"]
CMD ["/etc/mg-clickhouse/mg-clickhouse.yaml"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/api/v1/status || exit 1
