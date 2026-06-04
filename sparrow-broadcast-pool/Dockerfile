# Multi-arch build (amd64 + arm64) via buildx; do not compile on Umbrel device.
FROM --platform=$BUILDPLATFORM rust:1.82-bookworm AS builder

ARG TARGETARCH

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/

RUN apt-get update && apt-get install -y pkg-config libssl-dev \
    && if [ "$TARGETARCH" = "arm64" ]; then \
         apt-get install -y gcc-aarch64-linux-gnu; \
       fi \
    && rm -rf /var/lib/apt/lists/*

RUN if [ "$TARGETARCH" = "arm64" ]; then \
      export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc && \
      cargo build --release --target aarch64-unknown-linux-gnu && \
      cp target/aarch64-unknown-linux-gnu/release/broadcast-pool /tmp/broadcast-pool; \
    else \
      cargo build --release && \
      cp target/release/broadcast-pool /tmp/broadcast-pool; \
    fi

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 app

USER app
WORKDIR /home/app

COPY --from=builder /tmp/broadcast-pool /usr/local/bin/broadcast-pool
COPY src/api/dashboard.html /home/app/dashboard.html

ENV BROADCAST_POOL_DASHBOARD=/home/app/dashboard.html
ENV BROADCAST_POOL_DATA_DIR=/home/app/data
ENV BROADCAST_POOL_WEB_HOST=0.0.0.0
ENV BROADCAST_POOL_ELECTRUM_HOST=0.0.0.0

EXPOSE 8080 50050
CMD ["broadcast-pool", "start", "--foreground"]
