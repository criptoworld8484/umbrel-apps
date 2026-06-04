# Built natively per-arch in CI (amd64 + arm64 runners). Not compiled on Umbrel.
FROM rust:1.82-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo build --release && cp target/release/broadcast-pool /tmp/broadcast-pool

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
