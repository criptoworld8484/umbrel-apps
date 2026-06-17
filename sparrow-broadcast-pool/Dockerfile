# Self-contained build for Umbrel Community Store (context = this directory).
FROM rust:1.86-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo build --release && strip target/release/broadcast-pool

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 app

COPY --from=builder /build/target/release/broadcast-pool /usr/local/bin/broadcast-pool
COPY src/api/dashboard.html /home/app/dashboard.html
COPY entrypoint.sh /home/app/entrypoint.sh
RUN chmod +x /home/app/entrypoint.sh && chown -R app:app /home/app

USER app
WORKDIR /home/app

ENV BROADCAST_POOL_DASHBOARD=/home/app/dashboard.html
ENV BROADCAST_POOL_DATA_DIR=/home/app/data
ENV BROADCAST_POOL_WEB_HOST=0.0.0.0
ENV BROADCAST_POOL_ELECTRUM_HOST=0.0.0.0

EXPOSE 8080 50050
ENTRYPOINT ["/home/app/entrypoint.sh"]
