# Runtime image only. CI builds the binary with `cargo build --release` on the runner.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 app

COPY broadcast-pool-bin /usr/local/bin/broadcast-pool
COPY src/api/dashboard.html /home/app/dashboard.html
RUN chmod +x /usr/local/bin/broadcast-pool && chown -R app:app /home/app

USER app
WORKDIR /home/app

ENV BROADCAST_POOL_DASHBOARD=/home/app/dashboard.html
ENV BROADCAST_POOL_DATA_DIR=/home/app/data
ENV BROADCAST_POOL_WEB_HOST=0.0.0.0
ENV BROADCAST_POOL_ELECTRUM_HOST=0.0.0.0

EXPOSE 8080 50050
CMD ["broadcast-pool", "start", "--foreground"]
