FROM debian:12-slim

ARG FRIGATE_VERSION=1.5.3
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      ca-certificates curl nginx gettext-base \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) ARCH=x86_64 ;; \
      arm64) ARCH=aarch64 ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/frigate.tar.gz \
      "https://github.com/sparrowwallet/frigate/releases/download/${FRIGATE_VERSION}/frigate-${FRIGATE_VERSION}-${ARCH}.tar.gz"; \
    tar -xzf /tmp/frigate.tar.gz -C /opt; \
    rm -f /tmp/frigate.tar.gz

COPY entrypoint.sh /entrypoint.sh
COPY start-all.sh /start-all.sh
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/index.html /usr/share/nginx/html/index.html
RUN chmod +x /entrypoint.sh /start-all.sh \
    && mkdir -p /var/log/nginx /var/lib/nginx /run/nginx \
    && chown -R 1000:1000 /var/log/nginx /var/lib/nginx /run/nginx /etc/nginx

USER 1000
WORKDIR /data
EXPOSE 3006 50002

ENTRYPOINT ["/start-all.sh"]
