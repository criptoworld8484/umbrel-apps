FROM debian:12-slim

ARG FRIGATE_VERSION=1.5.3
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) ARCH=x86_64; DEB_ARCH=amd64 ;; \
      arm64) ARCH=aarch64; DEB_ARCH=arm64 ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/frigate.tar.gz \
      "https://github.com/sparrowwallet/frigate/releases/download/${FRIGATE_VERSION}/frigate-${FRIGATE_VERSION}-${ARCH}.tar.gz"; \
    tar -xzf /tmp/frigate.tar.gz -C /opt; \
    rm -f /tmp/frigate.tar.gz

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER 1000
WORKDIR /data
EXPOSE 50002

ENTRYPOINT ["/entrypoint.sh"]
