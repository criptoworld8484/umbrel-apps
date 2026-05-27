FROM eclipse-temurin:25-jre-jammy

ARG FRIGATE_VERSION=1.5.3
ARG FRIGATE_BUILD=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    openssl \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://github.com/sparrowwallet/frigate/releases/download/${FRIGATE_VERSION}/frigate-${FRIGATE_VERSION}-x86_64.tar.gz \
    && tar -xzf frigate-${FRIGATE_VERSION}-x86_64.tar.gz \
    && rm frigate-${FRIGATE_VERSION}-x86_64.tar.gz

RUN mkdir -p /data/db /data/cache \
    && chmod -R 755 /opt/frigate

COPY docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 50001 50002

WORKDIR /data

ENTRYPOINT ["/entrypoint.sh"]
CMD ["-n", "testnet4"]