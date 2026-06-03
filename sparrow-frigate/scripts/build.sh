#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-1.5.3}"
IMAGE="${2:-ghcr.io/criptoworld8484/frigate-umbrel:${VERSION}}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "${DIR}"
command -v docker >/dev/null || { echo "Docker required" >&2; exit 1; }

docker build --platform linux/amd64 -t "${IMAGE}" .
echo "Built ${IMAGE}"
echo "Push: docker push ${IMAGE}"
