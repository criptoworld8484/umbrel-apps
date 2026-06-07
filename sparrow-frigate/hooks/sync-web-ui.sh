#!/bin/sh
# Umbrel no copia web/ ni entrypoint.sh al actualizar — sincronizar desde app-stores.
set -eu

UI_VERSION="1.5.18"
UMBREL_ROOT="${UMBREL_ROOT:-/home/umbrel/umbrel}"
APP_ID="sparrow-frigate"
APP_DATA_DIR="${UMBREL_ROOT}/app-data/${APP_ID}"
MARKER="${APP_DATA_DIR}/web/.ui-bundle-version"

if [ -f "${APP_DATA_DIR}/web/nginx.conf" ]; then
  rm -f "${MARKER}"
fi
if [ -f "${APP_DATA_DIR}/web/index.html" ] && [ ! -f "${APP_DATA_DIR}/web/public/index.html" ]; then
  rm -f "${MARKER}"
fi
if [ ! -f "${APP_DATA_DIR}/web/public/vendor/qrcode.js" ] && [ ! -f "${APP_DATA_DIR}/web/lib/qrcode-generator.js" ]; then
  rm -f "${MARKER}"
fi

if [ -f "${MARKER}" ] && [ "$(cat "${MARKER}")" = "${UI_VERSION}" ]; then
  exit 0
fi

STORE_DIR=""
for store in "${UMBREL_ROOT}"/app-stores/*/; do
  [ -d "${store}${APP_ID}/web/public" ] || continue
  STORE_DIR="${store}${APP_ID}"
  break
done

if [ -z "${STORE_DIR}" ]; then
  echo "WARN (sparrow-frigate): no se encontró ${APP_ID} en app-stores."
  exit 0
fi

mkdir -p "${APP_DATA_DIR}/web"
find "${APP_DATA_DIR}/web" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
cp -a "${STORE_DIR}/web/." "${APP_DATA_DIR}/web/"
rm -f "${APP_DATA_DIR}/web/index.html" "${APP_DATA_DIR}/web/nginx.conf" 2>/dev/null || true

for f in entrypoint.sh torrc; do
  if [ -f "${STORE_DIR}/${f}" ]; then
    cp -a "${STORE_DIR}/${f}" "${APP_DATA_DIR}/${f}"
  fi
done

echo "${UI_VERSION}" > "${MARKER}"
echo "sparrow-frigate: paquete UI (${UI_VERSION}) sincronizado desde tienda."
