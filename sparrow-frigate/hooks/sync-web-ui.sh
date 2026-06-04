#!/bin/sh
# Umbrel solo copia docker-compose.yml, exports.sh, torrc y hooks al actualizar.
# La carpeta web/ debe sincronizarse desde la tienda o usarse la imagen ui-* en compose.
set -eu

UI_VERSION="1.5.13"
UMBREL_ROOT="${UMBREL_ROOT:-/home/umbrel/umbrel}"
APP_ID="sparrow-frigate"
APP_DATA_DIR="${UMBREL_ROOT}/app-data/${APP_ID}"
MARKER="${APP_DATA_DIR}/web/.ui-bundle-version"

# UI legada (nginx + index.html en raíz) — forzar resincronización
if [ -f "${APP_DATA_DIR}/web/nginx.conf" ]; then
  rm -f "${MARKER}"
fi
if [ -f "${APP_DATA_DIR}/web/index.html" ] && [ ! -f "${APP_DATA_DIR}/web/public/index.html" ]; then
  rm -f "${MARKER}"
fi

if [ -f "${MARKER}" ] && [ "$(cat "${MARKER}")" = "${UI_VERSION}" ]; then
  exit 0
fi

STORE_WEB=""
for store in "${UMBREL_ROOT}"/app-stores/*/; do
  [ -d "${store}${APP_ID}/web/public" ] || continue
  STORE_WEB="${store}${APP_ID}/web"
  break
done

if [ -z "${STORE_WEB}" ]; then
  echo "WARN (sparrow-frigate): no se encontró web/ en app-stores; usa imagen ui en docker-compose."
  exit 0
fi

mkdir -p "${APP_DATA_DIR}/web"
rm -rf "${APP_DATA_DIR}/web/"*
cp -a "${STORE_WEB}/." "${APP_DATA_DIR}/web/"
rm -f "${APP_DATA_DIR}/web/index.html" "${APP_DATA_DIR}/web/nginx.conf" 2>/dev/null || true
echo "${UI_VERSION}" > "${MARKER}"
echo "sparrow-frigate: UI sincronizada (${UI_VERSION}) desde tienda."
