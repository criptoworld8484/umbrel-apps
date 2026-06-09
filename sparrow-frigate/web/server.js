#!/usr/bin/env node
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");
const vm = require("vm");

const PORT = Number(process.env.PORT || 3006);
const PUBLIC_DIR = path.join(__dirname, "public");
const QR_LIB_CANDIDATES = [
  path.join(__dirname, "public", "vendor", "qrcode.js"),
  path.join(__dirname, "lib", "qrcode-generator.js"),
];
const ELECTRUM_PORT = process.env.ELECTRUM_PORT || process.env.APP_SPARROW_FRIGATE_ELECTRUM_PORT || "57001";
const LOCAL_HOST = process.env.DEVICE_DOMAIN_NAME || process.env.ELECTRUM_LOCAL_SERVICE || "umbrel.local";
const TOR_HOST = process.env.ELECTRUM_HIDDEN_SERVICE || process.env.APP_SPARROW_FRIGATE_RPC_HIDDEN_SERVICE || "notyetset.onion";
const FRIGATE_HOST = process.env.FRIGATE_STATUS_HOST || process.env.APP_SPARROW_FRIGATE_NODE_IP || "10.21.21.12";
const NETWORK = process.env.FRIGATE_NETWORK || process.env.APP_BITCOIN_NETWORK || "mainnet";
const APP_VERSION = process.env.APP_VERSION || "";

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

let qrFactory = null;

function getQrFactory() {
  if (!qrFactory) {
    let src = null;
    for (const candidate of QR_LIB_CANDIDATES) {
      if (fs.existsSync(candidate)) {
        src = fs.readFileSync(candidate, "utf8");
        break;
      }
    }
    if (!src) {
      throw new Error("QR library not found");
    }
    const sandbox = { module: { exports: {} }, exports: {} };
    vm.runInNewContext(src, sandbox);
    qrFactory = sandbox.module.exports;
  }
  return qrFactory;
}

function qrDataUrl(connectionString) {
  if (!connectionString || connectionString.includes("notyetset")) {
    return null;
  }
  try {
    const svg = generateQrSvg(connectionString);
    return `data:image/svg+xml;base64,${Buffer.from(svg).toString("base64")}`;
  } catch (_err) {
    return null;
  }
}

function enrichConnectionInfo(info) {
  return {
    ...info,
    qrDataUrl: qrDataUrl(info.connectionString),
  };
}

function connectionDetails() {
  const port = String(ELECTRUM_PORT);
  const local = {
    address: LOCAL_HOST,
    port,
    connectionString: `${LOCAL_HOST}:${port}:t`,
    ssl: false,
  };
  const tor = {
    address: TOR_HOST,
    port,
    connectionString: `${TOR_HOST}:${port}:t`,
    ssl: false,
  };
  return {
    local: enrichConnectionInfo(local),
    tor: enrichConnectionInfo(tor),
    network: NETWORK,
    appVersion: APP_VERSION,
    walletHint: `Sparrow → Server → Private Electrum → ${NETWORK}, sin SSL`,
  };
}

function generateQrSvg(text) {
  const qrcode = getQrFactory();
  const qr = qrcode(0, "H");
  qr.addData(text);
  qr.make();
  const moduleCount = qr.getModuleCount();
  const margin = 4;
  const targetSize = 220;
  const cellSize = Math.max(1, Math.floor(targetSize / (moduleCount + margin * 2)));
  const size = moduleCount * cellSize + margin * 2;
  const svg = qr.createSvgTag(cellSize, margin);
  return svg.replace(
    /<svg\b[^>]*>/,
    `<svg width="${targetSize}" height="${targetSize}" viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet">`
  );
}

function checkFrigatePort() {
  return new Promise((resolve) => {
    const socket = net.connect(
      { host: FRIGATE_HOST, port: Number(ELECTRUM_PORT), timeout: 3000 },
      () => {
        socket.end();
        resolve({ listening: true, host: FRIGATE_HOST, port: ELECTRUM_PORT });
      }
    );
    socket.on("error", () => resolve({ listening: false, host: FRIGATE_HOST, port: ELECTRUM_PORT }));
    socket.on("timeout", () => {
      socket.destroy();
      resolve({ listening: false, host: FRIGATE_HOST, port: ELECTRUM_PORT });
    });
  });
}

function sendJson(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function serveStatic(req, res) {
  let urlPath = req.url.split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";
  const filePath = path.join(PUBLIC_DIR, path.normalize(urlPath).replace(/^(\.\.(\/|\\|$))+/, ""));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end();
    return;
  }
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(data);
  });
}

const server = http.createServer(async (req, res) => {
  const urlPath = req.url.split("?")[0];

  if (urlPath === "/health" || urlPath === "/api/v1/ping") {
    sendJson(res, 200, { operational: true });
    return;
  }

  if (urlPath === "/api/v1/electrum-connection-details") {
    sendJson(res, 200, connectionDetails());
    return;
  }

  if (urlPath === "/api/v1/qr.svg") {
    try {
      const params = new URL(req.url, "http://localhost").searchParams;
      const network = params.get("network") === "tor" ? "tor" : "local";
      const details = connectionDetails();
      const info = details[network];
      const value = info.connectionString;
      if (!value || value.includes("notyetset")) {
        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("Tor hidden service not ready");
        return;
      }
      const svg = generateQrSvg(value);
      res.writeHead(200, {
        "Content-Type": "image/svg+xml; charset=utf-8",
        "Cache-Control": "no-cache",
      });
      res.end(svg);
    } catch (err) {
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("QR generation failed");
    }
    return;
  }

  if (urlPath === "/api/v1/status") {
    const frigate = await checkFrigatePort();
    sendJson(res, 200, {
      frigate,
      syncPercent: frigate.listening ? 100 : -2,
      message: frigate.listening
        ? "Electrum server listening"
        : "Connecting to Frigate — revisa logs si tarda",
    });
    return;
  }

  serveStatic(req, res);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Frigate UI listening on :${PORT}`);
});
