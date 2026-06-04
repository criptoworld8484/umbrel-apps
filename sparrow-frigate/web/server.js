#!/usr/bin/env node
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const net = require("net");

const PORT = Number(process.env.PORT || 3006);
const PUBLIC_DIR = path.join(__dirname, "public");
const ELECTRUM_PORT = process.env.ELECTRUM_PORT || process.env.APP_SPARROW_FRIGATE_ELECTRUM_PORT || "50002";
const LOCAL_HOST = process.env.DEVICE_DOMAIN_NAME || process.env.ELECTRUM_LOCAL_SERVICE || "umbrel.local";
const TOR_HOST = process.env.ELECTRUM_HIDDEN_SERVICE || process.env.APP_SPARROW_FRIGATE_RPC_HIDDEN_SERVICE || "notyetset.onion";
const FRIGATE_HOST = process.env.FRIGATE_STATUS_HOST || process.env.APP_SPARROW_FRIGATE_NODE_IP || "10.21.21.12";
const NETWORK = process.env.FRIGATE_NETWORK || process.env.APP_BITCOIN_NETWORK || "signet";
const APP_VERSION = process.env.APP_VERSION || "";

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

function connectionDetails() {
  const port = String(ELECTRUM_PORT);
  return {
    local: {
      address: LOCAL_HOST,
      port,
      connectionString: `${LOCAL_HOST}:${port}:t`,
      ssl: false,
    },
    tor: {
      address: TOR_HOST,
      port,
      connectionString: `${TOR_HOST}:${port}:t`,
      ssl: false,
    },
    network: NETWORK,
    appVersion: APP_VERSION,
    walletHint: "Sparrow → Server → Private Electrum → Signet, sin SSL",
  };
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
  if (req.url === "/health" || req.url === "/api/v1/ping") {
    sendJson(res, 200, { operational: true });
    return;
  }
  if (req.url === "/api/v1/electrum-connection-details") {
    sendJson(res, 200, connectionDetails());
    return;
  }
  if (req.url === "/api/v1/status") {
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
