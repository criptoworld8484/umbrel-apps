(function () {
  "use strict";

  var state = {
    network: "local",
    connection: null,
    syncPercent: -2,
    version: "",
  };

  var els = {};

  function $(id) {
    return document.getElementById(id);
  }

  function setNetwork(mode) {
    state.network = mode;
    var pill = $("toggle-pill");
    if (pill) {
      pill.classList.toggle("translate-x-full", mode === "tor");
    }
    ["btn-local", "btn-tor"].forEach(function (id) {
      var btn = $(id);
      if (!btn) return;
      var active = (id === "btn-local" && mode === "local") || (id === "btn-tor" && mode === "tor");
      btn.classList.toggle("text-white", active);
      btn.classList.toggle("text-slate-800", !active);
      btn.classList.toggle("dark:text-white", active);
    });
    renderConnection();
  }

  function currentInfo() {
    if (!state.connection) return null;
    return state.network === "tor" ? state.connection.tor : state.connection.local;
  }

  function renderConnection() {
    var info = currentInfo();
    var fields = ["address", "port", "ssl", "connection-string"];
    fields.forEach(function (key) {
      var el = $("field-" + key);
      if (!el) return;
      if (!info) {
        el.textContent = "…";
        return;
      }
      if (key === "address") el.textContent = info.address;
      else if (key === "port") el.textContent = String(info.port);
      else if (key === "ssl") el.textContent = info.ssl ? "Enabled" : "Disabled";
      else if (key === "connection-string") el.textContent = info.connectionString;
    });
    renderQr(info && info.connectionString);
  }

  function renderQr(value) {
    var canvas = $("qr-canvas");
    if (!canvas || typeof QRCode === "undefined") return;
    canvas.innerHTML = "";
    if (!value || value.indexOf("notyetset") !== -1) {
      canvas.innerHTML = '<p class="text-sm text-slate-500 p-8">Tor aún no disponible</p>';
      return;
    }
    new QRCode(canvas, {
      text: value,
      width: 220,
      height: 220,
      colorDark: "#0f172a",
      colorLight: "#ffffff",
      correctLevel: QRCode.CorrectLevel.H,
    });
  }

  function copyText(text, btn) {
    if (!text) return;
    navigator.clipboard.writeText(text).then(function () {
      if (!btn) return;
      btn.classList.add("copied");
      var prev = btn.textContent;
      btn.textContent = "Copiado";
      setTimeout(function () {
        btn.classList.remove("copied");
        btn.textContent = prev;
      }, 1500);
      });
  }

  function bindCopyButtons() {
    document.querySelectorAll("[data-copy]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var key = btn.getAttribute("data-copy");
        var info = currentInfo();
        if (!info) return;
        var map = {
          address: info.address,
          port: String(info.port),
          ssl: info.ssl ? "true" : "false",
          "connection-string": info.connectionString,
        };
        copyText(map[key], btn);
      });
    });
  }

  function updateStatus(data) {
    var pct = data.syncPercent;
    state.syncPercent = pct;
    var label = $("sync-label");
    var bar = $("sync-bar");
    if (!label || !bar) return;

    if (pct === -1) {
      label.textContent = "Esperando a que Bitcoin termine de sincronizar…";
      label.classList.add("pulse");
      bar.style.width = "0%";
      return;
    }
    if (pct < 0) {
      label.textContent = "Conectando con Frigate…";
      label.classList.add("pulse");
      bar.style.width = "5%";
      return;
    }
    label.classList.remove("pulse");
    var shown = pct >= 99.99 ? 100 : Math.round(pct);
    label.textContent = shown + "% Sincronizado";
    bar.style.width = shown + "%";

    var dot = $("status-dot");
    var statusText = $("status-text");
    if (data.frigate && data.frigate.listening) {
      if (dot) dot.setAttribute("fill", "#00CD98");
      if (statusText) {
        statusText.textContent = "Running";
        statusText.className = "ml-1 text-green-500 text-lg";
      }
    }
  }

  function fetchJson(url) {
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    });
  }

  function refresh() {
    fetchJson("/api/v1/electrum-connection-details")
      .then(function (data) {
        state.connection = data;
        var net = $("network-badge");
        if (net) net.textContent = (data.network || "signet").toUpperCase();
        renderConnection();
      })
      .catch(function () {});

    fetchJson("/api/v1/status")
      .then(updateStatus)
      .catch(function () {});
  }

  function init() {
    document.documentElement.classList.add("dark");
    $("btn-local") && $("btn-local").addEventListener("click", function () { setNetwork("local"); });
    $("btn-tor") && $("btn-tor").addEventListener("click", function () { setNetwork("tor"); });
    bindCopyButtons();
    refresh();
    setInterval(refresh, 10000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
