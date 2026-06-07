(function () {
  "use strict";

  var COPY_ICON =
    "M10.9336 18H4.21875C2.66789 18 1.40625 16.7384 1.40625 15.1875V5.66016C1.40625 4.1093 2.66789 2.84766 4.21875 2.84766H10.9336C12.4845 2.84766 13.7461 4.1093 13.7461 5.66016V15.1875C13.7461 16.7384 12.4845 18 10.9336 18ZM4.21875 4.25391C3.44339 4.25391 2.8125 4.8848 2.8125 5.66016V15.1875C2.8125 15.9629 3.44339 16.5938 4.21875 16.5938H10.9336C11.709 16.5938 12.3398 15.9629 12.3398 15.1875V5.66016C12.3398 4.8848 11.709 4.25391 10.9336 4.25391H4.21875ZM16.5586 13.4297V2.8125C16.5586 1.26164 15.297 0 13.7461 0H5.94141C5.55304 0 5.23828 0.314758 5.23828 0.703125C5.23828 1.09149 5.55304 1.40625 5.94141 1.40625H13.7461C14.5215 1.40625 15.1523 2.03714 15.1523 2.8125V13.4297C15.1523 13.8181 15.4671 14.1328 15.8555 14.1328C16.2438 14.1328 16.5586 13.8181 16.5586 13.4297Z";
  var COPIED_ICON =
    "M4.21875 18H10.9336C12.4845 18 13.7461 16.7384 13.7461 15.1875V5.66016C13.7461 4.1093 12.4845 2.84766 10.9336 2.84766H4.21875C2.66789 2.84766 1.40625 4.1093 1.40625 5.66016V15.1875C1.40625 16.7384 2.66789 18 4.21875 18ZM16.5586 2.8125V13.4297C16.5586 13.8181 16.2438 14.1328 15.8555 14.1328C15.4671 14.1328 15.1523 13.8181 15.1523 13.4297V2.8125C15.1523 2.03714 14.5215 1.40625 13.7461 1.40625H5.94141C5.55304 1.40625 5.23828 1.09149 5.23828 0.703125C5.23828 0.314758 5.55304 0 5.94141 0H13.7461C15.297 0 16.5586 1.26164 16.5586 2.8125Z";

  var state = {
    network: "local",
    connection: null,
    syncPercent: -2,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function setNetwork(mode) {
    state.network = mode;
    var pill = $("toggle-pill");
    if (pill) {
      pill.classList.toggle("translate-x-full", mode === "tor");
    }
    var btnLocal = $("btn-local");
    var btnTor = $("btn-tor");
    if (btnLocal) {
      btnLocal.classList.toggle("text-white", mode === "local");
      btnLocal.classList.toggle("duration-500", mode === "local");
      btnLocal.classList.toggle("text-slate-800", mode !== "local");
    }
    if (btnTor) {
      btnTor.classList.toggle("text-white", mode === "tor");
      btnTor.classList.toggle("duration-500", mode === "tor");
      btnTor.classList.toggle("text-slate-800", mode !== "tor");
    }
    renderConnection();
  }

  function currentInfo() {
    if (!state.connection) return null;
    return state.network === "tor" ? state.connection.tor : state.connection.local;
  }

  function setFieldsVisible(visible) {
    ["address", "port", "ssl"].forEach(function (key) {
      var wrap = $("field-" + key + "-wrap");
      var placeholder = $("placeholder-" + key);
      if (wrap) wrap.classList.toggle("hidden", !visible);
      if (placeholder) placeholder.classList.toggle("hidden", visible);
    });
  }

  function renderConnection() {
    var info = currentInfo();
    if (!info) {
      setFieldsVisible(false);
      renderQr(null);
      return;
    }

    setFieldsVisible(true);
    $("field-address").value = info.address || "";
    $("field-port").value = String(info.port || "");
    $("field-ssl").value = "Disabled";
    renderQr(info);
  }

  function renderQr(info) {
    var img = $("qr-image");
    var logo = $("qr-logo");
    var fallback = $("qr-fallback");
    if (!img) return;

    var value = info && info.connectionString;
    var dataUrl = info && info.qrDataUrl;

    if (!value || value.indexOf("notyetset") !== -1 || !dataUrl) {
      img.classList.add("hidden");
      img.removeAttribute("src");
      if (logo) logo.classList.add("hidden");
      if (fallback) fallback.classList.remove("hidden");
      return;
    }

    if (fallback) fallback.classList.add("hidden");
    img.src = dataUrl;
    img.classList.remove("hidden");
    if (logo) logo.classList.remove("hidden");
  }

  function copyFromInput(input, btn) {
    if (!input) return;
    input.select();
    input.setSelectionRange(0, 99999);
    var copied = false;
    try {
      copied = document.execCommand("copy");
    } catch (e) {
      copied = false;
    }
    if (!copied && navigator.clipboard) {
      navigator.clipboard.writeText(input.value);
      copied = true;
    }
    if (!copied || !btn) return;

    var path = btn.querySelector(".copy-path");
    if (path) {
      path.setAttribute("d", COPIED_ICON);
      path.setAttribute("fill", "#00CD98");
      path.setAttribute("fill-rule", "evenodd");
      path.setAttribute("clip-rule", "evenodd");
    }
    window.setTimeout(function () {
      input.blur();
      if (window.getSelection) window.getSelection().removeAllRanges();
      if (path) {
        path.setAttribute("d", COPY_ICON);
        path.setAttribute("fill", "#C3C6D1");
        path.removeAttribute("fill-rule");
        path.removeAttribute("clip-rule");
      }
    }, 1000);
  }

  function bindCopyButtons() {
    document.querySelectorAll(".copy-icon-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var key = btn.getAttribute("data-copy");
        var input = $("field-" + key);
        copyFromInput(input, btn);
      });
    });
  }

  function updateStatus(data) {
    var pct = data.syncPercent;
    state.syncPercent = pct;
    var label = $("sync-label");
    var bar = $("sync-bar");
    if (!label || !bar) return;

    var progress = Math.max(0, Math.min(100, pct));
    bar.style.width = progress + "%";
    bar.setAttribute("aria-valuenow", String(progress));

    label.classList.remove("animate-pulse");
    if (pct === -1) {
      label.textContent = "Waiting for Bitcoin Node to finish syncing...";
      label.classList.add("animate-pulse");
      return;
    }
    if (pct < 0) {
      label.textContent = "Connecting to Frigate server...";
      label.classList.add("animate-pulse");
      return;
    }
    var shown = pct >= 99.99 ? 100 : Math.round(pct);
    label.innerHTML =
      "<span>" + shown + "%</span><span class=\"align-self-end ml-1\">Synchronized</span>";

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
        var version = $("version-text");
        if (version) {
          version.textContent = data.appVersion || data.network || "...";
        }
        renderConnection();
      })
      .catch(function () {});

    fetchJson("/api/v1/status")
      .then(updateStatus)
      .catch(function () {});
  }

  function init() {
    $("btn-local") &&
      $("btn-local").addEventListener("click", function () {
        setNetwork("local");
      });
    $("btn-tor") &&
      $("btn-tor").addEventListener("click", function () {
        setNetwork("tor");
      });
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
