/*
 * Agora Player Shell.
 *
 * Connects to ws://<host>/ws and renders image/video commands on a pair
 * of layered <div>s, crossfading between them. Reconnects on drop.
 *
 * Protocol (server -> client):
 *   {"cmd":"show_image","url":"/assets/images/foo.jpg",
 *    "transition":"fade"|"none","duration_ms":600}
 *   {"cmd":"show_video","url":"/assets/videos/bar.mp4",
 *    "loop":true,"muted":false,"transition":"fade","duration_ms":600}
 *   {"cmd":"show_splash","url":"/assets/splash/default.png"}
 *   {"cmd":"stop"}
 *
 * Client -> server (informational):
 *   {"event":"ready"}                          (on initial connect)
 *   {"event":"ended","asset":"<url>"}           (single-play video ended)
 *   {"event":"error","asset":"<url>","msg":""} (load failure)
 */
(function () {
  "use strict";

  const layers = [
    document.getElementById("layer-a"),
    document.getElementById("layer-b"),
  ];
  let activeIdx = 0; // which layer is currently visible

  let ws = null;
  let reconnectTimer = null;

  function log(msg) {
    // Visible in chrome://inspect when debugging; harmless in kiosk.
    try { console.log("[shell]", msg); } catch (_) {}
  }

  function send(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      try { ws.send(JSON.stringify(obj)); } catch (_) {}
    }
  }

  /** Build the media element for a command. */
  function buildElement(cmd) {
    if (cmd.cmd === "show_video") {
      const v = document.createElement("video");
      v.src = cmd.url;
      v.autoplay = true;
      v.muted = !!cmd.muted;
      v.loop = !!cmd.loop;
      v.playsInline = true;
      v.preload = "auto";
      v.addEventListener("ended", () => {
        send({ event: "ended", asset: cmd.url });
      });
      v.addEventListener("error", () => {
        send({ event: "error", asset: cmd.url, msg: "video load failed" });
      });
      return v;
    }
    // Default: image (show_image, show_splash)
    const img = document.createElement("img");
    img.src = cmd.url;
    img.addEventListener("error", () => {
      send({ event: "error", asset: cmd.url, msg: "image load failed" });
    });
    return img;
  }

  /**
   * Swap to the inactive layer with a transition.
   * Returns the new active element so caller can attach handlers if needed.
   */
  function swapTo(cmd) {
    const next = 1 - activeIdx;
    const cur = layers[activeIdx];
    const nxt = layers[next];

    // Clear the destination layer and apply transition timing.
    const durMs = cmd.transition === "none" ? 0 : (cmd.duration_ms || 600);
    nxt.style.setProperty("--transition-ms", durMs + "ms");
    cur.style.setProperty("--transition-ms", durMs + "ms");
    nxt.classList.toggle("no-transition", durMs === 0);
    cur.classList.toggle("no-transition", durMs === 0);

    // Replace contents on the inactive layer first.
    while (nxt.firstChild) nxt.removeChild(nxt.firstChild);
    const el = buildElement(cmd);
    nxt.appendChild(el);

    // Force a layout flush so the transition runs.
    // eslint-disable-next-line no-unused-expressions
    nxt.offsetHeight;

    // Promote the new layer.
    nxt.classList.add("active");
    cur.classList.remove("active");
    activeIdx = next;

    // After the transition, tear down the now-hidden layer to free memory
    // (especially for video).
    const cleanupDelay = Math.max(durMs + 100, 50);
    setTimeout(() => {
      // Make sure we haven't been swapped again in the meantime.
      if (!cur.classList.contains("active")) {
        while (cur.firstChild) {
          const child = cur.firstChild;
          if (child.tagName === "VIDEO") {
            try { child.pause(); child.removeAttribute("src"); child.load(); }
            catch (_) {}
          }
          cur.removeChild(child);
        }
      }
    }, cleanupDelay);

    return el;
  }

  function clearAll() {
    layers.forEach((layer) => {
      layer.classList.remove("active");
      while (layer.firstChild) {
        const child = layer.firstChild;
        if (child.tagName === "VIDEO") {
          try { child.pause(); child.removeAttribute("src"); child.load(); }
          catch (_) {}
        }
        layer.removeChild(child);
      }
    });
  }

  function handleCommand(cmd) {
    log("cmd: " + cmd.cmd);
    switch (cmd.cmd) {
      case "show_image":
      case "show_splash":
        swapTo(cmd);
        break;
      case "show_video":
        swapTo(cmd);
        break;
      case "stop":
        clearAll();
        break;
      default:
        log("unknown cmd: " + cmd.cmd);
    }
  }

  function connect() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const url = proto + "//" + location.host + "/ws";
    log("connecting " + url);
    try {
      ws = new WebSocket(url);
    } catch (e) {
      scheduleReconnect();
      return;
    }
    ws.addEventListener("open", () => {
      log("ws open");
      send({ event: "ready" });
    });
    ws.addEventListener("message", (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); }
      catch (_) { log("bad json: " + ev.data); return; }
      try { handleCommand(msg); }
      catch (e) { log("handler error: " + e); }
    });
    ws.addEventListener("close", () => {
      log("ws close");
      scheduleReconnect();
    });
    ws.addEventListener("error", () => {
      log("ws error");
      // close handler will schedule the reconnect.
    });
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, 1000);
  }

  // Defer until DOM is ready (script is at end of <body>, but be safe).
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", connect);
  } else {
    connect();
  }
})();
