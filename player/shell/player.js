/*
 * Agora Player Shell.
 *
 * Connects to ws://<host>/ws and renders image/video commands on a pair
 * of layered <div>s, crossfading between them. Reconnects on drop.
 *
 * Protocol (server -> client):
 *   {"cmd":"show_image","url":"/assets/images/foo.jpg",
 *    "transition":"fade"|"cut","duration_ms":600}
 *   {"cmd":"show_video","url":"/assets/videos/bar.mp4",
 *    "loop":true,"muted":false,"transition":"fade","duration_ms":600}
 *   {"cmd":"show_video","url":"/assets/videos/bar.mp4",
 *    "loop":false,"loop_count":3,"muted":false,"transition":"fade",
 *    "duration_ms":600}
 *   {"cmd":"show_splash","url":"/assets/splash/default.png"}
 *   {"cmd":"stop"}
 *
 * Transitions: "fade" runs the crossfade for `duration_ms`; "cut" swaps
 * instantly. Missing or unrecognized values fall back to "cut" with a
 * console.warn — the CMS is the source of truth, the shell never guesses.
 *
 * loop_count (videos only): when > 0, the shell plays the video N times
 * in-place (no layer swap between iterations, so the loop is seamless)
 * and then emits {event:"ended", asset, completed_loops:N}. When absent
 * or 0, the cmd.loop field is honored as-is (HTML <video loop>).
 *
 * Client -> server (informational):
 *   {"event":"ready"}                          (on initial connect)
 *   {"event":"ended","asset":"<url>"}           (single-play video ended)
 *   {"event":"ended","asset":"<url>","completed_loops":N}
 *                                               (loop_count video done)
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
      v.playsInline = true;
      v.preload = "auto";

      // loop_count: finite seamless loop driven by ended events. We
      // never set HTML loop=true in this branch because that would
      // suppress the ended event entirely. Instead we count down,
      // currentTime=0 + play() to replay in-place, and emit ended
      // only when the count is exhausted. The user-facing "loop"
      // (HTML attr) is used only when loop_count isn't specified.
      const loopCount = Number.isInteger(cmd.loop_count) && cmd.loop_count > 0
        ? cmd.loop_count : 0;
      if (loopCount > 0) {
        let remaining = loopCount - 1;  // first play counts as iteration 1
        v.loop = false;
        v.addEventListener("ended", () => {
          if (remaining > 0) {
            remaining -= 1;
            try { v.currentTime = 0; v.play(); }
            catch (_) { /* if replay fails, fall through to terminal */ }
            return;
          }
          send({ event: "ended", asset: cmd.url, completed_loops: loopCount });
        });
      } else {
        v.loop = !!cmd.loop;
        v.addEventListener("ended", () => {
          send({ event: "ended", asset: cmd.url });
        });
      }
      v.addEventListener("error", () => {
        send({ event: "error", asset: cmd.url, msg: "video load failed" });
      });
      // Wall-clock anchored seek. When the slideshow engine restarts
      // mid-cycle it tells the shell how far into the video to start
      // (so a slideshow stays in sync across player restarts instead
      // of replaying every video from t=0). Must wait for
      // ``loadedmetadata`` -- HTMLMediaElement.currentTime can't be
      // assigned before duration is known.
      const startOffsetMs = Number.isFinite(cmd.start_offset_ms)
        ? Math.max(0, cmd.start_offset_ms) : 0;
      if (startOffsetMs > 0) {
        v.addEventListener("loadedmetadata", () => {
          try {
            const dur = v.duration;
            const target = startOffsetMs / 1000;
            // Clamp into [0, dur). If dur is unknown (Infinity for live
            // streams) just trust the request. Leaving a small epsilon
            // below dur so the browser doesn't immediately fire ended.
            if (Number.isFinite(dur) && dur > 0) {
              v.currentTime = Math.min(target, Math.max(0, dur - 0.05));
            } else {
              v.currentTime = target;
            }
          } catch (_) { /* ignore -- play from start */ }
        }, { once: true });
      }
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

    // Resolve the transition. CMS-driven; default and unknown both → "cut".
    const KNOWN_TRANSITIONS = ["fade", "cut"];
    let mode = cmd.transition;
    if (!mode) {
      mode = "cut";
    } else if (KNOWN_TRANSITIONS.indexOf(mode) === -1) {
      log("unknown transition '" + mode + "', falling back to cut");
      mode = "cut";
    }
    const durMs = mode === "fade" ? (cmd.duration_ms || 600) : 0;
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
