/*
 * Agora Player Shell.
 *
 * Connects to ws://<host>/ws and renders image/video commands on a pair
 * of layered <div>s, crossfading between them. Reconnects on drop.
 *
 * Protocol (server -> client):
 *   {"cmd":"show_image","url":"/assets/images/foo.jpg",
 *    "transition":"<mode>","duration_ms":600}
 *   {"cmd":"show_video","url":"/assets/videos/bar.mp4",
 *    "loop":true,"muted":false,"transition":"<mode>","duration_ms":600}
 *   {"cmd":"show_video","url":"/assets/videos/bar.mp4",
 *    "loop":false,"loop_count":3,"muted":false,"transition":"<mode>",
 *    "duration_ms":600}
 *   {"cmd":"show_splash","url":"/assets/splash/default.png"}
 *   {"cmd":"stop"}
 *
 * Transition modes (<mode> above): "cut" | "fade" | "fade_black" |
 * "dissolve" | "push" | "wipe" | "zoom" — see swapTo() docstring for
 * the per-mode semantics. Missing or unrecognized values fall back to
 * "cut" with a log line; the CMS is the source of truth, the shell
 * never guesses.
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
   *
   * Supported transitions (CMS-driven, see cms.schemas.asset.SLIDE_TRANSITIONS):
   *
   *   cut          Instant swap (durMs forced to 0).
   *   fade         Crossfade (opacity-only). Default if mode is missing.
   *   fade_black   Two-stage: outgoing fades to 0 over durMs/2, then
   *                incoming fades up over durMs/2. Gives a brief black
   *                pause; "scene change" feel.
   *   dissolve     Crossfade + outgoing scales 1.00 -> 1.05 (Ken Burns).
   *   push         Incoming slides in from the right, outgoing slides
   *                off to the left.
   *   wipe         Incoming reveals L->R via clip-path inset.
   *   zoom         Incoming scales 0.9 -> 1.0 + opacity 0 -> 1.
   *
   * Unknown values fall back to cut.
   */
  function swapTo(cmd) {
    const next = 1 - activeIdx;
    const cur = layers[activeIdx];
    const nxt = layers[next];

    const KNOWN_TRANSITIONS = [
      "cut", "fade", "fade_black", "dissolve", "push", "wipe", "zoom",
    ];
    let mode = cmd.transition;
    if (!mode) {
      mode = "cut";
    } else if (KNOWN_TRANSITIONS.indexOf(mode) === -1) {
      log("unknown transition '" + mode + "', falling back to cut");
      mode = "cut";
    }
    const durMs = mode === "cut" ? 0 : (cmd.duration_ms || 600);

    // Clear any per-mode classes left over from the previous transition.
    const TX_CLASSES = [
      "tx-fade", "tx-fade_black", "tx-dissolve", "tx-push", "tx-wipe", "tx-zoom",
      "tx-incoming", "tx-outgoing",
    ];
    [cur, nxt].forEach((l) => TX_CLASSES.forEach((c) => l.classList.remove(c)));

    nxt.style.setProperty("--transition-ms", durMs + "ms");
    cur.style.setProperty("--transition-ms", durMs + "ms");
    nxt.classList.toggle("no-transition", durMs === 0);
    cur.classList.toggle("no-transition", durMs === 0);

    // Replace contents on the inactive layer first.
    while (nxt.firstChild) nxt.removeChild(nxt.firstChild);
    const el = buildElement(cmd);
    nxt.appendChild(el);

    if (mode === "fade_black" && durMs > 0) {
      // Two-stage sequenced fade through the black stage background.
      // Stage 1: outgoing fades to 0 over durMs/2 (incoming stays at 0).
      // Stage 2: incoming fades to 1 over durMs/2.
      const half = Math.max(1, Math.floor(durMs / 2));
      cur.style.setProperty("--transition-ms", half + "ms");
      nxt.style.setProperty("--transition-ms", half + "ms");
      // Don't promote the incoming layer yet — first phase is the
      // outgoing fade-out only.
      cur.classList.remove("active");
      // After stage 1, promote the incoming layer (which is still at
      // opacity 0) to fade it up.
      setTimeout(() => {
        nxt.classList.add("active");
      }, half);
    } else {
      // Single-stage transitions: tag both layers with the mode class
      // (so per-mode CSS rules apply) and flip .active in a layout
      // flush so the browser sees both states and animates between.
      if (mode !== "cut" && mode !== "fade") {
        const txClass = "tx-" + mode;
        cur.classList.add(txClass);
        nxt.classList.add(txClass);
        cur.classList.add("tx-outgoing");
        nxt.classList.add("tx-incoming");
      }

      // Force a layout flush so the transition runs.
      // eslint-disable-next-line no-unused-expressions
      nxt.offsetHeight;

      // Promote the new layer.
      nxt.classList.add("active");
      cur.classList.remove("active");
    }
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
        // Strip mode classes from the now-hidden layer so the next
        // transition starts from a clean slate.
        TX_CLASSES.forEach((c) => cur.classList.remove(c));
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
