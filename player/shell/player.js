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
 *   {"cmd":"show_html","url":"/assets/composed/page.html",
 *    "transition":"<mode>","duration_ms":600}
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
    if (cmd.cmd === "show_html") {
      // Composed slide: render the local HTML bundle in an iframe so the
      // shell document keeps its WebSocket connection. sandbox allows
      // scripts (clock/ticker widgets) but not top-level navigation or
      // popup escape from kiosk.
      const f = document.createElement("iframe");
      f.src = cmd.url;
      f.setAttribute("sandbox", "allow-scripts allow-same-origin");
      f.style.cssText = "width:100%;height:100%;border:0;display:block;background:#000";
      f.addEventListener("error", () => {
        send({ event: "error", asset: cmd.url, msg: "html load failed" });
      });
      return f;
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

    // Hardware-overlay workaround for the Pi: on chromium-rpi the
    // <video> element is promoted to a DRM overlay plane that bypasses
    // the page compositor, so CSS opacity / transform / clip-path on
    // the parent layer can't fade it.  The result is the new asset
    // crossfades in *behind* the still-fully-opaque video plane, and
    // only becomes visible when the cleanup timer (durMs+100) tears
    // the <video> element down — which looks like "the transition
    // didn't run" to the eye.  Softplayer doesn't hit this because
    // its chromium composites video in GL.
    //
    // Snapshot the current video frame to a <canvas> and swap it in
    // for the <video> element BEFORE we kick off the transition.
    // The canvas is page-composited and opacity / transform behave
    // normally.  If drawImage fails (some hw-decoded paths produce
    // an unreadable surface), fall back to releasing the overlay
    // synchronously so the transition at least cuts cleanly instead
    // of holding the old video frame visible.
    if (mode !== "cut" && durMs > 0) {
      _freezeOutgoingVideo(cur);
    }

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
      //
      // CRITICAL: per-mode tx-* classes change transformable
      // properties (transform, clip-path).  Adding them while the
      // layer's `transition:` rule is live causes the browser to
      // run a spurious entry-state animation (e.g. the incoming
      // layer animates from scale(1) → scale(0.9) just to *reach*
      // the zoom entry state) BEFORE the real transition fires on
      // the .active flip.  The user-visible result is a fade then
      // a zoom (two back-to-back animations), not a single combined
      // zoom-and-fade.  We work around by temporarily disabling
      // transitions while we commit the entry-state classes, flushing
      // layout, and only then re-enabling transitions + flipping
      // .active.  "fade" doesn't need this (no entry-state class)
      // and "cut" doesn't transition at all.
      if (mode !== "cut" && mode !== "fade") {
        const txClass = "tx-" + mode;
        cur.classList.add("no-transition");
        nxt.classList.add("no-transition");
        cur.classList.add(txClass);
        nxt.classList.add(txClass);
        cur.classList.add("tx-outgoing");
        nxt.classList.add("tx-incoming");
        // Commit entry states with transitions off.
        // eslint-disable-next-line no-unused-expressions
        nxt.offsetHeight;
        cur.classList.remove("no-transition");
        nxt.classList.remove("no-transition");
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

  /**
   * Replace any <video> in the outgoing layer with a <canvas> snapshot
   * of its current frame.  See the call site in swapTo for the full
   * rationale (Pi hardware-overlay punch-through).  Best-effort:
   * silently no-ops if there's no video, and if drawImage fails the
   * video element is released anyway so the transition isn't blocked
   * by a stuck overlay plane.
   */
  function _freezeOutgoingVideo(layer) {
    if (!layer) return;
    const v = layer.querySelector("video");
    if (!v) return;
    let snapshotOk = false;
    try {
      const w = v.videoWidth || v.clientWidth || layer.clientWidth || 1920;
      const h = v.videoHeight || v.clientHeight || layer.clientHeight || 1080;
      const canvas = document.createElement("canvas");
      canvas.width = w;
      canvas.height = h;
      const ctx = canvas.getContext("2d");
      ctx.drawImage(v, 0, 0, w, h);
      // Inherit the same fit/sizing as <img>/<video> in player.css.
      canvas.style.width = "100%";
      canvas.style.height = "100%";
      canvas.style.objectFit = "contain";
      canvas.style.background = "#000";
      try { v.pause(); v.removeAttribute("src"); v.load(); } catch (_) {}
      layer.replaceChild(canvas, v);
      snapshotOk = true;
    } catch (e) {
      log("freeze snapshot failed: " + e);
    }
    if (!snapshotOk) {
      // drawImage refused (hw overlay frame unreadable).  Release the
      // overlay plane synchronously so the transition isn't masked.
      try { v.pause(); v.removeAttribute("src"); v.load(); } catch (_) {}
      try { layer.removeChild(v); } catch (_) {}
    }
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
      case "show_html":
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
