#!/usr/bin/env node
/* The globe's lifecycle, checked without a browser.
 *
 * Two faults lived here and neither was visible from reading one function:
 *   - the drag handlers are on WINDOW so a drag that leaves the canvas still tracks,
 *     which also means they outlive the canvas unless something removes them;
 *   - replacing the modal body detaches the canvas without stopping the animation loop.
 *
 * Both are lifecycle bugs, so the test drives the lifecycle: open the globe repeatedly
 * the way a reader clicking SITREP cards does, then rip the canvas out from under it.
 *
 * Run: node tests/globe_lifecycle_test.js
 */
"use strict";
const fs = require("fs"), path = require("path");

const HTML = path.join(__dirname, "..", "static", "index.html");
const src = fs.readFileSync(HTML, "utf8");

/* Pull the three functions out of the page rather than duplicating them here, so this
   cannot pass against a copy while the real page is broken. */
function grab(name) {
  const i = src.indexOf("function " + name + "(");
  if (i < 0) throw new Error("cannot find function " + name + " in index.html");
  let depth = 0, started = false;
  for (let j = i; j < src.length; j++) {
    if (src[j] === "{") { depth++; started = true; }
    else if (src[j] === "}") { depth--; if (started && depth === 0) return src.slice(i, j + 1); }
  }
  throw new Error("unbalanced braces reading " + name);
}

const listeners = { mousemove: 0, mouseup: 0 };
let pending = {}, nextId = 1, frames = 0;

global.window = {
  addEventListener: t => { if (t in listeners) listeners[t]++; },
  removeEventListener: t => { if (t in listeners) listeners[t]--; },
  devicePixelRatio: 1,
};
global.requestAnimationFrame = cb => { const id = nextId++; pending[id] = cb; return id; };
global.cancelAnimationFrame = id => { delete pending[id]; };
global.drawGlobe = () => { frames++; };          // the drawing itself is not under test
global.GL = { lon: 0, lat: 0 };

const canvas = () => ({
  isConnected: true, clientWidth: 400, clientHeight: 400, width: 0, height: 0,
  addEventListener() {}, getBoundingClientRect: () => ({ left: 0, top: 0 }),
  getContext: () => ({}),
});

/* Indirect eval, so the declarations land in global scope: a direct eval() under
   "use strict" keeps them in its own scope and the test cannot see them. */
(0, eval)(["globeStop", "globeStart", "globeTick"].map(grab).join("\n"));
const { globeStart, globeStop } = globalThis;
if (typeof globeStart !== "function") throw new Error("globeStart did not reach global scope");

const pump = n => { for (let i = 0; i < n; i++) {
  for (const id of Object.keys(pending)) { const cb = pending[id]; delete pending[id]; cb(); }
} };

let failed = 0;
const check = (name, ok, detail) => {
  console.log((ok ? "PASS " : "FAIL ") + name + (!ok && detail ? "  -> " + detail : ""));
  if (!ok) failed++;
};

/* Three opens, as a reader clicking three cards. */
for (let i = 0; i < 3; i++) globeStart(canvas(), [], null);
check("window drag handlers do not accumulate across reopens",
      listeners.mousemove === 1 && listeners.mouseup === 1,
      `mousemove=${listeners.mousemove} mouseup=${listeners.mouseup}`);

pump(1);
check("exactly one animation loop is scheduled", Object.keys(pending).length === 1,
      Object.keys(pending).length + " loops pending");

frames = 0; pump(1);
check("one frame draws the globe once", frames === 1, frames + " draws in one frame");

globeStop();
check("closing removes both window handlers",
      listeners.mousemove === 0 && listeners.mouseup === 0,
      `mousemove=${listeners.mousemove} mouseup=${listeners.mouseup}`);
check("closing leaves no loop scheduled", Object.keys(pending).length === 0);

/* showModal replacing xbody: the canvas goes, the loop must go with it. */
const doomed = canvas();
globeStart(doomed, [], null);
doomed.isConnected = false;
frames = 0; pump(3);
check("a detached canvas stops its own loop",
      Object.keys(pending).length === 0 && frames === 0,
      `${frames} draws after detach, ${Object.keys(pending).length} pending`);
check("and releases its handlers", listeners.mousemove === 0 && listeners.mouseup === 0);

console.log(failed ? `\n${failed} FAILED` : "\nall pass");
process.exit(failed ? 1 : 0);
