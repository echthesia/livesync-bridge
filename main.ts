import { defaultLoggerEnv } from "./lib/src/common/logger.ts";
import { LOG_LEVEL_DEBUG } from "./lib/src/common/logger.ts";
import { Hub } from "./Hub.ts";
import { Config } from "./types.ts";
import { parseArgs } from "jsr:@std/cli";

// Last-resort safety net for a long-running sync daemon. A transient backend
// hiccup — e.g. CouchDB returning a non-JSON body while still warming up at
// boot — can surface as an unhandled promise rejection from deep inside
// PouchDB's fire-and-forget init, which Deno treats as fatal. That previously
// crash-looped the bridge into systemd's start limit and left it down silently
// for days. Log and keep running instead; the per-peer supervisor re-establishes
// the actual sync.
globalThis.addEventListener("unhandledrejection", (event) => {
    event.preventDefault();
    console.error("[LSB] Unhandled rejection (kept alive):", event.reason);
});

const KEY = "LSB_"
defaultLoggerEnv.minLogLevel = LOG_LEVEL_DEBUG;
const configFile = Deno.env.get(`${KEY}CONFIG`) || "./dat/config.json";

console.log("LiveSync Bridge is now starting...");
let config: Config = { peers: [] };
const flags = parseArgs(Deno.args, {
    boolean: ["reset"],
    // string: ["version"],
    default: { reset: false },
});
if (flags.reset) {
    localStorage.clear();
}
try {
    const confText = await Deno.readTextFile(configFile);
    config = JSON.parse(confText);
} catch (ex) {
    console.error("Could not parse configuration!");
    console.error(ex);
}
console.log("LiveSync Bridge is now started!");
const hub = new Hub(config);
hub.start();