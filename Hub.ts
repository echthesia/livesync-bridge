import { Config, FileData } from "./types.ts";
import { Peer, PeerHealth } from "./Peer.ts";
import { PeerStorage } from "./PeerStorage.ts";
import { PeerCouchDB } from "./PeerCouchDB.ts";


export class Hub {
    conf: Config;
    peers = [] as Peer[];
    constructor(conf: Config) {
        this.conf = conf;
    }
    // Aggregate peer health for the /health endpoint. Unhealthy if any peer is
    // not ok, or if no peers were constructed (misconfiguration).
    health(): { ok: boolean; peers: PeerHealth[] } {
        const peers = this.peers.map((p) => p.health());
        return { ok: peers.length > 0 && peers.every((p) => p.ok), peers };
    }
    start() {
        for (const p of this.peers) {
            p.stop();
        }
        this.peers = [];
        for (const peer of this.conf.peers) {
            if (peer.type == "couchdb") {
                const p = new PeerCouchDB(peer, this.dispatch.bind(this));
                this.peers.push(p);
            } else if (peer.type == "storage") {
                const p = new PeerStorage(peer, this.dispatch.bind(this));
                this.peers.push(p);
            } else {
                throw new Error(`Unexpected Peer type: ${(peer as any)?.name} - ${(peer as any)?.type}`);
            }
        }
        for (const p of this.peers) {
            // Fire-and-forget by design (peers start concurrently), but never let a
            // peer's start() reject unhandled — that is fatal in Deno. PeerCouchDB
            // now supervises its own connect loop; this is belt-and-suspenders.
            p.start().catch((e) => {
                console.error(`[Hub] peer "${p.config.name}" start() failed:`, e);
            });
        }
    }

    async dispatch(source: Peer, path: string, data: FileData | false) {
        for (const peer of this.peers) {
            if (peer !== source && (source.config.group ?? "") === (peer.config.group ?? "")) {
                let ret = false;
                if (data === false) {
                    ret = await peer.delete(path);
                } else {
                    ret = await peer.put(path, data);
                }
                if (ret) {
                    // Logger(`  ${data === false ? "-x->" : "--->"} ${peer.config.name} ${path} `)
                } else {
                    // Logger(`        ${peer.config.name} ignored ${path} `)
                }
            }
        }
    }
}

