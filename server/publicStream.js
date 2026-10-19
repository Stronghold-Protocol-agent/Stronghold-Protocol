// Socket-boundary transport: the match's public projection and recipient selection stay authoritative.
import { randomBytes } from 'node:crypto';
import { publicSnapshot, diffPublic } from '../shared/publicPatch.js';
import { sendRaw } from './net.js';

// One unchanged snapshot can outlive many recovery requests in an untimed match.
const CACHE_VERSIONS = 16;
const remember = (cache, key, value) => {
  cache.set(key, value);
  if (cache.size > CACHE_VERSIONS) cache.delete(cache.keys().next().value);
};

export class PublicStream {
  constructor() {
    this.epoch = randomBytes(12).toString('hex');
    this.bases = new WeakMap();
    this.latest = new Map();
  }
  prepare(view) {
    try {
      const snapshot = publicSnapshot(view);
      return { view: snapshot, boundary: JSON.stringify([snapshot.phase, snapshot.round, snapshot.setupRevision]),
        fullFrames: new Map(), patches: new WeakMap(), bases: new Map() };
    } catch { return null; }
  }
  full(prepared, version = 1) {
    return JSON.stringify({ ...prepared.view, epoch: this.epoch, version });
  }
  send(playerId, ws, prepared, force = false) {
    if (!prepared) return false;
    this.latest.set(playerId, prepared);
    if (!ws) return false;
    const before = this.bases.get(ws);
    const version = (before?.version || 0) + 1;
    // Recipients sharing the same queued baseline also share the encoding work.
    const key = force || !before || before.prepared.boundary !== prepared.boundary ? `full:${version}` : before;
    const cache = typeof key === 'string' ? prepared.fullFrames : prepared.patches;
    let data = cache.get(key);
    if (!data) {
      data = this.full(prepared, version);
      if (typeof key !== 'string') {
        const ops = diffPublic(before.prepared.view, prepared.view);
        if (ops.length <= 10000 && ops.every((op) => op[1].length <= 64)) {
          const patch = JSON.stringify({ t: 'm.patch', epoch: this.epoch, base: before.version, version, ops });
          if (Buffer.byteLength(patch) < Buffer.byteLength(data)) data = patch;
        }
      }
      if (typeof key === 'string') remember(cache, key, data);
      else cache.set(key, data);
    }
    if (!sendRaw(ws, data)) return false;
    if (!prepared.bases.has(version)) remember(prepared.bases, version, { prepared, version });
    this.bases.set(ws, prepared.bases.get(version));
    return true;
  }
  resync(playerId, ws) {
    const prepared = this.latest.get(playerId);
    return prepared ? this.send(playerId, ws, prepared, true) : false;
  }
}
