// Real simulations through the same socket-boundary encoder, with full-view reconstruction checked at every send.
import assert from 'node:assert/strict';
import { Match } from '../../server/match/Match.js';
import { VirtualScheduler } from '../../server/match/scheduler.js';
import { getData } from '../../server/data.js';
import { attachSimClients } from './sim-client.mjs';
import { PublicStream } from '../../server/publicStream.js';
import { PublicReceiver, publicSnapshot } from '../../shared/publicPatch.js';

function makeMatch({ difficulty, seed }) {
  const sched = new VirtualScheduler();
  const h = { sched, onSend: [], onBroadcast: [], ended: null };
  const seats = Array.from({ length: 4 }, (_, i) => ({ seat: i, playerId: `p_${i}`, name: `P${i}`, isBot: false, connected: true }));
  h.m = new Match({ roomCode: 'TEST', mode: 'coop', difficulty, seed, seats, data: getData(), scheduler: sched,
    clientCombat: true, verify: 'off', botRehearsal: 0,
    send: (id, msg) => { for (const fn of h.onSend) fn(id, msg); return true; },
    broadcast: (msg) => { for (const fn of h.onBroadcast) fn(msg); },
    onEnd: (summary) => { h.ended = summary; },
  });
  h.clients = attachSimClients(h);
  h.autoHumans = () => { for (const ps of h.m.players.values()) ps.autoplay = true; };
  h.start = () => h.m.start();
  h.run = (pred, opts) => sched.runUntil(pred, opts);
  return h;
}

export function replayPublicDeltas({ seed = 3, difficulty = 'NORMAL', coverage = false, captureWire = null } = {}) {
  const stream = new PublicStream(), sockets = new Map(), phases = new Set();
  const totals = { fullPublicBytes: 0, deltaPublicBytes: 0, otherBytes: 0, fullFrames: 0, patches: 0, publicFrames: 0 };
  let expected = null;
  const connect = (id) => {
    const receiver = new PublicReceiver();
    const ws = { readyState: 1, bufferedAmount: 0, send(raw) {
      captureWire?.('delta', id, raw);
      const frame = JSON.parse(raw), received = receiver.receive(frame);
      assert.equal(received.resync, false);
      assert.deepEqual(received.view, expected);
      totals.deltaPublicBytes += Buffer.byteLength(raw);
      totals[frame.t === 'm.patch' ? 'patches' : 'fullFrames']++;
    } };
    sockets.set(id, ws);
  };
  for (let i = 0; i < 4; i++) connect(`p_${i}`);
  const h = makeMatch({ difficulty, seed });
  h.autoHumans();
  const send = (id, msg, prepared = null, force = true) => {
    const ws = sockets.get(id);
    if (!ws) return;
    const raw = JSON.stringify(msg);
    captureWire?.('full', id, raw);
    if (msg.t !== 'm.public') {
      totals.otherBytes += Buffer.byteLength(raw);
      captureWire?.('delta', id, raw);
      return;
    }
    expected = publicSnapshot(msg);
    phases.add(expected.phase);
    totals.fullPublicBytes += Buffer.byteLength(JSON.stringify(msg));
    totals.publicFrames++;
    stream.send(id, ws, prepared || stream.prepare(msg), force);
  };
  h.onSend.push((id, msg) => send(id, msg));
  h.onBroadcast.push((msg) => {
    const prepared = msg.t === 'm.public' ? stream.prepare(msg) : null;
    for (const id of sockets.keys()) send(id, msg, prepared, false);
  });
  let boosted = false, watched = false, reconnected = false;
  try {
    h.start();
    assert.ok(h.run(() => {
      const m = h.m;
      if (coverage && !boosted && m.phase === 'PREP' && m.round === 1) {
        boosted = true;
        for (const ps of m.order) {
          ps.lp = 400;
          for (const id of Object.keys(m.gd.raw.bonds || {})) ps.layers[id] = 300;
          // A fixed activated bond guarantees the hidden-round eligibility branch in this coverage fixture.
          ps.bondCountBonus.yanShip = 3; ps.layers.yanShip = 400; ps.recompute();
        }
      }
      if (!watched && m.phase === 'COMBAT') { watched = true; connect('viewer'); m.addSpectator('viewer'); }
      if (!reconnected && m.phase === 'PREP' && m.round === 2) {
        reconnected = true; sockets.delete('p_3'); m.onDisconnect('p_3'); connect('p_3'); m.onReconnect('p_3');
      }
      return h.ended != null;
    }, { maxSteps: 5e6 }));
    assert.equal(h.m.errorCount, 0, JSON.stringify(h.m.errors));
    assert.deepEqual(h.sched.errors, []);
    assert.ok(watched && reconnected, 'the scenario includes late spectating and a reconnect');
    assert.ok(totals.patches > 0);
    return { seed, difficulty, coverage, roundsPassed: h.ended.roundsPassed, phases: [...phases], ...totals,
      fullTotalBytes: totals.fullPublicBytes + totals.otherBytes, deltaTotalBytes: totals.deltaPublicBytes + totals.otherBytes,
      publicReductionPercent: Number((100 * (1 - totals.deltaPublicBytes / totals.fullPublicBytes)).toFixed(2)) };
  } finally { h.m.dispose(); h.clients?.closeAll(); }
}
