import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startServer } from '../../server/index.js';
import { StubMatch } from '../../server/match/StubMatch.js';
import { TestClient } from '../helpers/wsClient.js';
import { PublicReceiver } from '../../shared/publicPatch.js';

const ok = async (c, msg) => {
  const reply = await c.request(msg);
  assert.equal(reply.t, 'ok', JSON.stringify(reply));
};
const stripClock = ({ serverNow, ...rest }) => rest;

async function fixture(t, extra = {}) {
  const srv = await startServer({ port: 0, host: '127.0.0.1', quiet: true, assetIndex: false, studio: false,
    publicDeltas: true, seedFn: () => 123, heavyBurst: 40, ...extra });
  const clients = [];
  t.after(async () => { await Promise.all(clients.map((c) => c.terminate())); await srv.close(); });
  const connect = async (name, token, version = 2) => {
    const c = await TestClient.connect(`ws://127.0.0.1:${srv.port}/ws`);
    clients.push(c);
    const welcome = await c.hello(name, token, { version });
    Object.assign(c, { id: welcome.playerId, token: welcome.token, public: new PublicReceiver() });
    return c;
  };
  return { srv, connect, clients };
}
async function publicUntil(c, predicate = () => true) {
  for (let i = 0; i < 100; i++) {
    const frame = await c.waitFor(null, (m) => m.t === 'm.public' || m.t === 'm.patch');
    const received = c.public.receive(frame);
    assert.equal(received.resync, false, JSON.stringify(frame));
    assert.ok(received.view);
    if (predicate(received.view)) return { frame, view: received.view };
  }
  assert.fail('public predicate never reached');
}

test('enabled real sockets: version guard, recipient projections, gap recovery, setup, spectator and reconnect', { timeout: 20000 }, async (t) => {
  const { srv, connect, clients } = await fixture(t);
  assert.equal((await (await fetch(`${srv.url}/healthz`)).json()).version, 2);
  const old = await TestClient.connect(`ws://127.0.0.1:${srv.port}/ws`); clients.push(old);
  for (const version of [undefined, 1]) {
    const refused = await old.request({ t: 'hello', name: 'Old', version });
    assert.equal(refused.t, 'error'); assert.match(refused.detail, /version mismatch/);
  }
  const negotiated = await old.hello('Modern', null, { version: 1, versions: [1, 2] });
  assert.equal(negotiated.version, 2);
  const host = await connect('Host'), guest = await connect('Guest');
  await ok(host, { t: 'room.create', mode: 'coop', difficulty: 'HARD' });
  const state = await host.waitFor('room.state');
  await ok(guest, { t: 'room.join', code: state.code });
  await ok(guest, { t: 'room.ready', ready: true });
  await ok(host, { t: 'room.start' });
  const initial = await publicUntil(host);
  await publicUntil(guest);
  assert.equal(initial.frame.t, 'm.public');
  const room = srv.lobby.rooms.get(state.code), m = room.match;
  await ok(host, { t: 'g.infoReady', setupRevision: 0 });
  const changed = await publicUntil(host, (p) => p.players.some((x) => x.playerId === host.id && x.ready));
  assert.equal(changed.frame.t, 'm.patch');
  assert.deepEqual(stripClock(changed.view), stripClock(m.publicView()));
  const spectator = await connect('Viewer');
  await ok(spectator, { t: 'room.spectate', code: state.code });
  assert.equal((await publicUntil(spectator)).frame.t, 'm.public');
  assert.ok(!spectator.log.some((p) => p.t === 'm.private'));
  // Skip one frame, then prove that recovery does not invoke the combat reconnect path.
  await ok(host, { t: 'room.rerollSetup', setupRevision: 0 });
  await host.waitFor('m.patch');
  await ok(host, { t: 'room.cancelReroll', voteId: m.setupVote.id });
  const gap = await host.waitFor('m.patch');
  assert.equal(host.public.receive(gap).resync, true);
  const privateBefore = host.log.filter((p) => p.t === 'm.private').length;
  const reconnect = m.onReconnect; let reconnects = 0;
  m.onReconnect = (...args) => { reconnects++; return reconnect.apply(m, args); };
  await ok(host, { t: 'm.resync' });
  const recovered = await publicUntil(host);
  assert.equal(recovered.frame.t, 'm.public');
  assert.deepEqual(stripClock(recovered.view), stripClock(m.publicView()));
  assert.equal(reconnects, 0);
  assert.equal(host.log.filter((p) => p.t === 'm.private').length, privateBefore);
  srv.lobby.matchSend(room, room.matchCtx, guest.id, { ...m.publicView(), recipientOnly: 'guest' });
  await publicUntil(guest, (p) => p.recipientOnly === 'guest');
  await ok(guest, { t: 'm.resync' });
  assert.equal((await publicUntil(guest)).view.recipientOnly, 'guest');
  assert.ok(!host.log.some((p) => p.recipientOnly === 'guest'));
  // Opening-reroll proposals retain the match's existing 300 ms cooldown.
  await new Promise((resolve) => setTimeout(resolve, 310));
  await ok(host, { t: 'room.rerollSetup', setupRevision: 0 });
  await publicUntil(host, (p) => p.rerollVote != null);
  await ok(guest, { t: 'g.rerollVote', voteId: m.setupVote.id, agree: true });
  assert.equal((await publicUntil(host, (p) => p.setupRevision === 1)).frame.t, 'm.public');
  await guest.terminate();
  const resumed = await connect('Guest', guest.token);
  assert.equal((await publicUntil(resumed)).frame.t, 'm.public');
  await resumed.waitFor('m.private');
  await ok(host, { t: 'g.infoReady', setupRevision: 1 });
  await ok(resumed, { t: 'g.infoReady', setupRevision: 1 });
  assert.equal((await publicUntil(host, (p) => p.phase === 'BAND_DRAFT')).frame.t, 'm.public');
  assert.ok(!spectator.log.some((p) => p.t === 'm.private'));
});

test('default real socket protocol and public frames remain version 1', { timeout: 10000 }, async (t) => {
  const { srv, connect } = await fixture(t, { publicDeltas: false });
  assert.equal((await (await fetch(`${srv.url}/healthz`)).json()).version, 1);
  const host = await connect('Host', null, 1);
  await ok(host, { t: 'room.create', mode: 'solo', difficulty: 'FUNNY' });
  await ok(host, { t: 'room.start' });
  const full = await host.waitFor('m.public');
  assert.ok(!('epoch' in full) && !('version' in full));
  assert.equal((await host.request({ t: 'm.resync' })).t, 'error');
  assert.ok(!host.log.some((p) => p.t === 'm.patch'));
});

test('permanent spectator churn releases public projections while disconnected members retain recovery', { timeout: 20000 }, async (t) => {
  const { srv, connect } = await fixture(t);
  const host = await connect('Host'), guest = await connect('Guest');
  await ok(host, { t: 'room.create', mode: 'coop', difficulty: 'HARD' });
  const state = await host.waitFor('room.state');
  await ok(guest, { t: 'room.join', code: state.code });
  await ok(guest, { t: 'room.ready', ready: true });
  await ok(host, { t: 'room.start' });
  const initial = await publicUntil(host);
  await publicUntil(guest);
  const room = srv.lobby.rooms.get(state.code), stream = room.matchCtx.publicStream;
  const current = [host.id, guest.id].sort();
  for (let i = 0; i < 12; i++) {
    const viewer = await connect(`Viewer${i}`);
    await ok(viewer, { t: 'room.spectate', code: state.code });
    await publicUntil(viewer);
    const socket = srv.lobby.registry.byId(viewer.id).ws;
    assert.equal(stream.latest.size, 3);
    assert.equal(stream.bases.has(socket), true);
    if (i % 2) await ok(host, { t: 'room.removeSpectator', playerId: viewer.id });
    else await ok(viewer, { t: 'room.leave' });
    assert.deepEqual([...stream.latest.keys()].sort(), current);
    assert.equal(stream.bases.has(socket), false, 'an open departed socket no longer retains its baseline');
    await viewer.terminate();
  }
  await guest.terminate();
  await host.waitFor('room.state', (p) => p.seats.some((s) => s?.playerId === guest.id && !s.connected));
  assert.deepEqual([...stream.latest.keys()].sort(), current, 'a disconnected current member remains resumable');
  const resumed = await connect('Guest', guest.token);
  const recovered = await publicUntil(resumed);
  assert.equal(recovered.frame.t, 'm.public');
  assert.equal(recovered.frame.epoch, initial.frame.epoch);
  assert.deepEqual([...stream.latest.keys()].sort(), current);
  await ok(resumed, { t: 'room.leave' });
  assert.deepEqual([...stream.latest.keys()], [host.id], 'permanent player departure also releases recovery state');
});

test('terminal replay is a complete view and the next match has a fresh epoch', { timeout: 10000 }, async (t) => {
  const { srv, connect } = await fixture(t, { MatchClass: StubMatch });
  const host = await connect('Host');
  await ok(host, { t: 'room.create', mode: 'solo', difficulty: 'FUNNY' });
  const state = await host.waitFor('room.state');
  await ok(host, { t: 'room.start' });
  const initial = await publicUntil(host);
  await ok(host, { t: 'g.infoReady' });
  await host.waitFor('m.result');
  const room = srv.lobby.rooms.get(state.code);
  assert.equal(room.replay.publicFrame, undefined);
  assert.equal(JSON.parse(room.replay.publicFrames.get(host.id)).t, 'm.public');
  const finished = await publicUntil(host, (p) => p.phase === 'RESULT');
  await ok(host, { t: 'm.resync' });
  assert.ok((await publicUntil(host)).frame.version > finished.frame.version);
  await host.terminate();
  assert.equal(room.replay.publicStream.latest.has(host.id), true, 'a disconnected recipient retains its terminal replay');
  const resumed = await connect('Host', host.token);
  const terminal = await publicUntil(resumed);
  assert.equal(terminal.view.phase, 'RESULT');
  assert.equal(terminal.frame.epoch, initial.frame.epoch);
  await ok(resumed, { t: 'room.start' });
  const next = await publicUntil(resumed);
  assert.notEqual(next.frame.epoch, terminal.frame.epoch);
});

test('a spectator joining after settlement gets an explicit unavailable public recovery', { timeout: 10000 }, async (t) => {
  const { srv, connect } = await fixture(t, { MatchClass: StubMatch });
  const host = await connect('Host');
  await ok(host, { t: 'room.create', mode: 'coop', difficulty: 'NORMAL' });
  const state = await host.waitFor('room.state');
  await ok(host, { t: 'room.start' }); await publicUntil(host);
  await ok(host, { t: 'g.infoReady' }); await host.waitFor('m.result');
  const viewer = await connect('Late viewer');
  await ok(viewer, { t: 'room.spectate', code: state.code });
  await viewer.waitFor('room.state');
  const room = srv.lobby.rooms.get(state.code);
  assert.equal(room.match, null);
  assert.equal(room.replay.publicStream.latest.has(viewer.id), false);
  const reply = await viewer.request({ t: 'm.resync' });
  assert.equal(reply.t, 'error'); assert.equal(reply.code, 'WRONG_PHASE');
  assert.equal(reply.detail, 'public state unavailable');
  const current = await viewer.waitFor('room.state');
  assert.equal(current.inMatch, false);
});
