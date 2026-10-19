import { test } from 'node:test';
import assert from 'node:assert/strict';
import { publicSnapshot, diffPublic, applyPublicPatch, PublicReceiver, PUBLIC_RESYNC_UNAVAILABLE } from '../shared/publicPatch.js';
import { PublicStream } from '../server/publicStream.js';
import { Lobby } from '../server/lobby.js';
import { Net } from '../public/js/net.js';
import { C2S, validateC2S } from '../shared/protocol.js';
import { isIntentType, isMatchIntent, checkStorageWrite } from '../shared/plugins/ui.js';

const view = (n = 0) => ({ t: 'm.public', phase: 'PREP', round: 1, setupRevision: 0, serverNow: n,
  players: Array.from({ length: 4 }, (_, i) => ({ playerId: `p${i}`, ready: false, name: 'Player'.repeat(20), lp: 100 })), fields: [] });
const socket = () => ({ readyState: 1, bufferedAmount: 0, sent: [], send(data) { this.sent.push(JSON.parse(data)); }, terminate() { this.readyState = 3; } });

test('2000 seeded JSON transitions round-trip, preserve the base and are deterministic', () => {
  let seed = 1234567;
  const rand = () => ((seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0) / 2 ** 32);
  const tree = (depth = 0) => {
    const kind = Math.floor(rand() * (depth < 4 ? 6 : 4));
    if (kind === 0) return null;
    if (kind === 1) return Math.floor(rand() * 10);
    if (kind === 2) return rand() > 0.5;
    if (kind === 3) return `value${Math.floor(rand() * 20)}`;
    if (kind === 4) return Array.from({ length: Math.floor(rand() * 5) }, () => tree(depth + 1));
    return Object.fromEntries(Array.from({ length: Math.floor(rand() * 5) }, (_, i) => [`key${i}`, tree(depth + 1)]));
  };
  let before = { t: 'm.public', data: tree() };
  for (let i = 0; i < 2000; i++) {
    const after = { t: 'm.public', data: tree(), ...(i % 3 ? { optional: i } : {}) };
    const saved = JSON.stringify(before), ops = diffPublic(before, after);
    assert.deepEqual(applyPublicPatch(before, ops), after);
    assert.equal(JSON.stringify(before), saved);
    assert.deepEqual(ops, diffPublic(before, after));
    before = after;
  }
});

test('JSON normalization, detached sources, array resizing and hostile own keys', () => {
  const source = { t: 'm.public', omitted: undefined, list: [undefined, NaN, Infinity], skins: { a: 1 } };
  const detached = publicSnapshot(source);
  source.skins.a = 2;
  assert.deepEqual(detached, { t: 'm.public', list: [null, null, null], skins: { a: 1 } });
  const a = JSON.parse('{"t":"m.public","__proto__":{"x":1},"constructor":1,"list":[1,2]}');
  const b = JSON.parse('{"t":"m.public","__proto__":{"x":2},"constructor":2,"list":[1]}');
  const ops = diffPublic(a, b);
  assert.ok(ops.some((op) => op[0] === 'set' && op[1].join('.') === 'list'));
  assert.deepEqual(applyPublicPatch(a, ops), b);
  assert.equal({}.x, undefined);
});

test('invalid late operations fail atomically, including inherited, array, root and oversized paths', () => {
  const before = view();
  const cases = [
    ['set', ['missing', 'x'], 2], ['del', ['players', 0]], ['set', ['players', -1], 1],
    ['set', ['players', 'length'], 0], ['set', ['constructor', 'prototype', 'polluted'], true],
    ['del', []], ['set', ['t'], 'm.private'], ['set', ['x'], undefined],
    ['set', Array(65).fill('x'), 1], ['set', ['x'], NaN], ['del', ['absent']],
    ['set', ['x'], new Date(0)], ['set', ['x'], Array(2)],
  ];
  for (const bad of cases) {
    const saved = structuredClone(before);
    assert.throws(() => applyPublicPatch(before, [['set', ['players', 0, 'lp'], 1], bad]));
    assert.deepEqual(before, saved);
  }
});

test('receiver rejects gaps, duplicates, wrong epochs and invalid ops until one full recovery', () => {
  const receiver = new PublicReceiver();
  const full = { ...view(), epoch: 'match1', version: 1 };
  assert.deepEqual(receiver.receive(full).view, view());
  const patch = { t: 'm.patch', epoch: 'match1', base: 1, version: 2, ops: [['set', ['serverNow'], 1]] };
  assert.equal(receiver.receive(patch).view.serverNow, 1);
  assert.equal(receiver.receive(patch).resync, true);
  assert.deepEqual(receiver.receive(patch), { view: null, resync: false });
  assert.equal(receiver.view.serverNow, 1);
  receiver.receive({ ...full, version: 3 });
  assert.equal(receiver.receive({ ...patch, base: 3, version: 4, epoch: 'wrong' }).resync, true);
  receiver.receive({ ...full, version: 5 });
  assert.equal(receiver.receive({ ...patch, base: 5, version: 7 }).resync, true);
  receiver.receive({ ...full, version: 8 });
  assert.equal(receiver.receive({ ...patch, base: 8, version: 9, ops: [['set', ['x', 'bad'], 1]] }).resync, true);
  receiver.reset();
  assert.equal(receiver.receive(patch).resync, true);
});

test('stream shares frames, detaches projections, isolates recipients and advances only on queued frames', () => {
  const stream = new PublicStream(), a = socket(), b = socket(), c = socket();
  const original = view(), first = stream.prepare(original);
  stream.send('a', a, first); stream.send('b', b, first);
  assert.equal(stream.bases.get(a), stream.bases.get(b));
  original.players[0].lp = -1;
  const next = stream.prepare(view(1));
  stream.send('a', a, next); stream.send('b', b, next);
  assert.equal(a.sent.at(-1).t, 'm.patch');
  assert.deepEqual(a.sent, b.sent);
  assert.equal(first.view.players[0].lp, 100);
  const redacted = stream.prepare({ ...view(2), players: [], redacted: true });
  stream.send('c', c, redacted);
  stream.resync('c', c);
  assert.deepEqual(c.sent.at(-1).players, []);
  assert.equal(c.sent.at(-1).redacted, true);
  a.send = () => { throw new Error('queue failed'); };
  assert.equal(stream.send('a', a, stream.prepare(view(3))), false);
  assert.equal(stream.bases.get(a).version, 2);
  a.send = (data) => a.sent.push(JSON.parse(data));
  stream.send('a', a, stream.prepare(view(4)));
  assert.equal(a.sent.at(-1).base, 2);
  stream.resync('a', a);
  assert.equal(a.sent.at(-1).t, 'm.public');
  for (let i = 0; i < 100; i++) stream.resync('a', a);
  const retained = stream.latest.get('a');
  assert.ok(retained.fullFrames.size <= 16 && retained.bases.size <= 16, 'untimed recovery caches stay bounded');
  assert.equal(a.sent.at(-1).version, 104);
});

test('phase, round, setup, new sockets and next-match epochs always begin with a full view', () => {
  const stream = new PublicStream(), a = socket();
  const changes = [{}, { phase: 'COMBAT' }, { phase: 'COMBAT', round: 2 }, { phase: 'COMBAT', round: 2, setupRevision: 1 }];
  for (const extra of changes) { stream.send('a', a, stream.prepare({ ...view(), ...extra })); assert.equal(a.sent.at(-1).t, 'm.public'); }
  const b = socket(); stream.send('a', b, stream.prepare(view()));
  assert.equal(b.sent[0].version, 1);
  const next = new PublicStream(); next.send('a', b, next.prepare(view()));
  assert.notEqual(b.sent[0].epoch, b.sent[1].epoch);
});

test('unserializable public projections are logged and dropped without losing the queued baseline', () => {
  const ws = socket(), errors = [], stream = new PublicStream(), lobby = Object.create(Lobby.prototype);
  const seat = { playerId: 'player' }, session = { playerId: 'player', roomCode: 'TEST', connected: true, ws };
  const room = { code: 'TEST', seats: [seat], spectators: [], seatOf: () => seat, spectatorOf: () => null };
  const ctx = { publicStream: stream };
  lobby.registry = { byId: () => session }; lobby.log = { error: (line) => errors.push(line) };
  lobby.matchBroadcast(room, ctx, view());
  const original = stream.latest.get('player');
  const circular = view(); circular.self = circular;
  const throwing = view(); Object.defineProperty(throwing, 'bad', { enumerable: true, get() { throw new Error('getter'); } });
  for (const bad of [circular, { ...view(), number: 1n }, throwing]) {
    assert.equal(stream.prepare(bad), null);
    assert.doesNotThrow(() => lobby.matchBroadcast(room, ctx, bad));
    assert.equal(lobby.matchSend(room, ctx, 'player', bad), false);
  }
  assert.equal(errors.length, 6); assert.ok(errors.every((line) => line.includes('unserializable')));
  assert.equal(ws.sent.length, 1); assert.equal(stream.latest.get('player'), original);
  assert.equal(stream.bases.get(ws).version, 1);
  lobby.matchBroadcast(room, ctx, view(1));
  const receiver = new PublicReceiver();
  for (const frame of ws.sent) assert.equal(receiver.receive(frame).resync, false);
  assert.deepEqual(receiver.view, view(1));
});

test('retained replay preserves each recipient projection and cannot fall back to a broader view', () => {
  const stream = new PublicStream(), lobby = Object.create(Lobby.prototype);
  stream.send('player', null, stream.prepare(view()));
  stream.send('viewer', null, stream.prepare({ t: 'm.public', phase: 'RESULT', players: [], redacted: true }));
  const room = { seats: [{ playerId: 'player' }], spectators: [{ playerId: 'viewer' }] };
  room.replay = lobby.buildReplay(room, { publicStream: stream, results: new Map(), sharedResult: '{"t":"m.result"}', lastPublic: JSON.stringify(view()) });
  assert.equal(Object.hasOwn(room.replay, 'publicFrame'), false);
  assert.equal(JSON.parse(lobby.replayFor(room, 'viewer')[0]).redacted, true);
  assert.equal(JSON.parse(lobby.replayFor(room, 'viewer')[0]).players.length, 0);
  assert.equal(JSON.parse(lobby.replayFor(room, 'player')[0]).players.length, 4);
  room.replay.publicFrames.delete('viewer');
  assert.equal(lobby.replayFor(room, 'viewer').length, 1);
  lobby.dropReplay(room, 'viewer');
  assert.equal(stream.latest.has('viewer'), false);
  assert.equal(stream.latest.has('player'), true, 'the other recipient still owns its replay');
  lobby.dropReplay(room, 'player');
  assert.equal(stream.latest.size, 0);
  assert.equal(room.replay, null);
});

test('Net dispatch reconstructs before typed and wildcard listeners, with one public-only recovery', () => {
  const net = new Net(), sent = [], typed = [], all = [];
  net.ws = { readyState: 1, send: (raw) => sent.push(JSON.parse(raw)) };
  net._publicVersion = 2;
  net.on('m.public', (p) => typed.push(p)); net.on('*', (p) => all.push(p));
  const frame = (p) => net._onMessage(JSON.stringify(p));
  frame({ ...view(), epoch: 'test', version: 1 });
  frame({ t: 'm.patch', epoch: 'test', base: 1, version: 2, ops: [['set', ['serverNow'], 5]] });
  assert.equal(typed.at(-1).serverNow, 5);
  assert.deepEqual(typed, all);
  frame({ t: 'm.patch', epoch: 'test', base: 3, version: 4, ops: [] });
  frame({ t: 'm.patch', epoch: 'test', base: 4, version: 5, ops: [] });
  assert.deepEqual(sent, [{ t: 'm.resync' }]);
  assert.equal(typed.length, 2);
  frame({ t: 'room.state', code: 'NEXT' });
  assert.equal(net._public.view, null);
  net._teardownSocket();
});

test('m.resync is validated and blocked throughout the read-only plugin bridge', () => {
  assert.equal(validateC2S({ t: 'm.resync' }), null);
  assert.ok(Object.keys(C2S).every(isIntentType));
  assert.equal(isMatchIntent({ nested: { t: 'm.resync' } }), true);
  assert.equal(isIntentType('m.patch'), false);
  assert.ok(checkStorageWrite(Object.create(null), 'test', { t: 'm.resync' }).error);
});

test('Net retries rejected public recovery after the heavy bucket refills and cancels it on full state', () => {
  const timers = new Map(); let seq = 0;
  const net = new Net({ timers: { setTimeout(fn) { const id = ++seq; timers.set(id, fn); return id; }, clearTimeout(id) { timers.delete(id); },
    setInterval() {}, clearInterval() {} } });
  const sent = [];
  net.ws = { readyState: 1, send: (raw) => sent.push(JSON.parse(raw)) }; net._publicVersion = 2;
  const frame = (p) => net._onMessage(JSON.stringify(p));
  frame({ t: 'm.patch', epoch: 'missing', base: 1, version: 2, ops: [] });
  frame({ t: 'error', code: 'RATE', detail: 'm.resync too often' });
  assert.equal(timers.size, 1);
  const retry = [...timers.values()][0]; timers.clear(); retry();
  assert.deepEqual(sent, [{ t: 'm.resync' }, { t: 'm.resync' }]);
  frame({ t: 'error', code: 'RATE' });
  assert.equal(timers.size, 1, 'the general connection bucket also schedules recovery');
  const genericRetry = [...timers.values()][0]; timers.clear(); genericRetry();
  assert.equal(sent.length, 3);
  frame({ t: 'error', code: 'RATE', detail: 'm.resync too often' });
  frame({ ...view(), epoch: 'recovered', version: 3 });
  assert.equal(timers.size, 0);
  net._teardownSocket();
});

test('unavailable public recovery clears the waiting receiver and its retry without a player error', () => {
  const timers = new Map(); let seq = 0;
  const net = new Net({ timers: { setTimeout(fn) { const id = ++seq; timers.set(id, fn); return id; },
    clearTimeout(id) { timers.delete(id); }, setInterval() {}, clearInterval() {} } });
  const errors = [];
  net.ws = { readyState: 1, send() {} }; net._publicVersion = 2;
  net.on('error', (error) => errors.push(error));
  net._onMessage(JSON.stringify({ t: 'm.patch', epoch: 'gone', base: 1, version: 2, ops: [] }));
  net._onMessage(JSON.stringify({ t: 'error', code: 'RATE', detail: 'm.resync too often' }));
  assert.equal(net._public.waiting, true); assert.equal(timers.size, 1);
  net._onMessage(JSON.stringify({ t: 'error', code: 'WRONG_PHASE', detail: PUBLIC_RESYNC_UNAVAILABLE }));
  assert.equal(net._public.waiting, false); assert.equal(net._public.view, null);
  assert.equal(timers.size, 0); assert.deepEqual(errors, []);
  net._teardownSocket();
});
