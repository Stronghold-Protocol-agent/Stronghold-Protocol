// Finite payload and compressed WebSocket byte counts for real simulated matches; no browser or assets needed.
// Run: node tools/measure-public-deltas.mjs
import { replayPublicDeltas } from './lib/public-deltas-replay.mjs';
import { WebSocket, WebSocketServer } from 'ws';
import { once } from 'node:events';
import { WS_DEFLATE } from '../server/http/websocket.js';

async function wireBytes(frames) {
  const wss = new WebSocketServer({ port: 0, host: '127.0.0.1', perMessageDeflate: { ...WS_DEFLATE } });
  await once(wss, 'listening');
  const sockets = new Map(), clients = [];
  try {
    for (const id of new Set(frames.map(([id]) => id))) {
      const accepted = once(wss, 'connection');
      const client = new WebSocket(`ws://127.0.0.1:${wss.address().port}`, { perMessageDeflate: true });
      clients.push(client);
      await once(client, 'open');
      const [ws, request] = await accepted;
      const socket = request.socket;
      sockets.set(id, { ws, socket, initial: socket.bytesWritten });
    }
    // Finite batches let the real compressors and sockets drain instead of measuring an unbounded send queue.
    for (let i = 0; i < frames.length; i += 32) {
      await Promise.all(frames.slice(i, i + 32).map(([id, raw]) => new Promise((resolve, reject) => {
        sockets.get(id).ws.send(raw, (error) => error ? reject(error) : resolve());
      })));
    }
    return [...sockets.values()].reduce((sum, { socket, initial }) => sum + socket.bytesWritten - initial, 0);
  } finally {
    for (const client of clients) client.terminate();
    for (const { ws } of sockets.values()) ws.terminate();
    await new Promise((resolve) => wss.close(resolve));
  }
}
const matches = [];
for (const options of [{ seed: 3, difficulty: 'NORMAL' }, { seed: 31, difficulty: 'HARD' }, { seed: 31, difficulty: 'HARD', coverage: true }]) {
  const frames = { full: [], delta: [] };
  const result = replayPublicDeltas({ ...options, captureWire: (kind, id, raw) => frames[kind].push([id, raw]) });
  result.fullWireBytes = await wireBytes(frames.full);
  result.deltaWireBytes = await wireBytes(frames.delta);
  result.wireReductionPercent = Number((100 * (1 - result.deltaWireBytes / result.fullWireBytes)).toFixed(2));
  matches.push(result);
}
console.log(JSON.stringify({ metric: 'outbound bytes per real simulated match',
  transport: 'four human recipients, late spectator, one reconnect; payload UTF-8 and actual compressed WebSocket bytes',
  wire: 'captured frames replayed through ws with the server compression policy; upgrade handshakes and close frames excluded',
  coverage: 'The coverage fixture raises LP and bond layers to exercise late rounds; the other runs use legal autoplay decisions.', matches }, null, 2));
