// Match test adapters for the shared scripted client-combat participant.
import { SimClient } from '../../tools/lib/sim-client.mjs';
import { createBattleFromSpec } from '../../server/sim/spec.js';
import { RehearsalRun } from '../../server/sim/rehearsal.js';
export { SimClient, attachSimClients } from '../../tools/lib/sim-client.mjs';

/**
 * A SimClient behind a websocket test client (test/helpers/wsClient.js TestClient): frames are read from the socket,
 * reports go out without a rid (fire-and-forget, like the browser runner). Real timers.
 * @returns {SimClient}
 */
export function attachWsSimClient(tc, opts = {}) {
  const sc = new SimClient(tc.id, {
    send: (msg) => { if (tc.isOpen) tc.send({ ...msg, rid: null }); },
    setTimeout: (fn, ms) => setTimeout(fn, ms),
    clearTimeout: (t) => clearTimeout(t),
    ...opts,
  });
  tc.ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let msg;
    try { msg = JSON.parse(String(data)); } catch { return; }
    try { sc.onMessage(msg); } catch (e) { sc.error = e; }
  });
  return sc;
}

// ---------------------------------------------------------------------------------------------------
// Delegated bot layout rehearsals (DESIGN §14)

/**
 * The pick of one `b.rehearsal` task, computed the way the page's Worker does it (public/js/battle/rehearsalWorker.js):
 * build every candidate with the shared sim (a factory per candidate, so the run builds them as it reaches them — what
 * the server's own job does too), run them with the shared comparison (server/sim/rehearsal.js) and return the winning
 * index.
 * @param {{ playerId: string, cap: number, specs: object[] }} task
 * @param {any} ds DataSource of the match (the same data the server simulates with)
 * @param {{ BattleClass?: Function }} [opts]
 */
export function simulateRehearsal(task, ds, { BattleClass } = {}) {
  const run = new RehearsalRun(task.specs.map((spec) => () => createBattleFromSpec(spec, ds, { BattleClass, quiet: true, recordEvents: false })), task.playerId, task.cap);
  run.run();
  return run.score.index;
}

/**
 * A scripted host browser for delegated bot rehearsals: every `b.rehearsal` frame sent to a watched player (default:
 * every human seat) is answered through `m.handle` — after `delayMs` (0: a virtual timer, like a socket — the server
 * call that sent the frame has returned by then), or right inside that call with `sync` (the other extreme: the answer
 * beats the driver's callback) — with the pick the page's Worker would make (simulateRehearsal). Every
 * `b.rehearsalCancel` of a watched player is collected in `host.cancels` (the frame the page drops a task on). Test
 * knobs: `mute` (never answer), `error` (this browser cannot run the task), `tamper(frame) → any` (a wrong or malformed
 * answer), `manual` (collect the frames and answer with `host.answer(task, fields)`, e.g. after the server already took
 * the task over).
 * @param {ReturnType<import('./harness.js').makeMatch>} h
 * @param {{ players?: string[]|null, mute?: boolean, error?: boolean, delayMs?: number, tamper?: Function|null,
 *   manual?: boolean, sync?: boolean, ds?: any, BattleClass?: Function }} [o]
 */
export function attachRehearsalHost(h, { players = null, mute = false, error = false, delayMs = 0, tamper = null, manual = false, sync = false, ds = null, BattleClass = undefined } = {}) {
  const m = h.m;
  const host = { tasks: [], cancels: [], answered: 0 };
  const watched = (pid) => (players ? players.includes(pid) : !m.players.get(pid)?.isBot);
  h.onSend.push((pid, msg) => {
    if (!watched(pid)) return;
    if (msg.t === 'b.rehearsalCancel') { host.cancels.push(msg.taskId); return; }
    if (msg.t !== 'b.rehearsal') return;
    const task = { playerId: pid, frame: msg };
    host.tasks.push(task);
    if (manual || mute) return;
    const reply = () => {
      const choice = tamper ? tamper(msg) : simulateRehearsal(msg, ds || m.ds, { BattleClass });
      host.answered++;
      m.handle(pid, { t: 'b.rehearsalResult', taskId: msg.taskId, ...(error ? { error: true } : { choice }) });
    };
    if (sync) reply(); else h.sched.setTimeout(reply, delayMs);
  });
  /** Answer one collected task by hand (manual mode, or a late answer after a takeover). */
  host.answer = (task, fields = {}) => { host.answered++; return m.handle(task.playerId, { t: 'b.rehearsalResult', taskId: task.frame.taskId, ...fields }); };
  return host;
}
