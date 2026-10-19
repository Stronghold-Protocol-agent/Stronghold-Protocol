import { test } from 'node:test';
import assert from 'node:assert/strict';
import { replayPublicDeltas } from '../../tools/lib/public-deltas-replay.mjs';
import { makeMatch } from './harness.js';
import { PublicStream } from '../../server/publicStream.js';
import { PublicReceiver, publicSnapshot } from '../../shared/publicPatch.js';

test('real four-human match: each delta reconstructs the canonical public view, including late watch and reconnect', () => {
  const report = replayPublicDeltas();
  assert.ok(report.phases.includes('COMBAT'));
  assert.ok(report.deltaPublicBytes < report.fullPublicBytes);
});

test('protocol 2 reconstructs external AI success and legacy fallback without private configuration', (t) => {
  const token = 'private-bearer-delta-regression';
  const endpoint = 'https://ai.invalid/hidden-decision-config';
  for (const outcome of ['success', 'fallback']) {
    const h = makeMatch({ mode: 'coop', humans: 1, bots: 1, fake: true, seed: 123,
      botStrategy: 'external', externalReplay: true,
      externalAI: { transport: 'http', url: endpoint, token, seats: [1] } });
    t.after(() => h.m.dispose());
    const { m } = h, ai = m.order[1];
    const stream = new PublicStream(), receiver = new PublicReceiver();
    let expected, frames = 0, patches = 0;
    const ws = { readyState: 1, bufferedAmount: 0, send(raw) {
      const frame = JSON.parse(raw), received = receiver.receive(frame);
      assert.equal(received.resync, false, outcome);
      assert.deepEqual(received.view, expected, outcome);
      assert.ok(!raw.includes(token) && !raw.includes(endpoint), 'operator credentials and endpoint stay private');
      assert.equal(Object.hasOwn(received.view, 'externalAI'), false);
      frames++;
      if (frame.t === 'm.patch') patches++;
    } };
    const send = (msg, force) => {
      if (msg.t !== 'm.public') return;
      expected = publicSnapshot(msg);
      assert.equal(stream.send('p_0', ws, stream.prepare(msg), force), true);
    };
    h.onSend.push((id, msg) => { if (id === 'p_0') send(msg, true); });
    h.onBroadcast.push((msg) => send(msg, false));
    const pending = () => [...m.externalAI.jobs.values()][0];
    const reply = (intents, done) => m.externalReply(pending().id,
      { v: 'sp-ai/1', id: pending().id, intents, done }, false);
    try {
      h.start();
      assert.ok(h.drive(() => pending()?.kind === 'band', { ready: false }));
      const bandId = m.gd.bandIds().find((id) => id !== 'band_bldsk' && !Object.values(m.draft.picks).includes(id));
      assert.ok(bandId);
      reply([{ t: 'g.band', bandId }], true);
      assert.ok(h.drive(() => m.phase === 'PREP' && pending()?.kind === 'prep', { ready: false }));
      m.flush(true);
      assert.equal(m.externalAI.config.token, token);
      assert.equal(m.externalAI.config.url, endpoint);
      if (outcome === 'success') {
        const funds = ai.funds;
        const slot = ai.privateView().shop.slots.findIndex((s) => s && !s.sold && s.price <= funds);
        assert.ok(slot >= 0);
        reply([{ t: 'g.buy', slot }], false);
        assert.ok(ai.shop.slots[slot].sold || ai.funds < funds);
        reply([{ t: 'g.ready', ready: true }], true);
        assert.equal(m.externalAI.states.get(ai.playerId).failures, 0);
      } else {
        m.externalReply(pending().id, null, true);
        assert.ok(h.run(() => ai.ready));
        assert.ok(ai.deployCount > 0, 'legacy finished the incomplete external prep');
        assert.equal(m.externalAI.states.get(ai.playerId).failures, 1);
      }
      assert.ok(ai.ready);
      assert.equal(m.phase, 'PREP', 'the human remains unready while the AI finishes');
      m.flush(true);
      assert.deepEqual(receiver.view, publicSnapshot(m.publicView()));
      assert.ok(frames > 0 && patches > 0, `${outcome} produced real versioned public patches`);
      assert.equal(m.errorCount, 0);
      assert.deepEqual(h.sched.errors, []);
    } finally { m.dispose(); }
  }
});
