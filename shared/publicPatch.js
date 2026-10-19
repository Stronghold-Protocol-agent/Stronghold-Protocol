// Versioned public views. Plain JSON only; patches never change the last published tree in place.
export const PUBLIC_RESYNC_UNAVAILABLE = 'public state unavailable';

const plain = (v) => v !== null && typeof v === 'object'
  && (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null);
const put = (obj, key, value) => Object.defineProperty(obj, key, { value, enumerable: true, writable: true, configurable: true });

/** Detach a public projection with exactly the semantics of the JSON transport. */
export function publicSnapshot(view) {
  const copy = JSON.parse(JSON.stringify(view));
  if (!plain(copy) || copy.t !== 'm.public') throw new Error('invalid public view');
  return copy;
}

/** Deterministic sets/deletions; a resized array is replaced as a whole. */
export function diffPublic(before, after) {
  const ops = [];
  const walk = (a, b, path) => {
    if (a === b) return;
    if (Array.isArray(a) && Array.isArray(b) && a.length === b.length) {
      for (let i = 0; i < b.length; i++) walk(a[i], b[i], [...path, i]);
    } else if (plain(a) && plain(b)) {
      for (const key of Object.keys(a).sort()) if (!Object.hasOwn(b, key)) ops.push(['del', [...path, key]]);
      for (const key of Object.keys(b).sort()) {
        if (!Object.hasOwn(a, key)) ops.push(['set', [...path, key], b[key]]);
        else walk(a[key], b[key], [...path, key]);
      }
    } else ops.push(['set', path, b]);
  };
  walk(before, after, []);
  return ops;
}

const validValue = (v, depth = 0) => {
  if (depth > 64) return false;
  if (v === null || typeof v === 'string' || typeof v === 'boolean') return true;
  if (typeof v === 'number') return Number.isFinite(v);
  if (Array.isArray(v)) {
    const keys = Object.keys(v);
    return keys.length === v.length && keys.every((key, i) => key === String(i) && validValue(v[i], depth + 1));
  }
  return plain(v) && Object.values(v).every((x) => validValue(x, depth + 1));
};

/** Apply atomically with copy-on-write. Inherited properties and array holes are never paths. */
export function applyPublicPatch(before, ops) {
  if (!Array.isArray(ops) || ops.length > 10000) throw new Error('invalid operations');
  let result = before;
  for (const op of ops) {
    if (!Array.isArray(op) || !['set', 'del'].includes(op[0]) || op.length !== (op[0] === 'set' ? 3 : 2)
      || !Array.isArray(op[1]) || op[1].length > 64 || (op[0] === 'set' && !validValue(op[2]))) throw new Error('invalid operation');
    const [kind, path, value] = op;
    const change = (node, depth) => {
      if (depth === path.length) {
        if (kind !== 'set') throw new Error('cannot delete root');
        return value;
      }
      const key = path[depth];
      const array = Array.isArray(node);
      if (array ? !Number.isSafeInteger(key) || key < 0 || key >= node.length : !plain(node) || typeof key !== 'string') throw new Error('invalid path');
      const last = depth === path.length - 1;
      if (array && !Object.hasOwn(node, key)) throw new Error('missing array entry');
      if ((!last || kind === 'del') && !Object.hasOwn(node, key)) throw new Error('missing path');
      if (array && kind === 'del' && last) throw new Error('cannot delete array entry');
      const next = array ? node.slice() : { ...node };
      if (last && kind === 'del') delete next[key];
      else put(next, key, change(node[key], depth + 1));
      return next;
    };
    result = change(result, 0);
  }
  if (!plain(result) || result.t !== 'm.public') throw new Error('invalid public result');
  return result;
}

const version = (v) => Number.isSafeInteger(v) && v > 0;
const epoch = (v) => typeof v === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(v);

/** One receiver per socket. A bad frame publishes nothing and requests one recovery until a full arrives. */
export class PublicReceiver {
  constructor() { this.reset(); }
  reset() { this.view = null; this.epoch = null; this.version = 0; this.waiting = false; }
  receive(frame) {
    if (frame.t === 'm.public') {
      if (!epoch(frame.epoch) || !version(frame.version)) return this.gap();
      const { epoch: id, version: seq, ...view } = frame;
      if (!validValue(view)) return this.gap();
      this.view = view; this.epoch = id; this.version = seq; this.waiting = false;
      return { view, resync: false };
    }
    if (this.waiting) return { view: null, resync: false };
    if (frame.t !== 'm.patch' || !this.view || frame.epoch !== this.epoch || frame.base !== this.version
      || !version(frame.version) || frame.version !== frame.base + 1) return this.gap();
    try {
      const view = applyPublicPatch(this.view, frame.ops);
      this.view = view; this.version = frame.version;
      return { view, resync: false };
    } catch { return this.gap(); }
  }
  gap() {
    const resync = !this.waiting;
    this.waiting = true;
    return { view: null, resync };
  }
}
