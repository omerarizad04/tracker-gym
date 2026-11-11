/* Tracker Gym — standalone runtime.
   The app code was written for claude.ai's `window.claude.use('db' | 'assets')`.
   This file provides the same API on top of Supabase so the same app runs as a
   home-screen app: login, an in-memory store mirrored to the `docs` table,
   realtime sync between devices, a local cache for bad gym signal, and photo storage. */
(() => {
  'use strict';
  const cfg = window.TG_CONFIG || {};
  const configured = /^https:\/\//.test(cfg.url || '') && !!cfg.anonKey && !/PEGA/.test(cfg.anonKey);
  const sb = configured && window.supabase ? window.supabase.createClient(cfg.url, cfg.anonKey, { auth: { persistSession: true, autoRefreshToken: true } }) : null;
  const CACHE_KEY = 'tg-cache-v1';
  const store = new Map();               // "col/id" -> data
  const listeners = new Set();           // {kind:'col'|'doc', col, path, cb}
  const meta = { fromCache: true, hasPendingWrites: false };
  let uid = null, resolveReady, channel = null;
  const ready = new Promise(r => (resolveReady = r));

  /* ---------- snapshots ---------- */
  const docSnap = path => { const v = store.get(path); return { id: path.split('/')[1], exists: v !== undefined, data: () => v, metadata: { ...meta } }; };
  const colSnap = col => {
    const docs = [...store.keys()].filter(k => k.split('/')[0] === col).map(docSnap);
    return { docs, size: docs.length, empty: !docs.length, metadata: { ...meta }, docChanges: () => [] };
  };
  const emit = col => listeners.forEach(l => {
    if (l.kind === 'col' && (!col || l.col === col)) l.cb(colSnap(l.col));
    if (l.kind === 'doc' && (!col || l.col === col)) l.cb(docSnap(l.path));
  });
  const saveCache = () => { try { localStorage.setItem(CACHE_KEY, JSON.stringify({ uid, rows: [...store.entries()] })); } catch (e) {} };
  const loadCache = () => { try { const c = JSON.parse(localStorage.getItem(CACHE_KEY) || 'null'); if (c && c.uid === uid) c.rows.forEach(([k, v]) => store.set(k, v)); } catch (e) {} };

  /* ---------- server ---------- */
  async function fetchAll() {
    const rows = [];
    for (let from = 0; ; from += 1000) {
      const { data, error } = await sb.from('docs').select('col,id,data').range(from, from + 999);
      if (error) throw error;
      rows.push(...data);
      if (data.length < 1000) break;
    }
    store.clear();
    rows.forEach(r => store.set(`${r.col}/${r.id}`, r.data));
    meta.fromCache = false;
    saveCache();
    emit();
  }
  const refresh = () => fetchAll().catch(() => { meta.fromCache = true; });
  function subscribe() {
    if (channel) sb.removeChannel(channel);
    channel = sb.channel('docs-' + uid)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'docs', filter: `user_id=eq.${uid}` }, p => {
        const row = p.eventType === 'DELETE' ? p.old : p.new;
        if (!row || !row.col) { refresh(); return; }
        const k = `${row.col}/${row.id}`;
        if (p.eventType === 'DELETE') store.delete(k); else store.set(k, row.data);
        saveCache(); emit(row.col);
      })
      .subscribe();
  }
  const fail = error => { const e = new Error(error?.message || 'error'); e.code = /quota|limit/i.test(error?.message || '') ? 'quota_exceeded' : 'unavailable'; return e; };
  const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
  const merge = (a, b) => { const o = { ...(a || {}) }; Object.entries(b).forEach(([k, v]) => { o[k] = isObj(v) && isObj(o[k]) ? merge(o[k], v) : v; }); return o; };

  function docRef(path) {
    const [col, id] = path.split('/');
    return {
      id, path,
      get: async () => docSnap(path),
      set: async data => {
        const prev = store.get(path);
        store.set(path, JSON.parse(JSON.stringify(data))); emit(col);
        const { error } = await sb.from('docs').upsert({ user_id: uid, col, id, data, updated_at: new Date().toISOString() });
        if (error) { if (prev === undefined) store.delete(path); else store.set(path, prev); emit(col); throw fail(error); }
        saveCache();
      },
      update: async data => docRef(path).set(merge(store.get(path), data)),
      delete: async () => {
        const prev = store.get(path);
        store.delete(path); emit(col);
        const { error } = await sb.from('docs').delete().match({ user_id: uid, col, id });
        if (error) { if (prev !== undefined) store.set(path, prev); emit(col); throw fail(error); }
        saveCache();
      },
      onSnapshot: (cb) => { const l = { kind: 'doc', col, path, cb }; listeners.add(l); queueMicrotask(() => cb(docSnap(path))); return () => listeners.delete(l); }
    };
  }
  const db = {
    doc: docRef,
    collection: col => ({
      path: col,
      doc: id => docRef(`${col}/${id || Math.random().toString(36).slice(2, 12)}`),
      add: async data => { const r = docRef(`${col}/${Math.random().toString(36).slice(2, 12)}`); await r.set(data); return r; },
      get: async () => colSnap(col),
      onSnapshot: (cb) => { const l = { kind: 'col', col, cb }; listeners.add(l); queueMicrotask(() => cb(colSnap(col))); return () => listeners.delete(l); }
    })
  };

  /* ---------- photos (private bucket, signed URLs cached for the visit) ---------- */
  const urls = new Map(), pending = new Set();
  const BLANK = 'data:image/gif;base64,R0lGODlhAQABAAAAACw=';
  window.__img = p => {
    if (!p) return BLANK;
    if (p.url && !p.path && /^https?:|^data:/.test(p.url) && !p.id?.includes('/')) return p.url;
    const path = p.path || p.id;
    if (urls.has(path)) return urls.get(path);
    if (!pending.has(path)) {
      pending.add(path);
      sb.storage.from('photos').createSignedUrl(path, 60 * 60 * 24).then(({ data }) => {
        pending.delete(path);
        if (data?.signedUrl) { urls.set(path, data.signedUrl); window.dispatchEvent(new Event('tg-rerender')); }
      });
    }
    return BLANK;
  };
  const assets = {
    upload: async file => {
      const ext = ((file.name || '').split('.').pop() || 'jpg').toLowerCase().replace(/[^a-z0-9]/g, '') || 'jpg';
      const path = `${uid}/${Date.now()}-${Math.random().toString(36).slice(2, 7)}.${ext}`;
      const { error } = await sb.storage.from('photos').upload(path, file, { contentType: file.type || 'image/jpeg' });
      if (error) throw fail(error);
      return { id: path, path, url: null, sizeBytes: file.size, contentType: file.type };
    },
    list: async () => ({ assets: [], usage: {} }),
    delete: async id => { await sb.storage.from('photos').remove([id]); }
  };

  window.claude = { use: async name => { await ready; return name === 'db' ? db : name === 'assets' ? assets : null; } };

  /* ---------- login screen ---------- */
  function showLogin(msg) {
    let el = document.getElementById('auth');
    if (!el) {
      el = document.createElement('div');
      el.id = 'auth';
      el.innerHTML = `<form class="authbox" novalidate>
        <div class="authlogo" aria-hidden="true"></div>
        <h1>Tracker Gym</h1>
        <p class="muted" style="margin:0">Entra con tu cuenta para ver tus rutinas y tu progreso.</p>
        <label class="f"><span class="label">Correo</span><input id="au-email" type="email" autocomplete="username" inputmode="email" required></label>
        <label class="f"><span class="label">Contraseña</span><input id="au-pass" type="password" autocomplete="current-password" minlength="6" required></label>
        <p id="au-msg" class="small" style="margin:0;color:var(--bad)" role="alert"></p>
        <button class="primary" type="submit" id="au-in">Entrar</button>
        <button type="button" class="ghost" id="au-up">Crear mi cuenta (solo la primera vez)</button>
      </form>`;
      document.body.appendChild(el);
      const form = el.querySelector('form'), m = el.querySelector('#au-msg');
      const creds = () => ({ email: el.querySelector('#au-email').value.trim(), password: el.querySelector('#au-pass').value });
      const busy = b => el.querySelectorAll('button').forEach(x => (x.disabled = b));
      form.addEventListener('submit', async e => {
        e.preventDefault(); busy(true); m.textContent = '';
        const { data, error } = await sb.auth.signInWithPassword(creds());
        busy(false);
        if (error) { m.textContent = /invalid/i.test(error.message) ? 'Correo o contraseña incorrectos.' : /confirm/i.test(error.message) ? 'Confirma tu correo con el enlace que te llegó y vuelve a entrar.' : 'No se pudo entrar. Revisa tu conexión.'; return; }
        start(data.session);
      });
      el.querySelector('#au-up').addEventListener('click', async () => {
        const c = creds();
        if (!c.email || c.password.length < 6) { m.textContent = 'Escribe tu correo y una contraseña de al menos 6 caracteres.'; return; }
        busy(true); m.textContent = '';
        const { data, error } = await sb.auth.signUp(c);
        busy(false);
        if (error) { m.textContent = /registered|exists/i.test(error.message) ? 'Ya existe una cuenta con ese correo. Toca Entrar.' : /not allowed|disabled/i.test(error.message) ? 'El registro está cerrado. Usa tu cuenta existente.' : 'No se pudo crear la cuenta: ' + error.message; return; }
        if (data.session) start(data.session); else { m.style.color = 'var(--good)'; m.textContent = 'Cuenta creada. Confirma el correo que te llegó y luego toca Entrar.'; }
      });
    }
    el.hidden = false;
    if (msg) el.querySelector('#au-msg').textContent = msg;
  }

  let started = false;
  async function start(session) {
    if (started) return; started = true;
    uid = session.user.id;
    const el = document.getElementById('auth'); if (el) el.hidden = true;
    loadCache();
    try { await fetchAll(); } catch (e) { meta.fromCache = true; }
    subscribe();
    resolveReady();
    document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') { refresh(); subscribe(); } });
    window.addEventListener('online', () => { refresh(); subscribe(); });
  }

  if (!sb) {
    showLogin(window.supabase ? 'Falta configurar la base de datos (config.js).' : 'Sin conexión: abre la app con internet la primera vez.');
    return;
  }
  sb.auth.getSession().then(({ data }) => data.session ? start(data.session) : showLogin());
  sb.auth.onAuthStateChange((ev, session) => { if (ev === 'SIGNED_OUT') { try { localStorage.removeItem(CACHE_KEY); } catch (e) {} location.reload(); } });
  window.TG_SIGNOUT = () => sb.auth.signOut();
})();
