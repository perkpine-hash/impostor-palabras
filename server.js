const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const CATS = [
  ['Animales', ['Perro','Gato','Elefante','Jirafa','Delfín','Águila','Tortuga','Pingüino','Mono','León']],
  ['Comida', ['Pizza','Paella','Tortilla','Helado','Chocolate','Sushi','Hamburguesa','Ensalada','Churros','Sandía']],
  ['Lugares', ['Playa','Biblioteca','Aeropuerto','Hospital','Cine','Museo','Gimnasio','Colegio','Montaña','Circo']],
  ['Profesiones', ['Médico','Bombero','Piloto','Cocinero','Profesor','Policía','Carpintero','Cantante','Astronauta','Fotógrafo']],
  ['Deportes', ['Fútbol','Tenis','Natación','Baloncesto','Ciclismo','Boxeo','Esquí','Golf','Voleibol','Surf']],
  ['Objetos', ['Paraguas','Reloj','Linterna','Espejo','Mochila','Tijeras','Teléfono','Llave','Gafas','Guitarra']],
  ['Transporte', ['Tren','Bicicleta','Avión','Barco','Autobús','Moto','Metro','Globo','Patinete','Helicóptero']],
  ['Naturaleza', ['Volcán','Río','Desierto','Bosque','Cascada','Isla','Glaciar','Cueva','Arcoíris','Tormenta']]
];
const html = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});
const wss = new WebSocketServer({ server, maxPayload: 2048 });
const rooms = new Map();
const rnd = n => crypto.randomInt(n);
const conn = p => !!p && !!p.ws && p.ws.readyState === 1;
const norm = s => String(s).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
const newRoom = code => ({ code, players: new Map(), host: null, phase: 'lobby', order: [], imp: null, cat: '', word: '', hint: true, clues: [], votes: {}, res: '', seen: Date.now() });
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

function view(r, p) {
  const inGame = r.phase !== 'lobby';
  const ids = inGame ? r.order : [...r.players.keys()];
  const o = {
    t: 'state', phase: r.phase, me: p.id, host: r.host, room: r.code, hint: r.hint, res: r.res,
    players: ids.map(id => { const q = r.players.get(id); return { id, name: q.name, on: conn(q), voted: !!r.votes[id] }; }),
    clues: r.clues.map(c => ({ n: r.players.get(c.id).name, w: c.w }))
  };
  if (inGame) {
    const isI = p.id === r.imp;
    o.isImp = isI;
    o.cat = (isI && !r.hint) ? null : r.cat;
    o.word = isI ? null : r.word;
    o.turn = r.order[r.clues.length % r.order.length];
    o.round = Math.min(2, Math.floor(r.clues.length / r.order.length) + 1);
    if (r.phase === 'end') { o.word = r.word; o.cat = r.cat; o.imp = r.imp; }
  }
  return o;
}
function broadcast(r) { r.seen = Date.now(); for (const p of r.players.values()) if (conn(p)) send(p.ws, view(r, p)); }
function fixHost(r) { if (!conn(r.players.get(r.host))) { const n = [...r.players.values()].find(conn); r.host = n ? n.id : r.host; } }

function tally(r) {
  const c = {};
  Object.values(r.votes).forEach(v => c[v] = (c[v] || 0) + 1);
  let top = null, m = 0, tie = false;
  for (const k in c) { if (c[k] > m) { m = c[k]; top = k; tie = false; } else if (c[k] === m) tie = true; }
  const nm = id => r.players.get(id).name;
  if (!top || tie) { r.phase = 'end'; r.res = 'Empate en la votación: el impostor se salva. ¡Gana el impostor!'; }
  else if (top === r.imp) { r.phase = 'guess'; }
  else { r.phase = 'end'; r.res = 'Han expulsado a ' + nm(top) + ', que era inocente. ¡Gana el impostor!'; }
}
function checkVotes(r) {
  const need = r.order.filter(id => conn(r.players.get(id)));
  if (need.length && need.every(id => r.votes[id])) tally(r);
}

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'ping') return;
    if (m.t === 'join') {
      const code = String(m.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
      if (!code) return send(ws, { t: 'err', msg: 'Código de sala no válido' });
      let r = rooms.get(code); if (!r) { r = newRoom(code); rooms.set(code, r); }
      const name = String(m.name || '').trim().slice(0, 12) || 'Jugador';
      let p = m.pid ? r.players.get(String(m.pid)) : null;
      if (p) { p.ws = ws; p.name = name; }
      else {
        if (r.phase !== 'lobby') return send(ws, { t: 'err', msg: 'La partida ya ha empezado. Espera a la siguiente.' });
        if (r.players.size >= 12) return send(ws, { t: 'err', msg: 'La sala está llena (máximo 12)' });
        p = { id: crypto.randomUUID(), name, ws }; r.players.set(p.id, p);
        if (!r.host) r.host = p.id;
      }
      ws.p = p; ws.r = r; fixHost(r);
      send(ws, { t: 'joined', pid: p.id, room: code });
      return broadcast(r);
    }
    const p = ws.p, r = ws.r; if (!p || !r || p.ws !== ws) return;
    const isHost = p.id === r.host;
    if (m.t === 'start' && isHost && (r.phase === 'lobby')) {
      const on = [...r.players.values()].filter(conn);
      if (on.length < 3) return;
      const ids = on.map(x => x.id);
      for (let i = ids.length - 1; i > 0; i--) { const j = rnd(i + 1); [ids[i], ids[j]] = [ids[j], ids[i]]; }
      const c = CATS[rnd(CATS.length)];
      r.order = ids; r.imp = ids[rnd(ids.length)]; r.cat = c[0]; r.word = c[1][rnd(c[1].length)];
      r.hint = !!m.hint; r.clues = []; r.votes = {}; r.res = ''; r.phase = 'clue';
      // quitar a los desconectados que no juegan
      for (const [id, q] of r.players) if (!ids.includes(id)) r.players.delete(id);
    } else if (m.t === 'clue' && r.phase === 'clue') {
      if (r.order[r.clues.length % r.order.length] !== p.id) return;
      const w = String(m.w || '').trim().slice(0, 24); if (!w) return;
      r.clues.push({ id: p.id, w });
      if (r.clues.length >= 2 * r.order.length) r.phase = 'vote';
    } else if (m.t === 'skip' && isHost && r.phase === 'clue') {
      r.clues.push({ id: r.order[r.clues.length % r.order.length], w: '(pasa)' });
      if (r.clues.length >= 2 * r.order.length) r.phase = 'vote';
    } else if (m.t === 'goto' && isHost && r.phase === 'clue') {
      r.phase = 'vote';
    } else if (m.t === 'vote' && r.phase === 'vote') {
      if (!r.order.includes(p.id) || !r.order.includes(m.v) || m.v === p.id || r.votes[p.id]) return;
      r.votes[p.id] = m.v; checkVotes(r);
    } else if (m.t === 'guess' && r.phase === 'guess' && p.id === r.imp) {
      const ok = norm(m.g) === norm(r.word);
      r.phase = 'end';
      r.res = ok ? 'El impostor adivinó la palabra. ¡Gana el impostor!' : 'El impostor falló. ¡Gana la tripulación!';
    } else if (m.t === 'lobby' && isHost && r.phase === 'end') {
      r.phase = 'lobby'; r.order = []; r.imp = null; r.clues = []; r.votes = {}; r.res = '';
      for (const [id, q] of r.players) if (!conn(q)) r.players.delete(id);
    } else return;
    broadcast(r);
  });
  ws.on('close', () => {
    const p = ws.p, r = ws.r; if (!p || !r || p.ws !== ws) return;
    p.ws = null;
    if (r.phase === 'lobby') r.players.delete(p.id);
    fixHost(r);
    if (r.phase === 'vote') checkVotes(r);
    broadcast(r);
  });
});

setInterval(() => {  // mantiene vivas las conexiones y limpia salas vacías
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
  for (const [c, r] of rooms) if (![...r.players.values()].some(conn) && Date.now() - r.seen > 30 * 60 * 1000) rooms.delete(c);
}, 30000);

server.listen(PORT, () => console.log('Servidor listo en el puerto ' + PORT));
