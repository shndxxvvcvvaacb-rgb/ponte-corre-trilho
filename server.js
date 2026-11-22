const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require('tiktok-live-connector');

const PORT = process.env.PORT || 8787;
const MAX_LIVES = parseInt(process.env.MAX_LIVES, 10) || 20;
const MAX_MIN = parseInt(process.env.MAX_MIN, 10) || 15;
const MAX_PER_IP = 3;

let lives = 0;
const porIp = new Map();

function acharHtml() {
  try {
    const f = fs.readdirSync(__dirname).filter((n) => n.toLowerCase().endsWith('.html'));
    return f.length ? path.join(__dirname, f[0]) : null;
  } catch (e) { return null; }
}

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/status') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end('Ponte Corre Trilho ligada. Lives agora: ' + lives);
  }
  if (url === '/') {
    const arq = acharHtml();
    if (!arq) { res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('Ponte ligada, mas nao achei o jogo (.html).'); }
    fs.readFile(arq, (err, data) => {
      if (err) { res.writeHead(500); return res.end('Erro'); }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Nao encontrado');
});
const wss = new WebSocketServer({ server, maxPayload: 1024 });

const send = (ws, obj) => { try { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch (e) {} };

wss.on('connection', (ws, req) => {
  const ip = String((req.headers['x-forwarded-for'] || req.socket.remoteAddress || '')).split(',')[0].trim();
  const n = (porIp.get(ip) || 0) + 1;
  if (n > MAX_PER_IP) { send(ws, { type: 'error', msg: 'Muitas conexões deste aparelho. Feche as outras abas.' }); ws.close(); return; }
  porIp.set(ip, n);

  let conn = null, counted = false, timer = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  const stop = () => {
    clearTimeout(timer); timer = null;
    const c = conn; conn = null;
    if (c) { try { c.disconnect(); } catch (e) {} }
    if (counted) { counted = false; lives = Math.max(0, lives - 1); }
  };

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (msg.cmd === 'disconnect') return stop();
    if (msg.cmd !== 'connect' || conn) return;

    const user = String(msg.username || '').replace(/^@+/, '').trim();
    if (!/^[A-Za-z0-9_.]{2,24}$/.test(user)) return send(ws, { type: 'error', msg: 'Nome de usuário inválido.' });
    if (lives >= MAX_LIVES) return send(ws, { type: 'error', msg: 'O servidor está cheio agora. Tente de novo daqui a pouco.' });

    const opts = process.env.SIGN_API_KEY ? { signApiKey: process.env.SIGN_API_KEY } : {};
    const c = new TikTokLiveConnection(user, opts);
    conn = c; counted = true; lives++;

    c.on(WebcastEvent.GIFT, (data) => {
      const tipo = data.giftDetails && data.giftDetails.giftType;
      if (tipo === 1 && !data.repeatEnd) return;
      const nome = (data.giftDetails && data.giftDetails.giftName) || String(data.giftId || '');
      const quem = (data.user && (data.user.uniqueId || data.user.nickname)) || '';
      send(ws, { type: 'gift', gift: nome, count: data.repeatCount || 1, user: quem });
    });
    c.on(WebcastEvent.STREAM_END, () => send(ws, { type: 'end' }));
    c.on(ControlEvent.DISCONNECTED, () => { if (conn === c) send(ws, { type: 'end' }); });

    try {
      const state = await c.connect();
      if (conn !== c) { try { c.disconnect(); } catch (e) {} return; }
      send(ws, { type: 'live', room: String(state.roomId) });
      timer = setTimeout(() => { send(ws, { type: 'end' }); stop(); }, MAX_MIN * 60000);
    } catch (err) {
      console.log('Falha ao entrar em @' + user + ':', err && err.message ? err.message : err);
      stop();
      send(ws, { type: 'error', msg: 'Não consegui entrar na live de @' + user + '. Ela está ao vivo agora?' });
    }
  });

  const sair = () => { stop(); const k = (porIp.get(ip) || 1) - 1; if (k <= 0) porIp.delete(ip); else porIp.set(ip, k); };
  ws.on('close', sair);
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; try { ws.ping(); } catch (e) {}
  });
}, 30000);

server.listen(PORT, () => console.log('Ponte online ligada na porta ' + PORT));
