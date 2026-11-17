// とらのごもく — あいことば対戦 中継サーバー
// 役割: 部屋の管理、手番の管理、着手の正当性チェック（範囲/空き/手番）、
//       勝敗・引き分けの判定、相手への中継、切断の通知。
// 個人情報は一切扱わない。あいことばは数字4〜6桁、部屋は対戦終了/全員退出で消える。
"use strict";
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8377;
const SIZE = 15;
const DIRS = [[1, 0], [0, 1], [1, 1], [1, -1]];
const ROOM_TTL_MS = 10 * 60 * 1000; // 相手が来ない部屋の寿命
// 対局成立後の無操作タイムアウト (user spec 2026-11-12): 片方が
// 10分間うごかないと、両方に idle_timeout を知らせて部屋を閉じる。
// テスト用に環境変数で短縮できる (本番は既定の10分)。
const IDLE_TIMEOUT_MS = Number(process.env.IDLE_TIMEOUT_MS) || 10 * 60 * 1000;
const IDLE_SWEEP_MS = Math.max(250, Math.min(30000, Math.floor(IDLE_TIMEOUT_MS / 2)));
// 再接続の猶予 (user request 2026-11-17): 対局成立後に片方の回線が
// 切れても、部屋はすぐ消さず GRACE_MS の間その席を空けて待つ。
// その間に本人 (あいことば+席トークン) が戻れば対局を再開できる。
// テスト用に環境変数で短縮できる (本番は既定の30秒)。
const GRACE_MS = Number(process.env.REJOIN_GRACE_MS) || 30000;
function newToken() {
  return Math.random().toString(16).slice(2, 8) + Math.random().toString(16).slice(2, 8);
}

const rooms = new Map(); // code -> room

function newBoard() {
  return Array.from({ length: SIZE }, () => new Array(SIZE).fill(0));
}
function checkWin(board, r, c, p) {
  for (const [dr, dc] of DIRS) {
    let cnt = 1;
    for (let i = 1; i < 5; i++) {
      const rr = r + dr * i, cc = c + dc * i;
      if (rr < 0 || rr >= SIZE || cc < 0 || cc >= SIZE || board[rr][cc] !== p) break;
      cnt++;
    }
    for (let i = 1; i < 5; i++) {
      const rr = r - dr * i, cc = c - dc * i;
      if (rr < 0 || rr >= SIZE || cc < 0 || cc >= SIZE || board[rr][cc] !== p) break;
      cnt++;
    }
    if (cnt >= 5) return true;
  }
  return false;
}
function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj) {
  room.players.forEach((p) => p && send(p.ws, obj));
}
function cleanupRoom(room) {
  if (room.ttl) clearTimeout(room.ttl);
  if (room.graceTimer) clearTimeout(room.graceTimer);
  rooms.delete(room.code);
}
function genCode() {
  for (let tries = 0; tries < 50; tries++) {
    const code = String(Math.floor(1000 + Math.random() * 9000)); // 4桁
    if (!rooms.has(code)) return code;
  }
  return String(Math.floor(100000 + Math.random() * 900000)); // 混雑時は6桁
}

function makeRoom(code, hostWs, isPrivate) {
  const room = {
    code,
    // プライベートな部屋 (user spec 2026-11-17): あいことばを
    // 知っている人だけが入れる。クイックマッチの対象にはならない。
    priv: !!isPrivate,
    players: [{ ws: hostWs, color: 1, token: newToken(), away: false }], // 先手=1(黒), 後手=2(白)
    board: newBoard(),
    moves: 0,
    history: [], // 再接続時の局面復元用に全着手を保持する
    winner: null, // 決着済みのとき 0(引分)/1/2、未決着は null
    gameSeq: 0, // startGame のたびに増える。再接続側が「新しい対局が始まっていた」ことを検知する
    graceTimer: null,
    turn: 1,
    over: false,
    rematchWants: new Set(),
    ttl: null,
    lastActivity: Date.now(),
  };
  room.ttl = setTimeout(() => {
    if (room.players.length < 2) {
      send(hostWs, { type: "room_expired" });
      cleanupRoom(room);
    }
  }, ROOM_TTL_MS);
  hostWs._room = room;
  rooms.set(code, room);
  return room;
}

function startGame(room) {
  room.board = newBoard();
  room.moves = 0;
  room.history = [];
  room.winner = null;
  room.gameSeq++;
  room.turn = 1;
  room.over = false;
  room.rematchWants.clear();
  room.lastActivity = Date.now();
  if (room.ttl) { clearTimeout(room.ttl); room.ttl = null; }
  room.players.forEach((p) => send(p.ws, { type: "start", you: p.color, turn: 1, token: p.token, seq: room.gameSeq, code: room.code }));
}

// 回線が切れたときの処理 (user request 2026-11-17): 対局が成立
// している部屋では即座に部屋を消さず、席を空席 (away) にして
// GRACE_MS 待つ。相手には peer_away を知らせる。猶予内に本人が
// 戻らなければ、従来どおり opponent_left を送って部屋を閉じる。
// まだ相手が来ていない部屋は従来どおり即座に片付ける。
function armGrace(room) {
  if (room.graceTimer) clearTimeout(room.graceTimer);
  room.graceTimer = setTimeout(() => {
    room.graceTimer = null;
    if (!room.players.some((p) => p && p.away)) return;
    room.over = true;
    room.players.forEach((p) => { if (p && p.ws) { send(p.ws, { type: "opponent_left" }); p.ws._room = null; } });
    cleanupRoom(room);
  }, GRACE_MS);
}
function dropSeat(ws) {
  const room = ws._room;
  if (!room) return;
  if (room.gameSeq < 1) { leaveRoom(ws); return; }
  const seat = room.players.find((p) => p && p.ws === ws);
  ws._room = null;
  if (!seat) return;
  seat.ws = null;
  seat.away = true;
  room.players.forEach((p) => { if (p && p.ws) send(p.ws, { type: "peer_away" }); });
  armGrace(room);
}

function leaveRoom(ws) {
  const room = ws._room;
  if (!room) return;
  ws._room = null;
  // The peer is told in EVERY phase: mid-match AND after the
  // game is decided, while the result screen waits (a rematch
  // may already have been offered). The old `!room.over` guard
  // kept post-game leaves silent, so the waiting player never
  // learned the opponent had gone (user report 2026-11-10).
  room.over = true;
  room.players.forEach((p) => { if (p && p.ws !== ws) send(p.ws, { type: "opponent_left" }); });
  cleanupRoom(room);
}

const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
  } else {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("torano-gomoku relay server");
  }
});

const wss = new WebSocketServer({ server: httpServer, maxPayload: 4096 });
wss.on("connection", (ws) => {
  ws._room = null;
  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(String(data)); } catch (e) { return; }
    if (!msg || typeof msg.type !== "string") return;

    if (msg.type === "create") {
      if (ws._room) leaveRoom(ws);
      const code = genCode();
      const room = makeRoom(code, ws, msg.private === true);
      send(ws, { type: "created", code, private: room.priv });
      return;
    }
    if (msg.type === "list") {
      // 「へやを さがす」用 (user spec 2026-11-17): 待機中の公開部屋の
      // あいことば一覧を返す。プライベートな部屋は載せない。
      // 入る操作自体は既存の join (あいことば指定) で行う。
      if (ws._room) return;
      const codes = [];
      rooms.forEach((room) => {
        if (codes.length < 6 && room.players.length < 2 && !room.priv) codes.push(room.code);
      });
      send(ws, { type: "rooms", rooms: codes });
      return;
    }
    if (msg.type === "quick") {
      // クイックマッチ (user spec 2026-11-17): 待機中の公開部屋が
      // あれば客として入り、なければ自分がホストで公開部屋を作る。
      // プライベートな部屋と自分の部屋は対象にしない。
      let target = null;
      rooms.forEach((room) => {
        if (!target && room !== ws._room && room.players.length < 2 && !room.priv) target = room;
      });
      if (ws._room) leaveRoom(ws);
      if (target) {
        target.players.push({ ws, color: 2, token: newToken(), away: false });
        ws._room = target;
        startGame(target);
      } else {
        const code = genCode();
        makeRoom(code, ws, false);
        send(ws, { type: "created", code, private: false });
      }
      return;
    }
    if (msg.type === "join") {
      const code = String(msg.code || "");
      if (!/^\d{4,6}$/.test(code)) { send(ws, { type: "error", reason: "bad_code" }); return; }
      const room = rooms.get(code);
      if (!room) { send(ws, { type: "error", reason: "no_room" }); return; }
      if (room.players.length >= 2) { send(ws, { type: "error", reason: "full" }); return; }
      if (ws._room) leaveRoom(ws);
      room.players.push({ ws, color: 2, token: newToken(), away: false });
      ws._room = room;
      startGame(room);
      return;
    }
    if (msg.type === "rejoin") {
      // 切断からの復帰 (user request 2026-11-17): あいことばと
      // 席トークンが一致し、その席が空席 (away) のときだけ
      // 受け付ける。局面は全着手の履歴で返す。
      const room = rooms.get(String(msg.code || ""));
      const seat = room && room.players.find((p) => p && p.away && p.token === String(msg.token || ""));
      if (!room || !seat) { send(ws, { type: "error", reason: "no_room" }); return; }
      if (ws._room) leaveRoom(ws);
      seat.ws = ws;
      seat.away = false;
      ws._room = room;
      if (!room.players.some((p) => p && p.away) && room.graceTimer) {
        clearTimeout(room.graceTimer);
        room.graceTimer = null;
      }
      send(ws, {
        type: "rejoined", you: seat.color, turn: room.turn, seq: room.gameSeq,
        moves: room.history, over: room.over, winner: room.winner,
        rematch: Array.from(room.rematchWants),
      });
      room.players.forEach((p) => { if (p && p.ws && p.ws !== ws) send(p.ws, { type: "peer_back" }); });
      return;
    }
    const room = ws._room;
    if (!room) return;
    const me = room.players.find((p) => p && p.ws === ws);
    if (!me) return;

    if (msg.type === "move") {
      if (room.over || room.players.length < 2) return;
      if (room.turn !== me.color) return; // 自分の番でなければ無効
      const r = msg.r | 0, c = msg.c | 0;
      if (r < 0 || r >= SIZE || c < 0 || c >= SIZE) return;
      if (room.board[r][c] !== 0) return;
      room.board[r][c] = me.color;
      room.moves++;
      room.history.push({ r, c, p: me.color });
      room.lastActivity = Date.now();
      const win = checkWin(room.board, r, c, me.color);
      const draw = !win && room.moves >= SIZE * SIZE;
      broadcast(room, { type: "move", r, c, p: me.color });
      if (win) {
        room.over = true;
        room.winner = me.color;
        broadcast(room, { type: "game_over", winner: me.color, reason: "five" });
      } else if (draw) {
        room.over = true;
        room.winner = 0;
        broadcast(room, { type: "game_over", winner: 0, reason: "draw" });
      } else {
        room.turn = me.color === 1 ? 2 : 1;
      }
      return;
    }
    if (msg.type === "rematch") {
      if (!room.over) return;
      room.lastActivity = Date.now();
      room.rematchWants.add(me.color);
      if (room.rematchWants.size >= 2) {
        // 再戦では先手・後手を入れ替える（公平のため）
        room.players.forEach((p) => { p.color = p.color === 1 ? 2 : 1; });
        startGame(room);
      } else {
        room.players.forEach((p) => { if (p && p.ws !== ws) send(p.ws, { type: "rematch_offer" }); });
      }
      return;
    }
    if (msg.type === "leave") { leaveRoom(ws); return; }
  });
  ws.on("close", () => dropSeat(ws));
  ws.on("error", () => dropSeat(ws));
});

// 無操作の部屋を見回る: 対局が成立した部屋 (2人そろった部屋) で
// 最後の着手/再戦申し込みから IDLE_TIMEOUT_MS 経つと、両方へ
// idle_timeout を送って部屋を閉じる。相手が来ない部屋は従来の
// TTL (room_expired) が担当するので、ここでは扱わない。
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room) => {
    if (room.players.length < 2) return;
    if (room.players.some((p) => p && p.away)) return; // 再接続の猶予中は armGrace が担当
    if (now - room.lastActivity > IDLE_TIMEOUT_MS) {
      broadcast(room, { type: "idle_timeout" });
      room.players.forEach((p) => { if (p) p.ws._room = null; });
      cleanupRoom(room);
    }
  });
}, IDLE_SWEEP_MS);

httpServer.listen(PORT, () => {
  console.log("gomoku relay listening on :" + PORT);
});
