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
  rooms.delete(room.code);
}
function genCode() {
  for (let tries = 0; tries < 50; tries++) {
    const code = String(Math.floor(1000 + Math.random() * 9000)); // 4桁
    if (!rooms.has(code)) return code;
  }
  return String(Math.floor(100000 + Math.random() * 900000)); // 混雑時は6桁
}

function makeRoom(code, hostWs) {
  const room = {
    code,
    players: [{ ws: hostWs, color: 1 }], // 先手=1(黒), 後手=2(白)
    board: newBoard(),
    moves: 0,
    turn: 1,
    over: false,
    rematchWants: new Set(),
    ttl: null,
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
  room.turn = 1;
  room.over = false;
  room.rematchWants.clear();
  if (room.ttl) { clearTimeout(room.ttl); room.ttl = null; }
  room.players.forEach((p) => send(p.ws, { type: "start", you: p.color, turn: 1 }));
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
      makeRoom(code, ws);
      send(ws, { type: "created", code });
      return;
    }
    if (msg.type === "join") {
      const code = String(msg.code || "");
      if (!/^\d{4,6}$/.test(code)) { send(ws, { type: "error", reason: "bad_code" }); return; }
      const room = rooms.get(code);
      if (!room) { send(ws, { type: "error", reason: "no_room" }); return; }
      if (room.players.length >= 2) { send(ws, { type: "error", reason: "full" }); return; }
      if (ws._room) leaveRoom(ws);
      room.players.push({ ws, color: 2 });
      ws._room = room;
      startGame(room);
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
      const win = checkWin(room.board, r, c, me.color);
      const draw = !win && room.moves >= SIZE * SIZE;
      broadcast(room, { type: "move", r, c, p: me.color });
      if (win) {
        room.over = true;
        broadcast(room, { type: "game_over", winner: me.color, reason: "five" });
      } else if (draw) {
        room.over = true;
        broadcast(room, { type: "game_over", winner: 0, reason: "draw" });
      } else {
        room.turn = me.color === 1 ? 2 : 1;
      }
      return;
    }
    if (msg.type === "rematch") {
      if (!room.over) return;
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
  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});

httpServer.listen(PORT, () => {
  console.log("gomoku relay listening on :" + PORT);
});
