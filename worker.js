import { DurableObject } from "cloudflare:workers";

/*
  RPS Arena Online - Cloudflare Worker + Durable Object
  - 2 players per room
  - WebSocket realtime communication
  - Best-of-5 scoring
  - Room state is kept in the Durable Object
  - SQLite-backed Durable Object (configured by wrangler.toml)
*/

const MAX_NAME = 24;
const ROOM_RE = /^[A-Z0-9]{4,8}$/;
const MOVES = new Set(["rock", "paper", "scissors"]);

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function safeName(value) {
  const name = String(value || "Player").trim().slice(0, MAX_NAME);
  return name || "Player";
}

function resultFor(hostMove, guestMove) {
  if (hostMove === guestMove) return "draw";

  const hostWins =
    (hostMove === "rock" && guestMove === "scissors") ||
    (hostMove === "paper" && guestMove === "rock") ||
    (hostMove === "scissors" && guestMove === "paper");

  return hostWins ? "host" : "guest";
}

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;

    // One room is one Durable Object instance.
    // WebSockets are kept in memory while the object is active.
    this.sessions = new Map();
    this.moves = new Map();
    this.scores = { host: 0, guest: 0 };
    this.round = 0;
    this.gameOver = false;
    this.names = { host: null, guest: null };
  }

  playerList() {
    return [...this.sessions.values()].map((p) => ({
      role: p.role,
      name: p.name
    }));
  }

  send(ws, message) {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
      }
    } catch (_) {}
  }

  broadcast(message, except = null) {
    for (const ws of this.sessions.keys()) {
      if (ws !== except) this.send(ws, message);
    }
  }

  sendState(ws) {
    this.send(ws, {
      type: "state",
      players: this.playerList(),
      scores: this.scores,
      round: this.round,
      gameOver: this.gameOver,
      waiting: this.sessions.size < 2,
      movesSubmitted: {
        host: this.moves.has("host"),
        guest: this.moves.has("guest")
      }
    });
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return json({
        ok: true,
        service: "RPS Arena Online",
        version: "2.0.0",
        websocket: "Use /room/ROOMCODE?name=PLAYER"
      });
    }

    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);
    const roomCode = String(parts[1] || "").toUpperCase();

    if (!ROOM_RE.test(roomCode)) {
      return new Response("Invalid room code", { status: 400 });
    }

    if (this.sessions.size >= 2) {
      return new Response("Room is full", { status: 409 });
    }

    const requestedName = safeName(url.searchParams.get("name"));

    // If the room was empty, first player is host.
    const role = this.sessions.size === 0 ? "host" : "guest";

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept();

    this.sessions.set(server, {
      role,
      name: requestedName
    });
    this.names[role] = requestedName;

    server.addEventListener("message", (event) => {
      this.onMessage(server, event.data);
    });

    server.addEventListener("close", () => {
      this.onClose(server);
    });

    server.addEventListener("error", () => {
      this.onClose(server);
    });

    this.send(server, {
      type: "joined",
      role,
      room: roomCode,
      name: requestedName,
      players: this.playerList(),
      scores: this.scores,
      round: this.round
    });

    this.broadcast({
      type: "players",
      players: this.playerList(),
      ready: this.sessions.size === 2
    });

    this.broadcast({
      type: "state",
      players: this.playerList(),
      scores: this.scores,
      round: this.round,
      gameOver: this.gameOver,
      waiting: this.sessions.size < 2,
      movesSubmitted: {
        host: this.moves.has("host"),
        guest: this.moves.has("guest")
      }
    });

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }

  onClose(ws) {
    const player = this.sessions.get(ws);
    if (!player) return;

    this.sessions.delete(ws);

    if (this.names[player.role] === player.name) {
      this.names[player.role] = null;
    }

    // A disconnected player cannot leave a pending move behind.
    this.moves.delete(player.role);

    this.broadcast({
      type: "player_left",
      role: player.role,
      name: player.name,
      players: this.playerList()
    });
  }

  onMessage(ws, raw) {
    const player = this.sessions.get(ws);
    if (!player) return;

    let msg;
    try {
      msg = JSON.parse(String(raw));
    } catch (_) {
      this.send(ws, { type: "error", message: "Invalid message" });
      return;
    }

    const type = String(msg.type || "");

    if (type === "ping") {
      this.send(ws, {
        type: "pong",
        serverTime: Date.now()
      });
      return;
    }

    if (type === "state") {
      this.sendState(ws);
      return;
    }

    if (type === "reset") {
      // Only host can reset the match.
      if (player.role !== "host") {
        this.send(ws, {
          type: "error",
          message: "Only the host can reset the game."
        });
        return;
      }

      this.resetGame();
      return;
    }

    if (type === "move") {
      this.submitMove(ws, msg.move);
      return;
    }
  }

  submitMove(ws, rawMove) {
    const player = this.sessions.get(ws);
    if (!player) return;

    const move = String(rawMove || "").toLowerCase();

    if (!MOVES.has(move)) {
      this.send(ws, {
        type: "error",
        message: "Invalid move. Use rock, paper, or scissors."
      });
      return;
    }

    if (this.sessions.size < 2) {
      this.send(ws, {
        type: "error",
        message: "Waiting for the second player."
      });
      return;
    }

    if (this.gameOver) {
      this.send(ws, {
        type: "error",
        message: "Game is over. Reset the match to play again."
      });
      return;
    }

    if (this.moves.has(player.role)) {
      this.send(ws, {
        type: "error",
        message: "You already submitted this round."
      });
      return;
    }

    this.moves.set(player.role, move);

    // Never reveal the actual move before both players have submitted.
    this.broadcast({
      type: "move_received",
      role: player.role,
      name: player.name,
      ready: true,
      movesSubmitted: {
        host: this.moves.has("host"),
        guest: this.moves.has("guest")
      }
    });

    if (this.moves.has("host") && this.moves.has("guest")) {
      this.finishRound();
    }
  }

  finishRound() {
    const hostMove = this.moves.get("host");
    const guestMove = this.moves.get("guest");

    if (!hostMove || !guestMove) return;

    const winner = resultFor(hostMove, guestMove);

    if (winner === "host") this.scores.host++;
    if (winner === "guest") this.scores.guest++;

    this.round++;

    // Best of 5 = first to 3 round wins.
    if (this.scores.host >= 3 || this.scores.guest >= 3) {
      this.gameOver = true;
    }

    this.broadcast({
      type: "round_result",
      round: this.round,
      hostMove,
      guestMove,
      winner,
      scores: this.scores,
      gameOver: this.gameOver,
      matchWinner:
        this.scores.host >= 3
          ? "host"
          : this.scores.guest >= 3
            ? "guest"
            : null
    });

    this.moves.clear();

    this.broadcast({
      type: "state",
      players: this.playerList(),
      scores: this.scores,
      round: this.round,
      gameOver: this.gameOver,
      waiting: false,
      movesSubmitted: {
        host: false,
        guest: false
      }
    });
  }

  resetGame() {
    this.moves.clear();
    this.scores = { host: 0, guest: 0 };
    this.round = 0;
    this.gameOver = false;

    this.broadcast({
      type: "game_reset",
      scores: this.scores,
      round: this.round,
      gameOver: false
    });

    this.broadcast({
      type: "state",
      players: this.playerList(),
      scores: this.scores,
      round: 0,
      gameOver: false,
      waiting: this.sessions.size < 2,
      movesSubmitted: {
        host: false,
        guest: false
      }
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "") {
      return json({
        ok: true,
        service: "RPS Arena Online",
        version: "2.0.0",
        status: "online",
        websocket: "/room/ROOMCODE?name=PLAYER"
      });
    }

    if (url.pathname === "/health") {
      return json({
        ok: true,
        service: "RPS Arena Online",
        status: "healthy",
        time: new Date().toISOString()
      });
    }

    if (url.pathname.startsWith("/room/")) {
      const roomCode = String(
        url.pathname.split("/").filter(Boolean)[1] || ""
      ).toUpperCase();

      if (!ROOM_RE.test(roomCode)) {
        return new Response("Invalid room code", { status: 400 });
      }

      const id = env.ROOM.idFromName(roomCode);
      const stub = env.ROOM.get(id);

      return stub.fetch(request);
    }

    return json({
      ok: false,
      error: "Not found"
    }, 404);
  }
};
