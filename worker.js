import { DurableObject } from "cloudflare:workers";

const ROOM_RE = /^[A-Z0-9]{4,8}$/;
const MOVES = new Set(["rock", "paper", "scissors"]);
const MAX_NAME = 24;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function cleanName(value) {
  const s = String(value || "Player").trim().slice(0, MAX_NAME);
  return s || "Player";
}

function winner(hostMove, guestMove) {
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
    this.sessions = new Map();
    this.moves = new Map();
    this.scores = { host: 0, guest: 0 };
    this.round = 0;
    this.gameOver = false;
    this.names = { host: null, guest: null };
    this.loaded = false;
    this.finishingRound = false;
  }

  async load() {
    if (this.loaded) return;
    const saved = await this.ctx.storage.get("match");
    if (saved && typeof saved === "object") {
      this.scores = saved.scores || { host: 0, guest: 0 };
      this.round = Number(saved.round || 0);
      this.gameOver = !!saved.gameOver;
      this.names = saved.names || { host: null, guest: null };
    }
    this.loaded = true;
  }

  async persist() {
    await this.ctx.storage.put("match", {
      scores: this.scores,
      round: this.round,
      gameOver: this.gameOver,
      names: this.names
    });
  }

  players() {
    return [...this.sessions.values()].map(p => ({ role: p.role, name: p.name }));
  }

  send(ws, msg) {
    try {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
    } catch (_) {}
  }

  broadcast(msg, except = null) {
    for (const ws of this.sessions.keys()) {
      if (ws !== except) this.send(ws, msg);
    }
  }

  state() {
    return {
      type: "state",
      players: this.players(),
      scores: this.scores,
      round: this.round,
      gameOver: this.gameOver,
      waiting: this.sessions.size < 2,
      movesSubmitted: {
        host: this.moves.has("host"),
        guest: this.moves.has("guest")
      }
    };
  }

  async fetch(request) {
    await this.load();

    if (request.headers.get("Upgrade") !== "websocket") {
      return json({ ok: true, service: "RPS Arena Online", room: "Durable Object" });
    }

    const url = new URL(request.url);
    const code = String(url.pathname.split("/").filter(Boolean)[1] || "").toUpperCase();
    if (!ROOM_RE.test(code)) return new Response("Invalid room code", { status: 400 });
    if (this.sessions.size >= 2) return new Response("Room is full", { status: 409 });

    const name = cleanName(url.searchParams.get("name"));
    const role = this.sessions.size === 0 ? "host" : "guest";

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    const player = { role, name };
    this.sessions.set(server, player);
    this.names[role] = name;
    await this.persist();

    server.addEventListener("message", e => {
      this.onMessage(server, e.data);
    });
    server.addEventListener("close", () => this.onClose(server));
    server.addEventListener("error", () => this.onClose(server));

    this.send(server, {
      type: "joined",
      role,
      room: code,
      name,
      players: this.players(),
      scores: this.scores,
      round: this.round
    });

    this.broadcast({ type: "players", players: this.players(), ready: this.sessions.size === 2 });
    this.broadcast(this.state());

    return new Response(null, { status: 101, webSocket: client });
  }

  async onClose(ws) {
    const p = this.sessions.get(ws);
    if (!p) return;
    this.sessions.delete(ws);
    this.moves.clear();
    this.names[p.role] = null;

    // If the host leaves while the guest is still connected, promote the
    // remaining player so the next player can always join as guest.
    if (p.role === "host" && this.sessions.size === 1) {
      const remaining = [...this.sessions.entries()][0];
      remaining[1].role = "host";
      this.names.host = remaining[1].name;
      this.names.guest = null;
      this.send(remaining[0], {
        type: "role_changed",
        role: "host",
        name: remaining[1].name
      });
    }

    await this.persist();
    this.broadcast({ type: "player_left", role: p.role, name: p.name, players: this.players() });
    this.broadcast(this.state());
  }

  async onMessage(ws, raw) {
    await this.load();
    const p = this.sessions.get(ws);
    if (!p) return;

    let msg;
    try { msg = JSON.parse(String(raw)); }
    catch (_) { this.send(ws, { type: "error", message: "پیام نامعتبر است." }); return; }

    if (msg.type === "ping") {
      this.send(ws, { type: "pong", serverTime: Date.now() });
      return;
    }

    if (msg.type === "state") {
      this.send(ws, this.state());
      return;
    }

    if (msg.type === "reset") {
      if (p.role !== "host") {
        this.send(ws, { type: "error", message: "فقط میزبان می‌تواند مسابقه را ریست کند." });
        return;
      }
      this.moves.clear();
      this.scores = { host: 0, guest: 0 };
      this.round = 0;
      this.gameOver = false;
      await this.persist();
      this.broadcast({ type: "game_reset", scores: this.scores, round: 0, gameOver: false });
      this.broadcast(this.state());
      return;
    }

    if (msg.type === "move") {
      const move = String(msg.move || "").toLowerCase();
      if (!MOVES.has(move)) {
        this.send(ws, { type: "error", message: "حرکت نامعتبر است." });
        return;
      }
      if (this.sessions.size < 2) {
        this.send(ws, { type: "error", message: "منتظر بازیکن دوم باشید." });
        return;
      }
      if (this.gameOver) {
        this.send(ws, { type: "error", message: "مسابقه تمام شده است؛ ریست کنید." });
        return;
      }
      if (this.moves.has(p.role)) {
        this.send(ws, { type: "error", message: "حرکت این دور قبلاً ثبت شده است." });
        return;
      }

      this.moves.set(p.role, move);
      this.broadcast({
        type: "move_received",
        role: p.role,
        name: p.name,
        movesSubmitted: { host: this.moves.has("host"), guest: this.moves.has("guest") }
      });

      if (this.moves.has("host") && this.moves.has("guest")) await this.finishRound();
    }
  }

  async finishRound() {
    if (this.finishingRound) return;
    this.finishingRound = true;
    try {
      const hostMove = this.moves.get("host");
    const guestMove = this.moves.get("guest");
    if (!hostMove || !guestMove) return;

    const roundWinner = winner(hostMove, guestMove);
    if (roundWinner === "host") this.scores.host++;
    if (roundWinner === "guest") this.scores.guest++;
    this.round++;
    this.gameOver = this.scores.host >= 3 || this.scores.guest >= 3;

    this.broadcast({
      type: "round_result",
      round: this.round,
      hostMove,
      guestMove,
      winner: roundWinner,
      scores: this.scores,
      gameOver: this.gameOver,
      matchWinner: this.scores.host >= 3 ? "host" : this.scores.guest >= 3 ? "guest" : null
    });

    this.moves.clear();
    await this.persist();
      this.broadcast(this.state());
    } finally {
      this.finishingRound = false;
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return json({ ok: true, service: "RPS Arena Online", status: "healthy", time: new Date().toISOString() });
    }

    if (url.pathname.startsWith("/room/")) {
      const code = String(url.pathname.split("/").filter(Boolean)[1] || "").toUpperCase();
      if (!ROOM_RE.test(code)) return new Response("Invalid room code", { status: 400 });
      const id = env.ROOM.idFromName(code);
      return env.ROOM.get(id).fetch(request);
    }

    // Everything else (including /) is served by Workers Static Assets.
    return env.ASSETS.fetch(request);
  }
};
