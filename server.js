const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 2e6 });
const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, "public");
const DATA = path.join(ROOT, "data.json");
const UPLOADS = path.join(PUBLIC, "uploads");
fs.mkdirSync(UPLOADS, { recursive: true });

/* ---------- UPLOAD (raw stream, no size limit) ---------- */
app.post("/api/upload", (req, res) => {
  const token = String(req.query.token || "");
  const username = db.sessions[token];
  if (!username || !db.users[username]) return res.status(401).json({ error: "auth" });

  const rawName = decodeURIComponent(String(req.headers["x-file-name"] || "file"));
  const safeName = rawName.replace(/[\\/:*?"<>|]/g, "_").slice(0, 200) || "file";
  const ext = (path.extname(safeName) || "").slice(0, 12);
  const id = crypto.randomBytes(10).toString("hex");
  const fileName = id + ext;
  const dest = path.join(UPLOADS, fileName);

  const ws = fs.createWriteStream(dest);
  let size = 0, aborted = false;
  req.on("data", c => { size += c.length; });
  req.on("aborted", () => { aborted = true; ws.destroy(); try { fs.unlinkSync(dest); } catch {} });
  req.on("error", () => { aborted = true; ws.destroy(); });
  ws.on("error", () => { if (!aborted) res.status(500).json({ error: "write" }); });
  ws.on("finish", () => {
    if (aborted) return;
    res.json({
      url: "/uploads/" + fileName,
      name: safeName,
      size,
      type: String(req.headers["content-type"] || "application/octet-stream")
    });
  });
  req.pipe(ws);
});

app.use(express.json({ limit: "2mb" }));
app.use(express.static(PUBLIC));

/* ---------- DB ---------- */
let db = { users: {}, chats: {}, messages: [], sessions: {} };
try { if (fs.existsSync(DATA)) db = JSON.parse(fs.readFileSync(DATA, "utf8")); } catch (e) { console.error("db load failed", e); }
db.users ||= {}; db.chats ||= {}; db.messages ||= []; db.sessions ||= {};

const online = new Map();
const clean = s => String(s == null ? "" : s).trim().replace(/\s+/g, " ");
const uid = () => crypto.randomBytes(12).toString("hex");
const save = () => { try { fs.writeFileSync(DATA, JSON.stringify(db)); } catch (e) { console.error("save failed", e); } };

const hashPassword = (pw, salt) => crypto.scryptSync(String(pw), salt, 64).toString("hex");
function verifyPassword(pw, salt, hash) {
  try {
    const a = Buffer.from(hashPassword(pw, salt), "hex");
    const b = Buffer.from(String(hash || ""), "hex");
    return a.length === b.length && a.length > 0 && crypto.timingSafeEqual(a, b);
  } catch { return false; }
}

function publicUser(u) {
  if (!u) return null;
  return {
    username: u.username,
    displayName: u.displayName || u.username,
    bio: u.bio || "",
    avatar: u.avatar || "/bird.jpg",
    createdAt: u.createdAt,
    online: online.has(u.username)
  };
}

function lastMessage(chatId) {
  for (let i = db.messages.length - 1; i >= 0; i--) if (db.messages[i].chatId === chatId) return db.messages[i];
  return null;
}

function unreadCount(chat, username) {
  const lastRead = (chat.reads && chat.reads[username]) || 0;
  let n = 0;
  for (let i = db.messages.length - 1; i >= 0; i--) {
    const m = db.messages[i];
    if (m.chatId !== chat.id) continue;
    if (m.time <= lastRead) break;
    if (m.from !== username) n++;
  }
  return n;
}

function previewOf(m) {
  if (!m) return "";
  if (m.type === "image") return "📷 Фото";
  if (m.type === "video") return "🎥 Видео";
  if (m.type === "audio") return "🎙️ Голосовое";
  if (m.type === "file") return "📎 " + (m.name || "Файл");
  return m.text || "";
}

function chatSummary(chat, forUser) {
  const last = lastMessage(chat.id);
  let name = chat.name, avatar = chat.avatar;
  if (chat.type === "private") {
    const other = chat.members.find(m => m !== forUser);
    const u = db.users[other];
    name = u ? (u.displayName || u.username) : "Чат";
    avatar = u ? (u.avatar || "/bird.jpg") : "/bird.jpg";
  }
  return {
    id: chat.id,
    type: chat.type,
    name: name || "Группа",
    avatar: avatar || "/bird.jpg",
    members: chat.members.map(m => publicUser(db.users[m])).filter(Boolean),
    createdBy: chat.createdBy || null,
    createdAt: chat.createdAt,
    lastMessage: last ? { id: last.id, from: last.from, text: previewOf(last), time: last.time, type: last.type } : null,
    unread: unreadCount(chat, forUser)
  };
}

const userChats = username =>
  Object.values(db.chats)
    .filter(c => c.members.includes(username))
    .map(c => chatSummary(c, username));

function emitToChat(chat, event, payload) {
  chat.members.forEach(m => io.to("u:" + m).emit(event, payload));
}
function broadcastChatUpdate(chat) {
  chat.members.forEach(m => io.to("u:" + m).emit("chatUpdate", chatSummary(chat, m)));
}
function broadcastUserList() {
  io.emit("userList", Object.values(db.users).map(publicUser).filter(Boolean));
}

/* ---------- AUTH ROUTES ---------- */
function makeSession(username) {
  const token = crypto.randomBytes(24).toString("hex");
  db.sessions[token] = username;
  save();
  return token;
}

app.post("/api/register", (req, res) => {
  const username = clean(req.body?.username).toLowerCase().replace(/[^a-z0-9_.-]/g, "");
  const password = String(req.body?.password || "");
  const displayName = clean(req.body?.displayName).slice(0, 40) || username;

  if (username.length < 3) return res.status(400).json({ error: "Имя пользователя минимум 3 символа (a-z, 0-9, _ . -)" });
  if (!/^[a-z0-9_.-]+$/.test(username)) return res.status(400).json({ error: "Только латиница, цифры и _ . -" });
  if (password.length < 4) return res.status(400).json({ error: "Пароль минимум 4 символа" });
  if (db.users[username]) return res.status(400).json({ error: "Такой пользователь уже существует" });

  const salt = crypto.randomBytes(16).toString("hex");
  db.users[username] = {
    username,
    displayName,
    bio: "",
    avatar: "/bird.jpg",
    passwordHash: hashPassword(password, salt),
    salt,
    createdAt: Date.now()
  };
  const token = makeSession(username);
  save();
  broadcastUserList();
  res.json({ token, user: publicUser(db.users[username]) });
});

app.post("/api/login", (req, res) => {
  const username = clean(req.body?.username).toLowerCase();
  const password = String(req.body?.password || "");
  const u = db.users[username];
  if (!u || !u.passwordHash) return res.status(400).json({ error: "Неверное имя или пароль" });
  if (!verifyPassword(password, u.salt, u.passwordHash)) return res.status(400).json({ error: "Неверное имя или пароль" });
  const token = makeSession(username);
  res.json({ token, user: publicUser(u) });
});

app.post("/api/logout", (req, res) => {
  const token = String(req.body?.token || req.query?.token || "");
  if (token && db.sessions[token]) { delete db.sessions[token]; save(); }
  res.json({ ok: true });
});

app.get("/api/me", (req, res) => {
  const token = String(req.query.token || "");
  const username = db.sessions[token];
  if (!username || !db.users[username]) return res.status(401).json({ error: "auth" });
  res.json({ user: publicUser(db.users[username]) });
});

/* ---------- RTC CONFIG ---------- */
app.get("/api/rtc-config", (req, res) => {
  const ice = [{ urls: "stun:stun.l.google.com:19302" }, { urls: "stun:stun1.l.google.com:19302" }];
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    ice.push({ urls: process.env.TURN_URL, username: process.env.TURN_USERNAME, credential: process.env.TURN_CREDENTIAL });
  }
  res.json({ iceServers: ice });
});

app.get("/health", (req, res) => res.json({ ok: true, app: "NMX Messenger", version: "5.0.0" }));

/* ---------- SOCKET AUTH ---------- */
io.use((socket, next) => {
  const token = socket.handshake.auth && socket.handshake.auth.token;
  const username = token && db.sessions[token];
  if (!username || !db.users[username]) return next(new Error("unauthorized"));
  socket.data.username = username;
  next();
});

/* ---------- SOCKET ---------- */
io.on("connection", socket => {
  const username = socket.data.username;
  socket.join("u:" + username);
  online.set(username, (online.get(username) || 0) + 1);

  socket.emit("ready", {
    me: publicUser(db.users[username]),
    users: Object.values(db.users).map(publicUser).filter(Boolean),
    chats: userChats(username),
    online: [...online.keys()]
  });
  io.emit("presence", [...online.keys()]);
  broadcastUserList();

  /* --- messages --- */
  socket.on("loadMessages", chatId => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return;
    const list = db.messages.filter(m => m.chatId === chatId).slice(-300);
    socket.emit("chatMessages", { chatId, messages: list });
  });

  socket.on("sendMessage", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return;
    const text = String(p?.text || "").trim().slice(0, 4000);
    if (!text) return;
    const m = {
      id: uid(), chatId: chat.id, from: username, text,
      type: "text", time: Date.now(),
      replyTo: p?.replyTo || null,
      reactions: {}
    };
    db.messages.push(m);
    if (db.messages.length > 30000) db.messages = db.messages.slice(-30000);
    save();
    emitToChat(chat, "message", m);
    broadcastChatUpdate(chat);
  });

  socket.on("sendMedia", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return;
    const url = String(p?.url || "");
    if (!/^\/uploads\/[a-zA-Z0-9._-]+$/.test(url)) return;
    const type = ["image", "video", "audio", "file"].includes(p?.type) ? p.type : "file";
    const m = {
      id: uid(), chatId: chat.id, from: username,
      text: String(p?.caption || "").slice(0, 2000),
      type, url,
      name: String(p?.name || "file").slice(0, 200),
      size: Number(p?.size) || 0,
      mime: String(p?.mime || "").slice(0, 120),
      time: Date.now(),
      replyTo: p?.replyTo || null,
      reactions: {}
    };
    db.messages.push(m);
    if (db.messages.length > 30000) db.messages = db.messages.slice(-30000);
    save();
    emitToChat(chat, "message", m);
    broadcastChatUpdate(chat);
  });

  socket.on("editMessage", p => {
    const m = db.messages.find(x => x.id === p?.id);
    if (!m || m.from !== username) return;
    m.text = String(p?.text || "").trim().slice(0, 4000);
    m.edited = true;
    save();
    const chat = db.chats[m.chatId];
    if (chat) emitToChat(chat, "messageEdited", m);
  });

  socket.on("deleteMessage", id => {
    const m = db.messages.find(x => x.id === id);
    if (!m) return;
    const chat = db.chats[m.chatId];
    if (!chat) return;
    if (m.from !== username && chat.createdBy !== username) return;
    db.messages = db.messages.filter(x => x.id !== id);
    save();
    emitToChat(chat, "messageDeleted", { chatId: chat.id, id });
    broadcastChatUpdate(chat);
  });

  socket.on("reaction", p => {
    const m = db.messages.find(x => x.id === p?.messageId);
    if (!m) return;
    const chat = db.chats[m.chatId];
    if (!chat || !chat.members.includes(username)) return;
    const emoji = String(p?.emoji || "").slice(0, 8);
    if (!emoji) return;
    m.reactions ||= {};
    const list = m.reactions[emoji] || [];
    const i = list.indexOf(username);
    if (i >= 0) list.splice(i, 1); else list.push(username);
    if (list.length) m.reactions[emoji] = list; else delete m.reactions[emoji];
    save();
    emitToChat(chat, "messageReaction", { id: m.id, reactions: m.reactions });
  });

  socket.on("typing", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.members.forEach(m => {
      if (m !== username) io.to("u:" + m).emit("typing", { chatId: chat.id, from: username, active: !!p.active });
    });
  });

  socket.on("markRead", chatId => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.reads ||= {};
    chat.reads[username] = Date.now();
    save();
    chat.members.forEach(m => io.to("u:" + m).emit("chatRead", { chatId, username, time: chat.reads[username] }));
  });

  /* --- chats / groups --- */
  socket.on("createChat", p => {
    const members = Array.isArray(p?.members) ? p.members.map(clean).filter(Boolean) : [];
    const name = clean(p?.name).slice(0, 60);
    const uniq = [...new Set(members.filter(u => db.users[u] && u !== username))];
    if (!uniq.length) return socket.emit("errorMessage", "Выберите участников");

    let chat;
    if (uniq.length === 1 && !name) {
      const id = "p:" + [username, uniq[0]].sort().join("|");
      chat = db.chats[id];
      if (!chat) {
        chat = db.chats[id] = { id, type: "private", members: [username, uniq[0]], createdAt: Date.now(), reads: {}, createdBy: username };
      }
    } else {
      const id = "g:" + uid();
      chat = db.chats[id] = {
        id, type: "group",
        name: name || "Группа",
        avatar: "/bird.jpg",
        members: [username, ...uniq],
        createdAt: Date.now(),
        createdBy: username,
        reads: {}
      };
    }
    save();
    chat.members.forEach(m => io.to("u:" + m).emit("chatNew", chatSummary(chat, m)));
  });

  socket.on("addMembers", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || chat.type !== "group" || !chat.members.includes(username)) return;
    const add = (Array.isArray(p?.members) ? p.members : []).map(clean).filter(u => db.users[u] && !chat.members.includes(u));
    if (!add.length) return;
    chat.members.push(...add);
    save();
    chat.members.forEach(m => io.to("u:" + m).emit("chatNew", chatSummary(chat, m)));
    broadcastChatUpdate(chat);
  });

  socket.on("removeMember", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || chat.type !== "group") return;
    if (chat.createdBy !== username && p?.username !== username) return;
    chat.members = chat.members.filter(m => m !== p?.username);
    if (!chat.members.length) { delete db.chats[chat.id]; db.messages = db.messages.filter(m => m.chatId !== chat.id); save(); return; }
    save();
    io.to("u:" + p.username).emit("chatRemoved", chat.id);
    broadcastChatUpdate(chat);
  });

  socket.on("leaveChat", chatId => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.members = chat.members.filter(m => m !== username);
    if (!chat.members.length) { delete db.chats[chat.id]; db.messages = db.messages.filter(m => m.chatId !== chat.id); }
    save();
    socket.emit("chatRemoved", chatId);
    if (db.chats[chat.id]) broadcastChatUpdate(chat);
  });

  socket.on("updateGroup", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || chat.type !== "group" || !chat.members.includes(username)) return;
    if (typeof p?.name === "string" && p.name.trim()) chat.name = clean(p.name).slice(0, 60);
    if (typeof p?.avatar === "string" && p.avatar.length < 800000) chat.avatar = p.avatar;
    save();
    broadcastChatUpdate(chat);
  });

  /* --- profile --- */
  socket.on("profileUpdate", p => {
    const u = db.users[username];
    if (!u) return;
    if (typeof p?.displayName === "string") u.displayName = clean(p.displayName).slice(0, 40) || u.username;
    if (typeof p?.bio === "string") u.bio = String(p.bio).slice(0, 200);
    if (typeof p?.avatar === "string" && p.avatar.length < 800000) u.avatar = p.avatar;
    save();
    broadcastUserList();
    socket.emit("profileUpdated", publicUser(u));
    Object.values(db.chats).filter(c => c.members.includes(username)).forEach(broadcastChatUpdate);
  });

  /* --- WebRTC signaling --- */
  const relay = ev => socket.on(ev, p => {
    const to = clean(p?.to);
    if (!to || !db.users[to]) return;
    io.to("u:" + to).emit(ev, Object.assign({}, p, { from: username }));
  });
  ["call:invite", "call:signal", "call:end", "call:busy", "call:accept", "call:decline"].forEach(relay);

  socket.on("disconnect", () => {
    const n = (online.get(username) || 1) - 1;
    if (n <= 0) online.delete(username); else online.set(username, n);
    io.emit("presence", [...online.keys()]);
    if (n <= 0) broadcastUserList();
  });
});

app.get("*", (req, res) => res.sendFile(path.join(PUBLIC, "index.html")));
server.listen(PORT, () => console.log("NMX Messenger 5.0 running on " + PORT));
