const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6, pingTimeout: 30000 });
const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, "public");
const DATA = path.join(ROOT, "data.json");
const UPLOADS = path.join(PUBLIC, "uploads");
fs.mkdirSync(UPLOADS, { recursive: true });

/* ================= SECURITY HEADERS ================= */
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("Permissions-Policy", "geolocation=(), microphone=(self), camera=(self)");
  next();
});

/* ================= RATE LIMITING ================= */
const buckets = new Map();
function rateLimit(key, max, windowMs) {
  const now = Date.now();
  let b = buckets.get(key);
  if (!b || now - b.start > windowMs) { b = { start: now, count: 0 }; buckets.set(key, b); }
  b.count++;
  return b.count <= max;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, b] of buckets) if (now - b.start > 120000) buckets.delete(k);
}, 60000).unref();

/* ================= DB ================= */
let db = { users: {}, chats: {}, messages: [], sessions: {}, blocks: {} };
try { if (fs.existsSync(DATA)) db = JSON.parse(fs.readFileSync(DATA, "utf8")); }
catch (e) { console.error("[db] load error:", e.message); }
db.users ||= {}; db.chats ||= {}; db.messages ||= []; db.sessions ||= {}; db.blocks ||= {};

const messagesByChat = new Map();
function rebuildIndex() {
  messagesByChat.clear();
  for (const m of db.messages) {
    if (!messagesByChat.has(m.chatId)) messagesByChat.set(m.chatId, []);
    messagesByChat.get(m.chatId).push(m);
  }
}
rebuildIndex();

const online = new Map();
const clean = s => String(s == null ? "" : s).trim().replace(/\s+/g, " ");
const uid = () => crypto.randomBytes(12).toString("hex");

let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      const tmp = DATA + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(db));
      fs.renameSync(tmp, DATA);
    } catch (e) { console.error("[db] save error:", e.message); }
  }, 400);
}
function saveNow() {
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  try {
    const tmp = DATA + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DATA);
  } catch (e) { console.error("[db] save error:", e.message); }
}

function publicUser(u) {
  if (!u) return null;
  return {
    username: u.username,
    displayName: u.displayName || u.username,
    bio: u.bio || "",
    avatar: u.avatar || "/bird.jpg",
    createdAt: u.createdAt,
    lastSeen: u.lastSeen || null,
    online: online.has(u.username)
  };
}
function lastMessage(chatId) {
  const arr = messagesByChat.get(chatId);
  return arr && arr.length ? arr[arr.length - 1] : null;
}
function unreadCount(chat, username) {
  const lastRead = (chat.reads && chat.reads[username]) || 0;
  const arr = messagesByChat.get(chat.id) || [];
  let n = 0;
  for (let i = arr.length - 1; i >= 0; i--) {
    const m = arr[i];
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
  if (m.type === "poll") return "📊 " + (m.question || "Опрос");
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
    id: chat.id, type: chat.type,
    name: name || "Группа",
    avatar: avatar || "/bird.jpg",
    members: chat.members.map(m => publicUser(db.users[m])).filter(Boolean),
    createdBy: chat.createdBy || null,
    createdAt: chat.createdAt,
    pinned: chat.pinned || [],
    reads: chat.reads || {},
    expiresIn: chat.expiresIn || 0,
    lastMessage: last ? { id: last.id, from: last.from, text: previewOf(last), time: last.time, type: last.type } : null,
    unread: unreadCount(chat, forUser)
  };
}
const userChats = username => Object.values(db.chats).filter(c => c.members.includes(username)).map(c => chatSummary(c, username));
const emitToChat = (chat, event, payload) => chat.members.forEach(m => io.to("u:" + m).emit(event, payload));
const broadcastChatUpdate = chat => chat.members.forEach(m => io.to("u:" + m).emit("chatUpdate", chatSummary(chat, m)));
const broadcastUserList = () => io.emit("userList", Object.values(db.users).map(publicUser).filter(Boolean));

function makeSession(username) {
  const token = crypto.randomBytes(24).toString("hex");
  db.sessions[token] = { username, createdAt: Date.now(), expiresAt: Date.now() + 90 * 24 * 3600 * 1000 };
  save();
  return token;
}
function getSession(token) {
  const s = db.sessions[token];
  if (!s) return null;
  if (s.expiresAt && s.expiresAt < Date.now()) { delete db.sessions[token]; save(); return null; }
  return typeof s === "string" ? { username: s } : s;
}

/* ================= UPLOAD ================= */
app.post("/api/upload", (req, res) => {
  const ip = req.ip || "?";
  if (!rateLimit("upload:" + ip, 30, 60000)) return res.status(429).json({ error: "Слишком часто" });
  const token = String(req.query.token || "");
  const sess = getSession(token);
  const username = sess && sess.username;
  if (!username || !db.users[username]) return res.status(401).json({ error: "auth" });
  const rawName = decodeURIComponent(String(req.headers["x-file-name"] || "file"));
  const safeName = rawName.replace(/[\\/:*?"<>|]/g, "_").slice(0, 200) || "file";
  let ext = (path.extname(safeName) || "").toLowerCase().slice(0, 12);
  const mime = String(req.headers["content-type"] || "application/octet-stream");
  if (!ext) {
    if (mime.startsWith("image/")) ext = "." + mime.split("/")[1].split(";")[0];
    else if (mime.startsWith("audio/")) ext = mime.includes("mp4") ? ".m4a" : mime.includes("ogg") ? ".ogg" : ".webm";
    else if (mime.startsWith("video/")) ext = "." + mime.split("/")[1].split(";")[0];
    else ext = ".bin";
  }
  const fileName = crypto.randomBytes(10).toString("hex") + ext;
  const dest = path.join(UPLOADS, fileName);
  const ws = fs.createWriteStream(dest);
  let size = 0, aborted = false;
  req.on("data", c => {
    size += c.length;
    if (size > 4e6) { aborted = true; ws.destroy(); try { fs.unlinkSync(dest); } catch {} req.destroy(); }
  });
  req.on("aborted", () => { aborted = true; ws.destroy(); try { fs.unlinkSync(dest); } catch {} });
  req.on("error", () => { aborted = true; ws.destroy(); });
  ws.on("error", () => { if (!aborted) res.status(500).json({ error: "write" }); });
  ws.on("finish", () => {
    if (aborted) return;
    res.json({ url: "/uploads/" + fileName, name: safeName, size, type: mime });
  });
  req.pipe(ws);
});

app.use(express.json({ limit: "4mb" }));
app.use(express.static(PUBLIC));

/* ================= LOGIN (только username) ================= */
app.post("/api/login", (req, res) => {
  const ip = req.ip || "?";
  if (!rateLimit("login:" + ip, 30, 60000)) return res.status(429).json({ error: "Слишком часто, подожди минуту" });
  const username = clean(req.body?.username).toLowerCase().replace(/[^a-z0-9_.-]/g, "");
  if (username.length < 3) return res.status(400).json({ error: "Username минимум 3 символа (a-z, 0-9, _ . -)" });
  if (username.length > 24) return res.status(400).json({ error: "Username максимум 24 символа" });
  let u = db.users[username];
  let isNew = false;
  if (!u) {
    u = db.users[username] = {
      username,
      displayName: username,
      bio: "",
      avatar: "/bird.jpg",
      createdAt: Date.now(),
      lastSeen: null
    };
    isNew = true;
  }
  const token = makeSession(username);
  if (isNew) { saveNow(); broadcastUserList(); }
  res.json({ token, user: publicUser(u), isNew });
});

app.post("/api/logout", (req, res) => {
  const token = String(req.body?.token || "");
  if (token && db.sessions[token]) { delete db.sessions[token]; save(); }
  res.json({ ok: true });
});

app.delete("/api/account", (req, res) => {
  const sess = getSession(String(req.body?.token || ""));
  if (!sess) return res.status(401).json({ error: "auth" });
  const username = sess.username;
  delete db.users[username];
  for (const token in db.sessions) if (db.sessions[token].username === username) delete db.sessions[token];
  for (const id in db.chats) {
    const c = db.chats[id];
    c.members = c.members.filter(m => m !== username);
    if (!c.members.length) { delete db.chats[id]; messagesByChat.delete(id); }
  }
  db.messages = db.messages.filter(m => m.from !== username);
  rebuildIndex();
  db.blocks[username] = [];
  for (const k in db.blocks) db.blocks[k] = (db.blocks[k] || []).filter(x => x !== username);
  saveNow(); broadcastUserList();
  res.json({ ok: true });
});

app.get("/api/rtc-config", (req, res) => {
  const ice = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ];
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL)
    ice.push({ urls: process.env.TURN_URL, username: process.env.TURN_USERNAME, credential: process.env.TURN_CREDENTIAL });
  res.json({ iceServers: ice });
});

app.get("/health", (req, res) => res.json({ ok: true, app: "NMX Messenger", version: "6.0.0" }));

/* ================= SOCKET AUTH ================= */
io.use((socket, next) => {
  const token = socket.handshake.auth && socket.handshake.auth.token;
  const sess = token && getSession(token);
  if (!sess || !db.users[sess.username]) return next(new Error("unauthorized"));
  socket.data.username = sess.username;
  next();
});

/* ================= SCHEDULED / DISAPPEARING ================= */
function deliverMessage(m) {
  db.messages.push(m);
  if (!messagesByChat.has(m.chatId)) messagesByChat.set(m.chatId, []);
  messagesByChat.get(m.chatId).push(m);
  const chat = db.chats[m.chatId];
  if (chat) { emitToChat(chat, "message", m); broadcastChatUpdate(chat); }
}
setInterval(() => {
  const now = Date.now();
  let changed = false;
  const toDelete = [];
  for (const m of db.messages) {
    if (m.expiresAt && m.expiresAt < now) toDelete.push(m);
    if (!m.expiresAt && m.time) {
      const chat = db.chats[m.chatId];
      if (chat && chat.expiresIn > 0 && now - m.time > chat.expiresIn) {
        m.expiresAt = m.time + chat.expiresIn;
      }
    }
  }
  for (const m of toDelete) {
    const chat = db.chats[m.chatId];
    db.messages = db.messages.filter(x => x.id !== m.id);
    const arr = messagesByChat.get(m.chatId);
    if (arr) { const i = arr.findIndex(x => x.id === m.id); if (i >= 0) arr.splice(i, 1); }
    if (chat) emitToChat(chat, "messageDeleted", { chatId: m.chatId, id: m.id, reason: "expired" });
    changed = true;
  }
  const scheduled = db.messages.filter(m => m.scheduledAt && m.scheduledAt <= now && !m._sent);
  for (const m of scheduled) {
    m._sent = true;
    m.time = now;
    delete m.scheduledAt;
    const chat = db.chats[m.chatId];
    if (chat) { emitToChat(chat, "message", m); broadcastChatUpdate(chat); changed = true; }
  }
  if (changed) save();
}, 5000).unref();

/* ================= SOCKET ================= */
io.on("connection", socket => {
  const username = socket.data.username;
  socket.join("u:" + username);
  online.set(username, (online.get(username) || 0) + 1);
  if (db.users[username]) db.users[username].lastSeen = null;

  socket.emit("ready", {
    me: publicUser(db.users[username]),
    users: Object.values(db.users).map(publicUser).filter(Boolean),
    chats: userChats(username),
    online: [...online.keys()],
    blocked: db.blocks[username] || []
  });
  io.emit("presence", [...online.keys()]);
  broadcastUserList();

  socket.on("loadMessages", (p, cb) => {
    const chatId = typeof p === "string" ? p : p?.chatId;
    const before = typeof p === "object" ? p.before : null;
    const limit = typeof p === "object" && p.limit ? Math.min(p.limit, 200) : 100;
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return typeof cb === "function" && cb({ error: "no" });
    const arr = messagesByChat.get(chatId) || [];
    let slice;
    if (before) {
      const i = arr.findIndex(m => m.id === before);
      slice = i > 0 ? arr.slice(Math.max(0, i - limit), i) : [];
    } else {
      slice = arr.slice(-limit);
    }
    const hasMore = arr.length > (before ? (arr.findIndex(m => m.id === before) || 0) : slice.length);
    if (typeof cb === "function") cb({ chatId, messages: slice, hasMore });
    else socket.emit("chatMessages", { chatId, messages: slice, hasMore });
  });

  socket.on("searchMessages", ({ chatId, q }, cb) => {
    if (!rateLimit("search:" + username, 60, 60000)) return typeof cb === "function" && cb({ error: "Слишком часто" });
    const arr = messagesByChat.get(chatId) || [];
    const query = String(q || "").toLowerCase().slice(0, 100);
    if (!query) return typeof cb === "function" && cb({ results: [] });
    const results = arr.filter(m => (m.text || "").toLowerCase().includes(query)).slice(-50);
    if (typeof cb === "function") cb({ results });
  });

  socket.on("sendMessage", (p, cb) => {
    if (!rateLimit("msg:" + username, 30, 10000)) return typeof cb === "function" && cb({ error: "Слишком часто" });
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return typeof cb === "function" && cb({ error: "no" });
    const text = String(p?.text || "").trim().slice(0, 4000);
    if (!text) return typeof cb === "function" && cb({ error: "empty" });
    const m = {
      id: uid(), chatId: chat.id, from: username, text, type: "text", time: Date.now(),
      replyTo: p?.replyTo || null, forwarded: p?.forwarded || false, reactions: {}
    };
    if (p?.scheduledAt && p.scheduledAt > Date.now() + 2000) {
      m.scheduledAt = p.scheduledAt; m._sent = false;
      db.messages.push(m);
      if (!messagesByChat.has(chat.id)) messagesByChat.set(chat.id, []);
      messagesByChat.get(chat.id).push(m);
      save();
      return typeof cb === "function" && cb({ ok: true, scheduled: true });
    }
    deliverMessage(m);
    save();
    typeof cb === "function" && cb({ ok: true, message: m });
  });

  socket.on("sendMedia", (p, cb) => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return typeof cb === "function" && cb({ error: "no" });
    const url = String(p?.url || "");
    if (!/^\/uploads\/[a-zA-Z0-9._-]+$/.test(url)) return typeof cb === "function" && cb({ error: "url" });
    const type = ["image", "video", "audio", "file"].includes(p?.type) ? p.type : "file";
    const m = {
      id: uid(), chatId: chat.id, from: username,
      text: String(p?.caption || "").slice(0, 2000), type, url,
      name: String(p?.name || "file").slice(0, 200),
      size: Number(p?.size) || 0, mime: String(p?.mime || "").slice(0, 120),
      thumb: typeof p?.thumb === "string" && p.thumb.length < 200000 ? p.thumb : null,
      time: Date.now(), replyTo: p?.replyTo || null, forwarded: p?.forwarded || false, reactions: {}
    };
    deliverMessage(m);
    save();
    typeof cb === "function" && cb({ ok: true, message: m });
  });

  socket.on("sendPoll", (p, cb) => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return typeof cb === "function" && cb({ error: "no" });
    const question = String(p?.question || "").trim().slice(0, 300);
    const options = (Array.isArray(p?.options) ? p.options : []).map(o => String(o).trim().slice(0, 100)).filter(Boolean).slice(0, 10);
    if (!question || options.length < 2) return typeof cb === "function" && cb({ error: "Нужен вопрос и минимум 2 варианта" });
    const m = {
      id: uid(), chatId: chat.id, from: username, type: "poll",
      question, options, votes: options.map(() => []),
      multiple: !!p?.multiple,
      time: Date.now(), replyTo: null, forwarded: false, reactions: {}
    };
    deliverMessage(m);
    save();
    typeof cb === "function" && cb({ ok: true, message: m });
  });

  socket.on("vote", ({ messageId, optionIndex }, cb) => {
    const m = db.messages.find(x => x.id === messageId);
    if (!m || m.type !== "poll") return;
    const chat = db.chats[m.chatId];
    if (!chat || !chat.members.includes(username)) return;
    const i = Number(optionIndex);
    if (!m.votes || !m.votes[i]) return;
    if (m.multiple) {
      const list = m.votes[i];
      const idx = list.indexOf(username);
      if (idx >= 0) list.splice(idx, 1); else list.push(username);
    } else {
      m.votes = m.votes.map((list, j) => j === i ? (list.includes(username) ? [] : [username]) : list.filter(u => u !== username));
    }
    save();
    emitToChat(chat, "messagePoll", { id: m.id, chatId: chat.id, votes: m.votes });
    typeof cb === "function" && cb({ ok: true });
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
    const arr = messagesByChat.get(m.chatId);
    if (arr) { const i = arr.findIndex(x => x.id === id); if (i >= 0) arr.splice(i, 1); }
    if (chat.pinned) chat.pinned = chat.pinned.filter(x => x !== id);
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
    emitToChat(chat, "messageReaction", { id: m.id, chatId: chat.id, reactions: m.reactions });
  });

  socket.on("pinMessage", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.pinned ||= [];
    const i = chat.pinned.indexOf(p?.messageId);
    if (i >= 0) chat.pinned.splice(i, 1);
    else { chat.pinned.push(p.messageId); if (chat.pinned.length > 5) chat.pinned.shift(); }
    save();
    broadcastChatUpdate(chat);
  });

  socket.on("typing", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.members.forEach(m => { if (m !== username) io.to("u:" + m).emit("typing", { chatId: chat.id, from: username, active: !!p.active }); });
  });

  socket.on("markRead", chatId => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.reads ||= {};
    chat.reads[username] = Date.now();
    save();
    chat.members.forEach(m => io.to("u:" + m).emit("chatRead", { chatId, username, time: chat.reads[username] }));
  });

  socket.on("setDisappear", ({ chatId, ms }) => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.expiresIn = Math.max(0, Number(ms) || 0);
    save();
    broadcastChatUpdate(chat);
    emitToChat(chat, "disappearChanged", { chatId, ms: chat.expiresIn });
  });

  socket.on("createChat", p => {
    const members = Array.isArray(p?.members) ? p.members.map(clean).filter(Boolean) : [];
    const name = clean(p?.name).slice(0, 60);
    const uniq = [...new Set(members.filter(u => db.users[u] && u !== username))];
    if (!uniq.length) return socket.emit("errorMessage", "Выберите участников");
    let chat;
    if (uniq.length === 1 && !name) {
      const id = "p:" + [username, uniq[0]].sort().join("|");
      chat = db.chats[id];
      if (!chat) chat = db.chats[id] = { id, type: "private", members: [username, uniq[0]], createdAt: Date.now(), reads: {}, createdBy: username, pinned: [] };
    } else {
      const id = "g:" + uid();
      chat = db.chats[id] = {
        id, type: "group", name: name || "Группа", avatar: "/bird.jpg",
        members: [username, ...uniq], createdAt: Date.now(), createdBy: username, reads: {}, pinned: []
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
    if (!chat.members.length) { delete db.chats[chat.id]; messagesByChat.delete(chat.id); db.messages = db.messages.filter(m => m.chatId !== chat.id); save(); return; }
    save();
    io.to("u:" + p.username).emit("chatRemoved", chat.id);
    broadcastChatUpdate(chat);
  });

  socket.on("leaveChat", chatId => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return;
    chat.members = chat.members.filter(m => m !== username);
    if (!chat.members.length) { delete db.chats[chat.id]; messagesByChat.delete(chat.id); db.messages = db.messages.filter(m => m.chatId !== chat.id); }
    save();
    socket.emit("chatRemoved", chatId);
    if (db.chats[chat.id]) broadcastChatUpdate(chat);
  });

  socket.on("updateGroup", p => {
    const chat = db.chats[p?.chatId];
    if (!chat || chat.type !== "group" || !chat.members.includes(username)) return;
    if (typeof p?.name === "string" && p.name.trim()) chat.name = clean(p.name).slice(0, 60);
    if (typeof p?.avatar === "string" && p.avatar.length < 2_500_000) chat.avatar = p.avatar;
    save(); broadcastChatUpdate(chat);
  });

  socket.on("profileUpdate", p => {
    const u = db.users[username];
    if (!u) return;
    if (typeof p?.displayName === "string") u.displayName = clean(p.displayName).slice(0, 40) || u.username;
    if (typeof p?.bio === "string") u.bio = String(p.bio).slice(0, 200);
    if (typeof p?.avatar === "string" && p.avatar.length < 2_500_000) u.avatar = p.avatar;
    save(); broadcastUserList();
    socket.emit("profileUpdated", publicUser(u));
    Object.values(db.chats).filter(c => c.members.includes(username)).forEach(broadcastChatUpdate);
  });

  socket.on("blockUser", ({ username: target, block }, cb) => {
    const t = clean(target);
    if (!db.users[t] || t === username) return typeof cb === "function" && cb({ error: "no" });
    db.blocks[username] ||= [];
    const i = db.blocks[username].indexOf(t);
    if (block && i < 0) db.blocks[username].push(t);
    if (!block && i >= 0) db.blocks[username].splice(i, 1);
    save();
    socket.emit("blockList", db.blocks[username]);
    typeof cb === "function" && cb({ ok: true, list: db.blocks[username] });
  });

  socket.on("exportChat", ({ chatId }, cb) => {
    const chat = db.chats[chatId];
    if (!chat || !chat.members.includes(username)) return typeof cb === "function" && cb({ error: "no" });
    const arr = messagesByChat.get(chatId) || [];
    typeof cb === "function" && cb({
      chat: { id: chat.id, name: chat.name, type: chat.type, members: chat.members },
      messages: arr,
      exportedAt: Date.now()
    });
  });

  const relay = ev => socket.on(ev, p => {
    const to = clean(p?.to);
    if (!to || !db.users[to]) return;
    if ((db.blocks[to] || []).includes(username)) return;
    io.to("u:" + to).emit(ev, Object.assign({}, p, { from: username }));
  });
  ["call:invite", "call:signal", "call:end", "call:busy", "call:accept", "call:decline"].forEach(relay);

  socket.on("disconnect", () => {
    const n = (online.get(username) || 1) - 1;
    if (n <= 0) {
      online.delete(username);
      if (db.users[username]) db.users[username].lastSeen = Date.now();
      save();
    } else online.set(username, n);
    io.emit("presence", [...online.keys()]);
    if (n <= 0) broadcastUserList();
  });
});

app.get("*", (req, res) => res.sendFile(path.join(PUBLIC, "index.html")));

server.listen(PORT, () => console.log("NMX Messenger 6.0 running on " + PORT));

process.on("SIGTERM", () => { console.log("SIGTERM"); saveNow(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000); });
process.on("SIGINT", () => { console.log("SIGINT"); saveNow(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000); });
process.on("uncaughtException", e => { console.error("uncaught:", e); saveNow(); });
process.on("unhandledRejection", e => { console.error("unhandled:", e); });
