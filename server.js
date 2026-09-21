const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

const users = new Map();

io.on("connection", (socket) => {
  socket.on("join", (username) => {
    username = String(username || "Аноним").trim().slice(0, 30) || "Аноним";
    users.set(socket.id, username);
    io.emit("system", `${username} вошёл в NMX Messenger`);
    io.emit("online", Array.from(users.values()));
  });

  socket.on("message", (text) => {
    const username = users.get(socket.id) || "Аноним";
    text = String(text || "").trim().slice(0, 1000);
    if (!text) return;
    io.emit("message", { username, text, time: new Date().toLocaleTimeString([], {hour:"2-digit", minute:"2-digit"}) });
  });

  socket.on("disconnect", () => {
    const username = users.get(socket.id);
    if (username) {
      users.delete(socket.id);
      io.emit("system", `${username} вышёл из NMX Messenger`);
      io.emit("online", Array.from(users.values()));
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`NMX Messenger запущен: http://localhost:${PORT}`);
});
