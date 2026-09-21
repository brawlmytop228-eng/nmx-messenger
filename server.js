const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const fs = require("fs");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 8e6 });
const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const PUBLIC = path.join(ROOT, "public");
const DATA = path.join(ROOT, "data.json");
const UPLOADS = path.join(PUBLIC, "uploads");
fs.mkdirSync(UPLOADS, { recursive: true });

app.use(express.json({limit:"12mb"}));
app.use(express.urlencoded({extended:true, limit:"12mb"}));
app.use(express.static(PUBLIC));

let db = { users:{}, messages:[], groups:[], stories:[] };
try { if(fs.existsSync(DATA)) db = JSON.parse(fs.readFileSync(DATA,"utf8")); } catch {}
db.users ||= {}; db.messages ||= []; db.groups ||= []; db.stories ||= [];

const online = new Map();
const clean = s => String(s||"").trim().replace(/\s+/g," ").slice(0,32);
const uid = () => Math.random().toString(36).slice(2)+Date.now().toString(36);
const save = () => { try { fs.writeFileSync(DATA, JSON.stringify(db)); } catch {} };
const publicUser = u => ({username:u.username, bio:u.bio||"", avatar:u.avatar||"/bird.jpg", createdAt:u.createdAt});


app.get("/api/rtc-config",(req,res)=>{
 const ice=[{urls:"stun:stun.l.google.com:19302"}];
 if(process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL){ice.push({urls:process.env.TURN_URL,username:process.env.TURN_USERNAME,credential:process.env.TURN_CREDENTIAL});}
 res.json({iceServers:ice});
});
app.get("/health",(req,res)=>res.json({ok:true,app:"NMX Messenger",version:"4.0.0"}));

io.on("connection", socket => {
  socket.on("login", raw => {
    const username = clean(raw);
    if(!username) return socket.emit("errorMessage","Введите имя");
    if(!db.users[username]) {
      db.users[username]={username,bio:"",avatar:"/bird.jpg",createdAt:Date.now()};
      save();
    }
    socket.data.username=username;
    socket.join("u:"+username);
    online.set(username,(online.get(username)||0)+1);
    socket.emit("init",{
      me:publicUser(db.users[username]),
      users:Object.values(db.users).map(publicUser),
      messages:db.messages.slice(-1500),
      online:[...online.keys()]
    });
    io.emit("presence",[...online.keys()]);
    io.emit("userList",Object.values(db.users).map(publicUser));
  });

  socket.on("profileUpdate", p => {
    const u=db.users[socket.data.username]; if(!u)return;
    u.bio=clean(p?.bio).slice(0,160);
    if(typeof p?.avatar==="string" && p.avatar.length<700000) u.avatar=p.avatar;
    save();
    io.emit("userList",Object.values(db.users).map(publicUser));
    socket.emit("profileUpdated",publicUser(u));
  });

  socket.on("privateMessage", p => {
    const from=socket.data.username, to=clean(p?.to);
    const text=String(p?.text||"").trim().slice(0,4000);
    if(!from||!to||!text||!db.users[to])return;
    const m={id:uid(),from,to,text,time:Date.now(),replyTo:p.replyTo||null,type:"text"};
    db.messages.push(m); db.messages=db.messages.slice(-10000); save();
    io.to("u:"+from).to("u:"+to).emit("privateMessage",m);
  });

  socket.on("mediaMessage", p => {
    const from=socket.data.username,to=clean(p?.to);
    if(!from||!to||!db.users[to]||!p?.data)return;
    const raw=String(p.data);
    if(raw.length>7_000_000)return;
    const m={id:uid(),from,to,text:p.caption||"",time:Date.now(),replyTo:p.replyTo||null,type:p.type||"file",data:raw,name:String(p.name||"file").slice(0,120)};
    db.messages.push(m); db.messages=db.messages.slice(-10000); save();
    io.to("u:"+from).to("u:"+to).emit("privateMessage",m);
  });

  socket.on("reaction", p => {
    io.to("u:"+clean(p?.to)).to("u:"+socket.data.username).emit("reaction",{messageId:p?.messageId,emoji:p?.emoji,from:socket.data.username});
  });

  socket.on("typing",p=>{
    const from=socket.data.username,to=clean(p?.to);
    if(from&&to)io.to("u:"+to).emit("typing",{from,active:!!p.active});
  });

  socket.on("deleteMessage",mid=>{
    const m=db.messages.find(x=>x.id===mid);
    if(!m||m.from!==socket.data.username)return;
    db.messages=db.messages.filter(x=>x.id!==mid);save();io.emit("messageDeleted",mid);
  });

  socket.on("editMessage",p=>{
    const m=db.messages.find(x=>x.id===p?.id);
    if(!m||m.from!==socket.data.username)return;
    m.text=String(p.text||"").trim().slice(0,4000);m.edited=true;save();
    io.to("u:"+m.from).to("u:"+m.to).emit("messageEdited",m);
  });


  socket.on("call:invite", p => {
    const from=socket.data.username,to=clean(p?.to);
    if(from&&to) io.to("u:"+to).emit("call:invite",{from,kind:p.kind==="video"?"video":"audio"});
  });
  socket.on("call:signal", p => {
    const from=socket.data.username,to=clean(p?.to);
    if(from&&to&&p?.data) io.to("u:"+to).emit("call:signal",{from,data:p.data});
  });
  socket.on("call:end", p => {
    const from=socket.data.username,to=clean(p?.to);
    if(from&&to) io.to("u:"+to).emit("call:end",{from});
  });
  socket.on("call:busy", p => {
    const from=socket.data.username,to=clean(p?.to);
    if(from&&to) io.to("u:"+to).emit("call:busy",{from});
  });

  socket.on("disconnect",()=>{
    const u=socket.data.username;if(!u)return;
    const n=(online.get(u)||1)-1;
    if(n<=0)online.delete(u);else online.set(u,n);
    io.emit("presence",[...online.keys()]);
  });
});

app.get("*",(req,res)=>res.sendFile(path.join(PUBLIC,"index.html")));
server.listen(PORT,()=>console.log("NMX Messenger 4.0 running on "+PORT));
