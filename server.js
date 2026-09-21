const express=require("express");
const http=require("http");
const {Server}=require("socket.io");
const fs=require("fs"),path=require("path");
const app=express(),server=http.createServer(app),io=new Server(server);
const PORT=process.env.PORT||3000,DATA=path.join(__dirname,"data.json");
app.use(express.static(path.join(__dirname,"public")));
let db={users:{},messages:[]};
try{if(fs.existsSync(DATA))db=JSON.parse(fs.readFileSync(DATA,"utf8"))}catch{}
const online=new Map();
const clean=s=>String(s||"").trim().replace(/\s+/g," ").slice(0,24);
const uid=()=>Math.random().toString(36).slice(2)+Date.now().toString(36);
const save=()=>{try{fs.writeFileSync(DATA,JSON.stringify(db))}catch{}};
const pub=u=>({username:u.username,avatar:"/bird.jpg",createdAt:u.createdAt});
io.on("connection",socket=>{
 socket.on("login",raw=>{
  const username=clean(raw); if(!username)return socket.emit("errorMessage","Введите имя");
  if(!db.users[username]){db.users[username]={username,createdAt:Date.now()};save()}
  socket.data.username=username; socket.join("u:"+username);
  online.set(username,(online.get(username)||0)+1);
  socket.emit("init",{me:pub(db.users[username]),users:Object.values(db.users).map(pub),messages:db.messages.slice(-1000),online:[...online.keys()]});
  io.emit("presence",[...online.keys()]); io.emit("userList",Object.values(db.users).map(pub));
 });
 socket.on("privateMessage",p=>{
  const from=socket.data.username,to=clean(p?.to),text=String(p?.text||"").trim().slice(0,2000);
  if(!from||!to||!text||!db.users[to])return;
  const m={id:uid(),from,to,text,time:Date.now(),replyTo:p.replyTo||null};
  db.messages.push(m);db.messages=db.messages.slice(-5000);save();
  io.to("u:"+from).to("u:"+to).emit("privateMessage",m);
 });
 socket.on("typing",p=>{if(socket.data.username&&p?.to)io.to("u:"+clean(p.to)).emit("typing",{from:socket.data.username,active:!!p.active})});
 socket.on("deleteMessage",id=>{const m=db.messages.find(x=>x.id===id);if(!m||m.from!==socket.data.username)return;db.messages=db.messages.filter(x=>x.id!==id);save();io.emit("messageDeleted",id)});
 socket.on("disconnect",()=>{const u=socket.data.username;if(!u)return;const n=(online.get(u)||1)-1;if(n<=0)online.delete(u);else online.set(u,n);io.emit("presence",[...online.keys()])});
});
app.get("*",(req,res)=>res.sendFile(path.join(__dirname,"public","index.html")));
server.listen(PORT,()=>console.log("NMX Messenger 2.0 running on "+PORT));
