"""
dashboard.py
─────────────────────────────────────────────────────────────────────────────
Self-contained HTML dashboard for the order-flow scalper, served at
``/api/v1/scalper/dashboard``. Vanilla HTML/CSS/JS — no build step — it opens
the ``/api/v1/scalper/ws`` websocket and renders per-symbol checklists, walls,
metrics and a live signal log.
"""

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Order-Flow Scalper</title>
<style>
  :root {
    --bg:#0b0e14; --panel:#141925; --panel2:#1b2130; --border:#232b3d;
    --text:#e6e9f0; --muted:#8a93a6; --green:#16c784; --red:#ea3943;
    --amber:#f0b90b; --blue:#4b9fff;
  }
  * { box-sizing:border-box; }
  body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; background:var(--bg); color:var(--text); }
  header { display:flex; align-items:center; justify-content:space-between; padding:16px 24px; border-bottom:1px solid var(--border); background:var(--panel); position:sticky; top:0; z-index:10; }
  header h1 { font-size:18px; margin:0; font-weight:600; }
  header h1 span { color:var(--blue); }
  .status { display:flex; gap:18px; align-items:center; font-size:13px; color:var(--muted); }
  .dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:6px; }
  .dot.on { background:var(--green); box-shadow:0 0 8px var(--green); }
  .dot.off { background:var(--red); }
  main { padding:24px; max-width:1400px; margin:0 auto; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(380px,1fr)); gap:18px; }
  .card { background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:18px; transition:border-color .2s; }
  .card.BUY { border-color:var(--green); box-shadow:0 0 0 1px var(--green) inset; }
  .card.SELL { border-color:var(--red); box-shadow:0 0 0 1px var(--red) inset; }
  .card-head { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:4px; }
  .sym { font-size:20px; font-weight:700; }
  .price { font-size:18px; font-variant-numeric:tabular-nums; }
  .badge { display:inline-block; padding:4px 12px; border-radius:20px; font-weight:700; font-size:13px; letter-spacing:.5px; }
  .badge.BUY { background:rgba(22,199,132,.15); color:var(--green); }
  .badge.SELL { background:rgba(234,57,67,.15); color:var(--red); }
  .badge.HOLD { background:rgba(138,147,166,.15); color:var(--muted); }
  .section-title { font-size:11px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin:14px 0 8px; }
  ul.checklist { list-style:none; padding:0; margin:0; }
  ul.checklist li { display:flex; align-items:center; gap:8px; padding:4px 0; font-size:13px; }
  .check { width:16px; text-align:center; }
  li.met { color:var(--text); }
  li.unmet { color:var(--muted); }
  .metrics { display:grid; grid-template-columns:1fr 1fr; gap:6px 14px; margin-top:12px; font-size:12px; }
  .metrics div { display:flex; justify-content:space-between; }
  .metrics .k { color:var(--muted); }
  .metrics .v { font-variant-numeric:tabular-nums; }
  .pos { color:var(--green); } .neg { color:var(--red); }
  .walls { margin-top:10px; font-size:12px; display:flex; gap:10px; flex-wrap:wrap; }
  .wall-tag { padding:3px 8px; border-radius:6px; background:var(--panel2); border:1px solid var(--border); }
  .wall-tag.buy { border-color:rgba(22,199,132,.5); }
  .wall-tag.sell { border-color:rgba(234,57,67,.5); }
  .progress { height:6px; background:var(--panel2); border-radius:4px; overflow:hidden; margin-top:6px; }
  .progress > div { height:100%; }
  .progress > div.buy { background:var(--green); }
  .progress > div.sell { background:var(--red); }
  .sidebar { margin-top:28px; }
  table.log { width:100%; border-collapse:collapse; font-size:13px; }
  table.log th, table.log td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }
  table.log th { color:var(--muted); font-weight:500; font-size:11px; text-transform:uppercase; letter-spacing:1px; }
  table.log td.sig-BUY { color:var(--green); font-weight:700; }
  table.log td.sig-SELL { color:var(--red); font-weight:700; }
  .muted { color:var(--muted); }
</style>
</head>
<body>
<header>
  <h1>⚡ Order-Flow <span>Scalper</span></h1>
  <div class="status">
    <span><span id="dot" class="dot off"></span><span id="conn">Disconnected</span></span>
    <span id="uptime">uptime —</span>
    <span id="cfg"></span>
  </div>
</header>
<main>
  <div id="grid" class="grid"></div>
  <div class="sidebar">
    <div class="section-title">Recent Signals</div>
    <table class="log">
      <thead><tr><th>Time</th><th>Symbol</th><th>Signal</th><th>Price</th><th>Delta</th><th>Vol×</th></tr></thead>
      <tbody id="log"></tbody>
    </table>
  </div>
</main>
<script>
const BUY_LABELS = {
  delta_turning_positive:"Delta turning positive",
  market_buys_increasing:"Market buys increasing",
  buy_wall_below_price:"Buy wall below price",
  no_large_sell_wall_above:"No large sell wall above",
  volume_spike:"Volume spike",
};
const SELL_LABELS = {
  delta_negative:"Delta negative",
  market_sells_increasing:"Market sells increasing",
  large_sell_wall_above:"Large sell wall above",
  buy_wall_disappears:"Buy wall disappears",
};
function fmtPrice(p){ if(p==null) return "—"; return Number(p).toLocaleString(undefined,{maximumFractionDigits:8}); }
function fmtTime(ts){ if(!ts) return "—"; return new Date(ts*1000).toLocaleTimeString(); }
function checklist(conditions,labels){
  return Object.entries(labels).map(([key,label])=>{
    const ok=!!(conditions||{})[key];
    return `<li class="${ok?"met":"unmet"}"><span class="check">${ok?"✅":"▫️"}</span>${label}</li>`;
  }).join("");
}
function renderCard(s){
  const m=s.metrics||{}, w=s.walls||{};
  const deltaCls=(m.delta||0)>=0?"pos":"neg";
  const buyPct=(s.buy_score/s.buy_score_max)*100;
  const sellPct=(s.sell_score/s.sell_score_max)*100;
  const walls=[];
  if(w.has_buy_wall) walls.push(`<span class="wall-tag buy">🟢 Buy wall @ ${fmtPrice(w.buy_wall_price)} (${fmtPrice(w.buy_wall_qty)})</span>`);
  if(w.has_sell_wall) walls.push(`<span class="wall-tag sell">🔴 Sell wall @ ${fmtPrice(w.sell_wall_price)} (${fmtPrice(w.sell_wall_qty)})</span>`);
  if(!walls.length) walls.push(`<span class="wall-tag muted">No significant walls</span>`);
  return `<div class="card ${s.signal}">
    <div class="card-head"><span class="sym">${s.symbol}</span><span class="price">${fmtPrice(s.last_price)}</span></div>
    <span class="badge ${s.signal}">${s.signal}</span>
    ${s.enough_flow?"":'<span class="muted" style="font-size:11px;margin-left:8px">low flow</span>'}
    <div class="section-title">Before BUY (${s.buy_score}/${s.buy_score_max})</div>
    <div class="progress"><div class="buy" style="width:${buyPct}%"></div></div>
    <ul class="checklist">${checklist(s.buy_conditions,BUY_LABELS)}</ul>
    <div class="section-title">Before SELL (${s.sell_score}/${s.sell_score_max})</div>
    <div class="progress"><div class="sell" style="width:${sellPct}%"></div></div>
    <ul class="checklist">${checklist(s.sell_conditions,SELL_LABELS)}</ul>
    <div class="walls">${walls.join("")}</div>
    <div class="metrics">
      <div><span class="k">Delta</span><span class="v ${deltaCls}">${m.delta}</span></div>
      <div><span class="k">Δ prev</span><span class="v">${m.delta_prev}</span></div>
      <div><span class="k">Buy vol</span><span class="v pos">${m.buy_volume}</span></div>
      <div><span class="k">Sell vol</span><span class="v neg">${m.sell_volume}</span></div>
      <div><span class="k">Buy / Sell trades</span><span class="v">${m.buy_trades} / ${m.sell_trades}</span></div>
      <div><span class="k">Volume ×</span><span class="v">${m.volume_ratio}×</span></div>
    </div>
  </div>`;
}
function renderLog(signals){
  document.getElementById("log").innerHTML=(signals||[]).map(s=>`
    <tr><td class="muted">${fmtTime(s.timestamp)}</td><td>${s.symbol}</td>
    <td class="sig-${s.signal}">${s.signal}</td><td>${fmtPrice(s.price)}</td>
    <td>${s.metrics?s.metrics.delta:"—"}</td><td>${s.metrics?s.metrics.volume_ratio+"×":"—"}</td></tr>`
  ).join("")||'<tr><td colspan="6" class="muted">No signals yet…</td></tr>';
}
function update(data){
  const st=data.status||{};
  document.getElementById("dot").className="dot "+(st.connected?"on":"off");
  document.getElementById("conn").textContent=st.connected?"Live":"Connecting…";
  document.getElementById("uptime").textContent="uptime "+(st.uptime_sec||0)+"s";
  if(st.config){ document.getElementById("cfg").textContent=
    `win ${st.config.window_sec}s · wall ${st.config.wall_multiple}× · spike ${st.config.volume_spike_multiple}×`; }
  const snaps=(data.snapshots||[]).sort((a,b)=>a.symbol.localeCompare(b.symbol));
  document.getElementById("grid").innerHTML=snaps.map(renderCard).join("");
  renderLog(data.signals||[]);
}
function connect(){
  const proto=location.protocol==="https:"?"wss":"ws";
  const ws=new WebSocket(`${proto}://${location.host}/api/v1/scalper/ws`);
  ws.onmessage=(e)=>{ try{ update(JSON.parse(e.data)); }catch(_){} };
  ws.onclose=()=>{ document.getElementById("dot").className="dot off";
    document.getElementById("conn").textContent="Reconnecting…"; setTimeout(connect,2000); };
}
connect();
</script>
</body>
</html>
"""
