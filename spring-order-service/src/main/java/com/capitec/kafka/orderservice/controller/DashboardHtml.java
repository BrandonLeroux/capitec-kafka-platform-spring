package com.capitec.kafka.orderservice.controller;

public class DashboardHtml {
    public static String build() {
        return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Capitec Admin Dashboard (Spring Boot)</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;padding:2rem}
    h1{font-size:1.4rem;font-weight:700;margin-bottom:1.5rem}
    .tabs{display:flex;gap:0;margin-bottom:2rem;border-bottom:1px solid #334155}
    .tab{padding:.6rem 1.4rem;font-size:.85rem;font-weight:600;cursor:pointer;color:#64748b;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .15s}
    .tab.active{color:#818cf8;border-bottom-color:#6366f1}
    .tab-panel{display:none}.tab-panel.active{display:block}
    .stock-ok{color:#22c55e;font-weight:700}
    .stock-low{color:#f59e0b;font-weight:700}
    .stock-out{color:#ef4444;font-weight:700}
    .stats{display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:2rem}
    .stat{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:1rem 1.5rem;min-width:120px;cursor:pointer;transition:border-color .15s,background .15s}
    .stat:hover{border-color:#6366f1;background:#262f42}
    .stat.active{border-color:#6366f1;background:#1e1b4b}
    .stat-label{font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:.3rem}
    .stat-value{font-size:1.5rem;font-weight:700}
    .toolbar{display:flex;gap:.75rem;margin-bottom:1rem;flex-wrap:wrap;align-items:center}
    input[type=text]{background:#1e293b;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:.5rem .75rem;font-size:.85rem;outline:none;width:260px}
    select{background:#1e293b;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:.5rem .75rem;font-size:.85rem;outline:none;cursor:pointer}
    .btn{background:#6366f1;color:#fff;border:none;border-radius:8px;padding:.5rem 1rem;font-size:.85rem;font-weight:600;cursor:pointer}
    table{width:100%;border-collapse:collapse;font-size:.82rem}
    thead th{text-align:left;padding:.6rem .75rem;color:#64748b;font-weight:600;font-size:.72rem;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #334155}
    tbody tr{border-bottom:1px solid #1e293b;transition:background .1s}
    tbody tr:hover{background:#1e293b}
    td{padding:.6rem .75rem;color:#cbd5e1;vertical-align:middle}
    td.mono{font-family:monospace;color:#e2e8f0;font-weight:600}
    .badge{display:inline-block;padding:.15rem .55rem;border-radius:999px;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
    .badge-CONFIRMED{background:#1e1b4b;color:#818cf8}
    .badge-PAYMENT-INIT{background:#052e16;color:#22c55e}
    .badge-PAYMENT-PROCESSED{background:#0c1a2e;color:#38bdf8}
    .badge-PACKED{background:#042f2e;color:#06b6d4}
    .badge-OUT-FOR-DELIVERY{background:#431407;color:#f97316}
    .badge-DELIVERED{background:#1e293b;color:#94a3b8}
    .badge-CANCELLED{background:#2d0707;color:#ef4444}
    .badge-PAYMENT-FAILED{background:#450a0a;color:#fca5a5}
    .badge-UNKNOWN{background:#1e293b;color:#475569}
    .pagination{display:flex;gap:.5rem;align-items:center;margin-top:1rem;font-size:.82rem;color:#64748b}
    .pagination button{background:#1e293b;border:1px solid #334155;color:#e2e8f0;border-radius:6px;padding:.3rem .65rem;cursor:pointer;font-size:.8rem}
    .pagination button:disabled{opacity:.4;cursor:default}
    .empty{text-align:center;padding:3rem;color:#475569}
    .auto-badge{font-size:.65rem;background:#1e293b;border:1px solid #334155;border-radius:4px;padding:.1rem .4rem;color:#64748b;margin-left:.5rem;cursor:pointer}
    .sb-badge{background:#052e16;color:#22c55e;font-size:.65rem;padding:.15rem .45rem;border-radius:4px;margin-left:.5rem;font-weight:700}
  </style>
</head>
<body>
<h1>Admin Dashboard <span class="sb-badge">Spring Boot</span> <span class="auto-badge" id="auto-label">auto-refresh: ON</span>
  <a href="http://localhost:30080" target="_blank" style="margin-left:1rem;font-size:.75rem;font-weight:600;color:#38bdf8;text-decoration:none;border:1px solid #334155;border-radius:6px;padding:.25rem .7rem;background:#0c1a2e">Kafka UI ↗</a>
</h1>
<div class="tabs">
  <div class="tab active" onclick="switchTab('orders')">Orders</div>
  <div class="tab"        onclick="switchTab('customers')">Customers</div>
  <div class="tab"        onclick="switchTab('inventory')">Inventory</div>
</div>

<div class="tab-panel active" id="tab-orders">
  <div class="stats" id="order-stats"></div>
  <div class="toolbar">
    <input type="text" id="order-search" placeholder="Search order, customer, product…" oninput="debounce(()=>{orderPage=1;loadOrders()})"/>
    <select id="status-filter" onchange="orderPage=1;loadOrders()">
      <option value="ALL">All statuses</option>
      <option>CONFIRMED</option><option>PAYMENT-INIT</option><option>PAYMENT-PROCESSED</option>
      <option>PACKED</option><option>OUT-FOR-DELIVERY</option><option>DELIVERED</option>
      <option>CANCELLED</option><option value="PAYMENT-FAILED">PAYMENT FAILED</option>
    </select>
    <button class="btn" onclick="loadOrders()">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Order ID</th><th>Customer</th><th>Product</th><th>Amount</th><th>Status</th><th>Reason</th><th>Received</th><th>Updated</th></tr></thead>
    <tbody id="order-tbody"></tbody>
  </table>
  <div class="empty" id="order-empty" style="display:none">No orders found.</div>
  <div class="pagination" id="order-pagination"></div>
</div>

<div class="tab-panel" id="tab-customers">
  <div class="toolbar">
    <input type="text" id="cust-search" placeholder="Search name, email, cell…" oninput="debounce(()=>{custPage=1;loadCustomers()})"/>
    <button class="btn" onclick="loadCustomers()">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Customer #</th><th>ID</th><th>Name</th><th>ID Number</th><th>Email</th><th>Cell</th><th>Registered</th></tr></thead>
    <tbody id="cust-tbody"></tbody>
  </table>
  <div class="empty" id="cust-empty" style="display:none">No customers found.</div>
  <div class="pagination" id="cust-pagination"></div>
</div>

<div class="tab-panel" id="tab-inventory">
  <div class="toolbar">
    <input type="text" id="inv-search" placeholder="Search SKU, name, product…" oninput="debounce(()=>loadInventory())"/>
    <select id="inv-cat" onchange="loadInventory()">
      <option value="ALL">All categories</option>
      <option>Tyres</option><option>Brakes</option><option>Batteries</option>
      <option>Filters</option><option>Wipers</option><option>Shocks</option>
      <option>Lighting</option><option>Oils</option>
    </select>
    <button class="btn" onclick="loadInventory()">Refresh</button>
  </div>
  <div id="inv-stats" style="display:flex;gap:1rem;margin-bottom:1rem;flex-wrap:wrap"></div>
  <table>
    <thead><tr><th>SKU</th><th>Product ID</th><th>Name</th><th>Category</th><th>Stock</th><th>Reorder Level</th><th>Unit Price</th><th>Updated</th></tr></thead>
    <tbody id="inv-tbody"></tbody>
  </table>
  <div class="empty" id="inv-empty" style="display:none">No inventory found.</div>
</div>

<script>
  let orderPage=1,custPage=1;const pageSize=20;
  let autoRefresh=true,autoTimer=null,debTimer=null,activeTab='orders';

  function switchTab(n){
    activeTab=n;
    document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',['orders','customers','inventory'][i]===n));
    document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
    document.getElementById('tab-'+n).classList.add('active');
    if(n==='orders')loadOrders();else if(n==='inventory')loadInventory();else loadCustomers();
  }

  async function loadInventory(){
    const s=document.getElementById('inv-search').value.trim();
    const cat=document.getElementById('inv-cat').value;
    const p=new URLSearchParams({size:200});
    if(s)p.set('search',s);if(cat!=='ALL')p.set('category',cat);
    const data=await fetch('/api/inventory?'+p).then(r=>r.json()).catch(()=>({total:0,lowStock:0,items:[]}));
    document.getElementById('inv-stats').innerHTML=[
      ['Total SKUs',data.total||0],['Low Stock',data.lowStock||0]
    ].map(([l,v])=>`<div class="stat"><div class="stat-label">${l}</div><div class="stat-value">${v}</div></div>`).join('');
    const items=data.items||[];
    const empty=document.getElementById('inv-empty');
    if(!items.length){document.getElementById('inv-tbody').innerHTML='';empty.style.display='';return;}
    empty.style.display='none';
    document.getElementById('inv-tbody').innerHTML=items.map(i=>{
      const cls=i.quantity===0?'stock-out':i.quantity<=i.reorderLevel?'stock-low':'stock-ok';
      return`<tr><td class="mono">${i.sku}</td><td style="font-size:.75rem;color:#64748b">${i.productID}</td>
      <td>${i.name}</td><td>${i.category}</td>
      <td class="${cls}">${i.quantity}</td><td>${i.reorderLevel}</td>
      <td>R ${Number(i.unitPrice).toLocaleString('en-ZA',{minimumFractionDigits:2})}</td>
      <td>${i.updatedAt}</td></tr>`;}).join('');
  }

  async function loadOrders(){
    const s=document.getElementById('order-search').value.trim();
    const st=document.getElementById('status-filter').value;
    const p=new URLSearchParams({page:orderPage,size:pageSize});
    if(s)p.set('search',s);if(st!=='ALL')p.set('status',st);
    const data=await fetch('/api/orders?'+p).then(r=>r.json());
    const stats=data.stats||{};
    const grandTotal=Object.values(stats).filter((_,i)=>i>0).reduce((a,b)=>a+(typeof b==='number'?b:0),0) ||
      Object.entries(stats).filter(([k])=>k!=='PAYMENT-FAILED').reduce((a,[,v])=>a+v,0);
    const activeSt=document.getElementById('status-filter').value;
    const tiles=[
      ['Total',grandTotal,'ALL'],
      ['Confirmed',stats['CONFIRMED']||0,'CONFIRMED'],
      ['Payment Init',stats['PAYMENT-INIT']||0,'PAYMENT-INIT'],
      ['Processed',stats['PAYMENT-PROCESSED']||0,'PAYMENT-PROCESSED'],
      ['Packed',stats['PACKED']||0,'PACKED'],
      ['Out for Del.',stats['OUT-FOR-DELIVERY']||0,'OUT-FOR-DELIVERY'],
      ['Delivered',stats['DELIVERED']||0,'DELIVERED'],
      ['Cancelled',stats['CANCELLED']||0,'CANCELLED'],
      ['Payment Failed',stats['PAYMENT-FAILED']||0,'PAYMENT-FAILED'],
    ];
    document.getElementById('order-stats').innerHTML=tiles.map(([l,v,filterVal])=>
      `<div class="stat${activeSt===filterVal?' active':''}" onclick="filterByStatus('${filterVal}')">
        <div class="stat-label">${l}</div>
        <div class="stat-value">${v}</div>
      </div>`).join('');
    const empty=document.getElementById('order-empty');
    if(!data.orders.length){document.getElementById('order-tbody').innerHTML='';empty.style.display='';return;}
    empty.style.display='none';
    document.getElementById('order-tbody').innerHTML=data.orders.map(o=>`
      <tr><td class="mono">${o.orderID}</td><td>${o.customerID}</td>
      <td>${fmt(o.product)}</td>
      <td>R ${Number(o.amount).toLocaleString('en-ZA',{minimumFractionDigits:2})}</td>
      <td><span class="badge badge-${o.status||'UNKNOWN'}">${o.status||'—'}</span></td>
      <td style="font-size:.78rem;color:#94a3b8">${o.cancellationReason||'—'}</td>
      <td>${o.receivedAt}</td><td>${o.updatedAt}</td></tr>`).join('');
    renderPag('order',data.total,orderPage,p=>{orderPage=p;loadOrders()});
  }

  async function loadCustomers(){
    const s=document.getElementById('cust-search').value.trim();
    const p=new URLSearchParams({page:custPage,size:pageSize});if(s)p.set('search',s);
    const data=await fetch('/api/customers?'+p).then(r=>r.json());
    const empty=document.getElementById('cust-empty');
    if(!data.customers.length){document.getElementById('cust-tbody').innerHTML='';empty.style.display='';return;}
    empty.style.display='none';
    document.getElementById('cust-tbody').innerHTML=data.customers.map(c=>`
      <tr><td class="mono">${c.customerNumber>0?c.customerNumber:'—'}</td>
      <td class="mono">${c.customerID}</td>
      <td>${c.firstName} ${c.lastName}</td><td>${c.idNumber||'—'}</td>
      <td>${c.email||'—'}</td><td>${c.cell||'—'}</td><td>${c.createdAt}</td></tr>`).join('');
    renderPag('cust',data.total,custPage,p=>{custPage=p;loadCustomers()});
  }

  function fmt(s){if(!s)return'—';return s.replace(/_/g,' ').split(' ').map(w=>w.charAt(0).toUpperCase()+w.slice(1).toLowerCase()).join(' ').replace(/(\\d+) (\\d+) (R\\d+)/i,'$1/$2 $3');}
  function renderPag(px,total,page,fn){
    const pages=Math.ceil(total/pageSize);const el=document.getElementById(px+'-pagination');
    if(pages<=1){el.innerHTML='';return;}
    el.innerHTML=`<button onclick="(${fn.toString()})(${page-1})" ${page===1?'disabled':''}>← Prev</button>
      <span>Page ${page} of ${pages} (${total})</span>
      <button onclick="(${fn.toString()})(${page+1})" ${page>=pages?'disabled':''}>Next →</button>`;
  }
  function filterByStatus(val){
    document.getElementById('status-filter').value=val;
    orderPage=1;loadOrders();
  }

  function debounce(fn){clearTimeout(debTimer);debTimer=setTimeout(fn,300);}
  function scheduleAuto(){clearTimeout(autoTimer);if(!autoRefresh)return;autoTimer=setTimeout(()=>{if(activeTab==='orders')loadOrders();else if(activeTab==='inventory')loadInventory();else loadCustomers();scheduleAuto();},3000);}
  document.getElementById('auto-label').addEventListener('click',()=>{autoRefresh=!autoRefresh;document.getElementById('auto-label').textContent='auto-refresh: '+(autoRefresh?'ON':'OFF');scheduleAuto();});
  loadOrders();scheduleAuto();
</script>
</body></html>
""";
    }
}
