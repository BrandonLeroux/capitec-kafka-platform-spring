package com.capitec.kafka.producer.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
public class ProducerController {

    private static final Logger log = LoggerFactory.getLogger(ProducerController.class);

    private final KafkaTemplate<String, String> kafka;
    private final String orderTopic;
    private final String customerTopic;

    public ProducerController(KafkaTemplate<String, String> kafka,
                              @Value("${app.topics.order}")    String orderTopic,
                              @Value("${app.topics.customer}") String customerTopic) {
        this.kafka         = kafka;
        this.orderTopic    = orderTopic;
        this.customerTopic = customerTopic;
    }

    @GetMapping("/")
    public ResponseEntity<String> ui() {
        return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(buildHtml());
    }

    @PostMapping("/send")
    public ResponseEntity<?> send(@RequestBody Map<String, String> body) {
        String key   = body.getOrDefault("key", "key-" + System.currentTimeMillis());
        String value = body.get("value");
        if (value == null || value.isBlank()) return ResponseEntity.badRequest().body(Map.of("error", "value required"));

        var future = kafka.send(orderTopic, key, value);
        try {
            var meta = future.get();
            log.info("Sent topic={} key={} offset={}", orderTopic, key, meta.getRecordMetadata().offset());
            return ResponseEntity.ok(Map.of(
                "topic", meta.getRecordMetadata().topic(),
                "partition", meta.getRecordMetadata().partition(),
                "offset", meta.getRecordMetadata().offset(),
                "key", key));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody Map<String, String> body) {
        String customerID = body.getOrDefault("customerID", "CUST-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        String payload = String.format(
            "{\"customerID\":\"%s\",\"firstName\":\"%s\",\"lastName\":\"%s\",\"email\":\"%s\",\"cell\":\"%s\"}",
            customerID,
            body.getOrDefault("firstName", ""),
            body.getOrDefault("lastName", ""),
            body.getOrDefault("email", ""),
            body.getOrDefault("cell", ""));
        kafka.send(customerTopic, customerID, payload);
        log.info("Customer registered customerID={}", customerID);
        return ResponseEntity.ok(Map.of("customerID", customerID, "isNew", true));
    }

    private String buildHtml() {
        return """
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"/>
<title>Kafka Producer UI (Spring Boot)</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:2rem}
.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:2rem;width:100%;max-width:640px}
h1{font-size:1.25rem;font-weight:700;margin-bottom:.25rem}
.sb-badge{font-size:.65rem;background:#052e16;color:#22c55e;padding:.15rem .45rem;border-radius:4px;margin-left:.5rem;font-weight:700}
.topic-badge{display:inline-block;background:#0f172a;border:1px solid #334155;border-radius:6px;padding:.2rem .6rem;font-size:.75rem;color:#94a3b8;margin-bottom:1.5rem}
label{display:block;font-size:.8rem;color:#94a3b8;margin-bottom:.4rem;margin-top:1rem}
input,textarea{width:100%;background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:.6rem .75rem;font-size:.9rem;font-family:inherit;outline:none;transition:border-color .15s}
input:focus,textarea:focus{border-color:#6366f1}
textarea{resize:vertical;min-height:140px;font-family:monospace;font-size:.82rem}
.row{display:flex;gap:.75rem;margin-top:1rem}
button{flex:1;background:#6366f1;color:#fff;border:none;border-radius:8px;padding:.65rem 1rem;font-size:.9rem;font-weight:600;cursor:pointer;transition:background .15s}
button:hover{background:#4f46e5}
button:disabled{background:#334155;color:#64748b;cursor:not-allowed}
#clear-btn{flex:0 0 auto;background:#1e293b;border:1px solid #334155;color:#94a3b8}
#clear-btn:hover{background:#0f172a;color:#e2e8f0}
.log{margin-top:1.5rem}
.log h2{font-size:.78rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:.5rem}
.log-list{list-style:none;display:flex;flex-direction:column;gap:.4rem;max-height:220px;overflow-y:auto}
.log-item{background:#0f172a;border:1px solid #1e293b;border-radius:6px;padding:.5rem .75rem;font-size:.78rem;font-family:monospace;display:flex;gap:.5rem;align-items:flex-start}
.log-item.ok{border-left:3px solid #22c55e}
.log-item.err{border-left:3px solid #ef4444}
.log-meta{color:#64748b;white-space:nowrap}
.log-body{color:#cbd5e1;word-break:break-all}
.toast{position:fixed;bottom:1.5rem;right:1.5rem;background:#22c55e;color:#fff;padding:.6rem 1rem;border-radius:8px;font-size:.85rem;font-weight:600;opacity:0;pointer-events:none;transition:opacity .3s}
.toast.show{opacity:1}
.toast.error{background:#ef4444}
</style></head><body>
<div class="card">
  <h1>Kafka Producer <span class="sb-badge">Spring Boot</span></h1>
  <span class="topic-badge">topic: order-created</span>
  <label>Key <span style="color:#475569">(optional)</span></label>
  <input id="key" type="text" placeholder="order-001"/>
  <label>Value</label>
  <textarea id="value" placeholder='{"orderID":"001","amount":150.00}'></textarea>
  <div class="row">
    <button id="send-btn" onclick="sendMessage()">Send</button>
    <button id="clear-btn" onclick="clearLog()">Clear log</button>
  </div>
  <div class="log">
    <h2>Message log</h2>
    <ul class="log-list" id="log"></ul>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
  async function sendMessage(){
    const key=document.getElementById('key').value.trim();
    const value=document.getElementById('value').value.trim();
    if(!value){showToast('Value is required',true);return;}
    const btn=document.getElementById('send-btn');
    btn.disabled=true;btn.textContent='Sending…';
    try{
      const res=await fetch('/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key,value})});
      const data=await res.json();
      if(res.ok){addLog(true,`partition=${data.partition} offset=${data.offset} key=${data.key}`,value);showToast(`Sent → partition ${data.partition}, offset ${data.offset}`);document.getElementById('value').value='';document.getElementById('key').value='';}
      else{addLog(false,data.error,value);showToast(data.error,true);}
    }catch(e){addLog(false,e.message,value);showToast(e.message,true);}
    finally{btn.disabled=false;btn.textContent='Send';}
  }
  function addLog(ok,meta,body){const li=document.createElement('li');li.className='log-item '+(ok?'ok':'err');li.innerHTML=`<span class="log-meta">${meta}</span><span class="log-body">${esc(body)}</span>`;document.getElementById('log').prepend(li);}
  function clearLog(){document.getElementById('log').innerHTML='';}
  function showToast(msg,error=false){const t=document.getElementById('toast');t.textContent=msg;t.className='toast show'+(error?' error':'');setTimeout(()=>t.className='toast',2500);}
  function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
  document.getElementById('value').addEventListener('keydown',e=>{if(e.key==='Enter'&&e.metaKey)sendMessage();});
</script></body></html>
""";
    }
}
