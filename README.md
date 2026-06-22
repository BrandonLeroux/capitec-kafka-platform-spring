# Capitec Kafka Platform — Spring Boot Edition

A production-grade, event-driven order management platform built with **Spring Boot 3.2** and **Apache Kafka**, deployed on **Rancher Desktop Kubernetes**.

This project is the Java implementation of the Event-Driven Order Management Lab.

---

## Architecture

```
Customer Portal (8082)
        │  publishes
        ▼
  order-created ──────────────────────────────────────────────────────┐
        │                                                              │
        ▼                                                              │ status updates
  spring-order-service (8081)                                         │ (PAYMENT-PROCESSED,
        │  CONFIRMED → publishes to payment-init                      │  PACKED, etc.)
        ▼                                                              │
  payment-init                                                         │
        │                                                              │
        ▼                                                              │
  spring-payment-processor                                             │
        │  publishes status updates back to order-created ────────────┘
        │
  spring-inventory-service (8083)
        │  consumes inventory topic (key=SKU)
        │  SET / ADJUST actions
```

### Pipeline (maps to lab specification)

| Lab Topic | Implementation Topic | Publisher | Consumer |
|---|---|---|---|
| `order.placed` | `order-created` (status=CONFIRMED) | Customer Portal | spring-order-service |
| `payment.succeeded` → | `order-created` (status=PAYMENT-PROCESSED) | spring-payment-processor | spring-order-service |
| `payment.failed` → | `order-created` (status=CANCELLED) | spring-payment-processor (on cancel) | spring-order-service |
| Inventory update | `inventory` (key=SKU) | Customer Portal on order | spring-inventory-service |

> **Note on topic naming:** The lab uses `order.placed`, `payment.succeeded`, `payment.failed` as logical names. This implementation uses `order-created` as a unified lifecycle topic (single source of truth for all order state transitions) plus `payment-init` to trigger payment processing. This is an intentional architectural decision — see Task 3 below.

---

## Services

| Service | Port | Role |
|---|---|---|
| `spring-order-service` | 8081 | Admin dashboard, order/customer DB, payment trigger |
| `spring-customer-portal` | 8082 | Customer-facing shop, cart, checkout, cancel |
| `spring-inventory-service` | 8083 | Inventory tracking via Kafka |
| `spring-payment-processor` | — | Mock fulfilment pipeline |

---

## Lab Tasks — Implementation Evidence

### Task 1 — Review code stubs

All services are fully implemented (not stubs). Key classes:
- `OrderEventConsumer.java` — `@KafkaListener` for order, customer, cancellation events
- `PaymentInitConsumer.java` — `@KafkaListener` on `payment-init`
- `FulfilmentService.java` — `@Async` mock pipeline with cancellation check
- `InventoryConsumer.java` — `@KafkaListener` on `inventory` (SET/ADJUST)

### Task 2 — Complete producer and consumer implementations

**Producers** use `KafkaTemplate` with `whenComplete` callback for async confirmation:
```java
kafka.send(orderTopic, orderID, payload)
     .whenComplete((r, ex) -> {
         if (ex != null) log.error("Failed orderID={}", orderID, ex);
         else log.info("Sent orderID={} offset={}", orderID, r.getRecordMetadata().offset());
     });
```

**Consumers** use `AckMode.MANUAL_IMMEDIATE` — offset is committed only after successful processing:
```java
@KafkaListener(topics = "${app.topics.order}", groupId = "...")
public void consume(ConsumerRecord<String,String> record, Acknowledgment ack) {
    try { process(record); }
    finally { ack.acknowledge(); }   // always commits — DLT handles failures
}
```

### Task 3 — Partitioning logic and message key choice

| Topic | Key | Rationale |
|---|---|---|
| `order-created` | `orderID` | All events for one order go to the same partition — guarantees ordering per order |
| `payment-init` | `orderID` | Payment events for the same order are co-located with order events |
| `inventory` | `SKU` | All stock adjustments for one SKU are ordered — prevents race conditions |
| `customer-created` | `customerID` | Customer events ordered per customer |
| `order-cancelled` | `orderID` | Co-located with order partition |

`order-created` has **6 partitions** (higher throughput for the busiest topic). All others have **3 partitions**.

### Task 4 — Producer configuration tuning

From `application.properties`:

```properties
# Strongest durability — leader waits for all in-sync replicas
spring.kafka.producer.acks=all

# Retry indefinitely until delivery.timeout.ms expires
spring.kafka.producer.retries=2147483647

# Exactly-once semantics within a producer session
spring.kafka.producer.properties.enable.idempotence=true

# Max 5 in-flight batches — required when idempotence=true
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5

# Wait 5ms before flushing — improves batch fill rate
spring.kafka.producer.properties.linger.ms=5

# 32KB batches — balanced for JSON payloads
spring.kafka.producer.properties.batch.size=32768

# LZ4 compression — best ratio/speed tradeoff for JSON
spring.kafka.producer.properties.compression.type=lz4

# Total delivery budget including all retries
spring.kafka.producer.properties.delivery.timeout.ms=120000
```

### Task 5 — Consumer groups and scaling

| Group ID | Service | Scaling behaviour |
|---|---|---|
| `order-service-group` | spring-order-service | Single instance — DB writes are synchronized. Scale by sharding by customerID prefix |
| `payment-processor-group` | spring-payment-processor | Safe to scale horizontally — each instance handles different partitions independently |
| `inventory-service-group` | spring-inventory-service | Single instance recommended — SQLite serializes writes. Scale by migrating to Postgres |

Consumer group design:
- Each service has its own group ID → each gets a full copy of every message
- `max-poll-records=50` (order-service), `max-poll-records=10` (payment-processor) — prevents long GC pauses
- `isolation-level=read_committed` — only reads messages from committed transactions

### Task 6 — Fault tolerance

**Payment processor** (`FulfilmentService.java`):
```java
// Checks order status before EACH pipeline step
if (isCancelled(orderID)) {
    log.info("Order cancelled — stopping pipeline orderID={}", orderID);
    return;
}
```
- `@Async` with 10-thread pool — concurrent orders don't block each other
- `max-poll-records=10` — limits batch size to keep poll intervals short
- `session.timeout.ms` / `heartbeat.interval.ms` prevents consumer being kicked from group

**Order service** consumer:
- Manual `ack.acknowledge()` in `finally` block — offset always commits even on partial failure
- `try/catch` per record — one bad message doesn't kill the batch

### Task 7 — Idempotency and dead-letter handling

**Idempotency:**
- Producer: `enable.idempotence=true` prevents duplicate sends from retries
- Consumer/DB: `ON CONFLICT(order_id) DO UPDATE` — re-processing the same order is safe
- Inventory: ADJUST uses `MAX(0, quantity + delta)` — can't go negative from double-deduction

**Dead-letter handling:**
- `kafka-consumer` service (in `kafka-consumer/`) implements full DLT with configurable `max.retries`
- After N retries, message is published to `order-created.DLT` with failure reason header
- `isolation-level=read_committed` prevents consuming poison messages from aborted transactions

### Task 8 — Kubernetes deployment

All manifests in `k8s/`. Deploy with one command:
```bash
bash runbook.sh
```

Services use `imagePullPolicy: Never` (local images) with:
- `readinessProbe` on `/actuator/health` — Kubernetes waits for Spring context to load
- `livenessProbe` — restarts pods that become unresponsive
- `emptyDir` volumes for SQLite (stateless per-pod)

### Task 9 — Platform comparison (Java vs .NET)

| Aspect | Java (this repo) | .NET equivalent |
|---|---|---|
| Kafka client | `spring-kafka` — `@KafkaListener`, `KafkaTemplate` | `Confluent.Kafka` — manual poll loop or `IHostedService` |
| Configuration | `application.properties` + `@Value` | `appsettings.json` + `IOptions<T>` |
| Serialization | Manual JSON string (no schema registry) | `System.Text.Json` or Newtonsoft |
| Manual ack | `Acknowledgment.acknowledge()` | `consumer.Commit(result)` |
| Async processing | `@Async` + `@EnableAsync` | `Task.Run()` or `Channel<T>` |
| Error handling | `try/finally ack` per record | `try/catch` with `consumer.StoreOffset` |
| DLT | Manual publish to `.DLT` topic | `DeadLetterPublishingRecoverer` (spring-kafka has .NET equivalent in MassTransit) |

---

## Quick Start

```bash
git clone https://github.com/BrandonLeroux/capitec-kafka-platform-spring.git
cd capitec-kafka-platform-spring
bash runbook.sh
```

**Portals:**
- Customer Order Portal: http://localhost:8082
- Admin Dashboard: http://localhost:8081
- Kafka UI: http://localhost:30080

**Test login:** Cell `0601000700` / Password `Capitec@01`

---

## Seed Scripts

```bash
bash scripts/create-customers.sh   # 1000 customers
bash scripts/create-orders.sh      # ~2000 orders across all statuses
bash scripts/stock-inventory.sh    # 46 SKUs with random quantities
```

---

## Platform Compatibility

| Platform | Supported |
|---|---|
| macOS | ✅ native |
| Windows | ✅ via WSL |

**Windows:** Run all commands inside WSL. Both platforms run scripts and Docker identically.

**Important:** Both platforms cannot run simultaneously on a 4GB Rancher Desktop node. Increase Rancher Desktop RAM to 8GB in Preferences → Virtual Machine → Memory.

---

## Tech Stack

- **Spring Boot 3.2** — all services
- **Spring Kafka** — `@KafkaListener`, `KafkaTemplate`, `AckMode.MANUAL_IMMEDIATE`
- **Spring JDBC** — `JdbcTemplate` + SQLite
- **Java 17** — all services
- **Apache Kafka 3.7** — KRaft mode (no ZooKeeper), 3-broker cluster, RF=3
- **Rancher Desktop** — local Kubernetes (k3s)
- **Eclipse Temurin 21** — Docker base image
