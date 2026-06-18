# Capitec Kafka Platform — Spring Boot Edition

A Spring Boot rewrite of the [capitec-kafka-platform](https://github.com/BrandonLeroux/capitec-kafka-platform), demonstrating the same event-driven microservices architecture using **Spring Boot 3.2** and **Spring Kafka**.

## What's different from the plain-Java version

| Aspect | Plain Java | Spring Boot |
|---|---|---|
| HTTP server | `ServerSocket` loop | `@RestController` + Spring MVC |
| Kafka consumers | Manual `KafkaConsumer` poll loop | `@KafkaListener` |
| Kafka producers | Manual `KafkaProducer` | `KafkaTemplate` |
| Manual commit | `commitSync()` | `AckMode.MANUAL_IMMEDIATE` + `Acknowledgment.acknowledge()` |
| DB access | Raw JDBC `PreparedStatement` | `JdbcTemplate` |
| Config | `.properties` file loaded manually | `application.properties` + `@Value` |
| Async processing | `ExecutorService` | `@Async` + `@EnableAsync` |
| Session management | In-memory `ConcurrentHashMap` | `HttpSession` (Spring MVC) |

## Services

| Service | Port | Purpose |
|---|---|---|
| `spring-order-service` | 8081 | Admin dashboard, Kafka consumer, REST API |
| `spring-customer-portal` | 8082 | Customer-facing order portal |
| `spring-inventory-service` | 8083 | Inventory tracking |
| `spring-payment-processor` | — | Mock payment fulfilment pipeline |
| `spring-producer-ui` | 8080 | Dev tool — raw message publishing |

## Platform Compatibility

- **macOS** — native
- **Windows** — run inside **WSL**

## Quick start

```bash
# 1. Deploy Kafka cluster
kubectl apply -f ../capitec-kafka-platform/k8s/kafka-statefulset.yaml
kubectl rollout status statefulset/kafka --timeout=180s

# 2. Build all services
for svc in spring-order-service spring-customer-portal spring-inventory-service spring-payment-processor spring-producer-ui; do
  cd $svc && mvn clean package -q && docker build -t $svc:latest . -q && cd ..
done

# 3. Deploy to Kubernetes
kubectl apply -f k8s/

# 4. Port-forward
kubectl port-forward svc/spring-order-service   8081:8081 &
kubectl port-forward svc/spring-customer-portal 8082:8082 &
kubectl port-forward svc/spring-producer-ui     8080:8080 &
kubectl port-forward svc/spring-inventory-service 8083:8083 &
```

Or use the included runbook:
```bash
bash runbook.sh
```

## Topics

| Topic | Purpose |
|---|---|
| `order-created` | All order lifecycle events |
| `customer-created` | New customer registrations |
| `order-cancelled` | Cancellation with reason |
| `payment-init` | Triggers payment processor |
| `inventory` | Stock SET/ADJUST events (key=SKU) |

## Test login

After seeding:
- **Cell:** `0601000700`
- **Password:** `Capitec@01`

## Tech Stack

- **Spring Boot 3.2** — all services
- **Spring Kafka** — `@KafkaListener` consumers, `KafkaTemplate` producers
- **Spring JDBC** — `JdbcTemplate` with SQLite
- **SQLite** — embedded DB
- **Java 17**
- **Rancher Desktop** — local Kubernetes (k3s)
