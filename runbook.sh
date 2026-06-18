#!/bin/bash
# =============================================================================
# Capitec Kafka Platform (Spring Boot) — Runbook
# Builds, deploys and seeds the entire platform from scratch.
# Usage:  bash runbook.sh [--skip-build] [--skip-seed]
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log_step()  { echo -e "\n${BOLD}${BLUE}> $1${NC}"; }
log_ok()    { echo -e "  ${GREEN}OK${NC}  $1"; }
log_warn()  { echo -e "  ${YELLOW}WARN${NC} $1"; }
log_error() { echo -e "  ${RED}FAIL${NC} $1" >&2; }
log_info()  { echo -e "  ${CYAN}-->${NC} $1"; }
die()       { log_error "$1"; exit 1; }

SKIP_BUILD=false; SKIP_SEED=false
for arg in "$@"; do [[ "$arg" == "--skip-build" ]] && SKIP_BUILD=true; [[ "$arg" == "--skip-seed" ]] && SKIP_SEED=true; done

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIG_DIR="$(dirname "$REPO_DIR")/capitec-kafka-platform"
cd "$REPO_DIR"

echo ""
echo -e "${BOLD}================================================${NC}"
echo -e "${BOLD}  Capitec Kafka Platform -- Spring Boot         ${NC}"
echo -e "${BOLD}================================================${NC}"
[[ "$SKIP_BUILD" == true ]] && echo -e "  ${YELLOW}--skip-build${NC}: using existing Docker images"
[[ "$SKIP_SEED"  == true ]] && echo -e "  ${YELLOW}--skip-seed${NC}: skipping data seeding"

# ── Step 1: Prerequisites ─────────────────────────────────────────────────────
log_step "Step 1/7 -- Checking prerequisites"
for cmd in java mvn docker kubectl; do
  command -v "$cmd" &>/dev/null && log_ok "$cmd found" || die "$cmd not found. Please install it."
done
JAVA_VER=$(mvn -version 2>&1 | grep -oE 'Java version: [0-9]+' | grep -oE '[0-9]+$' || echo "0")
[[ "$JAVA_VER" -ge 17 ]] && log_ok "Java $JAVA_VER" || die "Java 17+ required. Found: $JAVA_VER"
kubectl cluster-info &>/dev/null && log_ok "Kubernetes reachable" || die "kubectl cannot reach cluster."
docker info &>/dev/null && log_ok "Docker running" || die "Docker daemon not running."

# ── Step 2: Kafka cluster ─────────────────────────────────────────────────────
log_step "Step 2/7 -- Deploying Kafka cluster"
KAFKA_MANIFEST="$ORIG_DIR/k8s/kafka-statefulset.yaml"
[[ -f "$KAFKA_MANIFEST" ]] || KAFKA_MANIFEST="$REPO_DIR/../capitec-kafka-platform/k8s/kafka-statefulset.yaml"
[[ -f "$KAFKA_MANIFEST" ]] && kubectl apply -f "$KAFKA_MANIFEST" &>/dev/null || log_warn "kafka-statefulset.yaml not found -- Kafka may already be running"
log_info "Waiting for Kafka brokers (up to 3 minutes)..."
kubectl rollout status statefulset/kafka --timeout=180s &>/dev/null \
  && log_ok "All Kafka brokers ready" \
  || { READY=$(kubectl get pods -l app=kafka --no-headers 2>/dev/null | grep -c "Running" || true); [[ "$READY" -ge 1 ]] && log_warn "$READY broker(s) Running" || die "Kafka failed to start."; }

# ── Step 3: Topics ────────────────────────────────────────────────────────────
log_step "Step 3/7 -- Creating Kafka topics"
KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[[ -z "$KAFKA_POD" ]] && die "No Kafka pods found."
for topic in order-created customer-created order-cancelled payment-init inventory; do
  kubectl exec "$KAFKA_POD" -- /bin/bash -c \
    "kafka-topics --bootstrap-server localhost:9092 --create --topic $topic --partitions 3 --replication-factor 3 --if-not-exists 2>/dev/null" &>/dev/null \
    && log_ok "Topic: $topic" || log_warn "$topic may exist"
done

# ── Step 4: Build ─────────────────────────────────────────────────────────────
log_step "Step 4/7 -- Building services"
SERVICES=("spring-order-service" "spring-customer-portal" "spring-inventory-service" "spring-payment-processor" "spring-producer-ui")
for svc in "${SERVICES[@]}"; do
  if [[ "$SKIP_BUILD" == true ]] && docker image inspect "$svc:latest" &>/dev/null; then
    log_ok "$svc -- existing image"
  else
    log_info "Building $svc..."
    (cd "$svc" && mvn clean package -q 2>&1) || die "Maven build failed for $svc"
    docker build -t "$svc:latest" "$svc" -q &>/dev/null || die "Docker build failed for $svc"
    log_ok "$svc -- built"
  fi
done

# ── Step 5: Deploy ────────────────────────────────────────────────────────────
log_step "Step 5/7 -- Deploying to Kubernetes"
for svc in "${SERVICES[@]}"; do
  kubectl apply -f "k8s/$svc.yaml" &>/dev/null || die "kubectl apply failed for $svc"
  kubectl rollout restart deployment/"$svc" &>/dev/null || true
  log_info "Waiting for $svc..."
  kubectl rollout status deployment/"$svc" --timeout=90s &>/dev/null \
    && log_ok "$svc -- running" \
    || { READY=$(kubectl get deployment "$svc" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0); [[ "$READY" -ge 1 ]] && log_warn "$svc timeout but pod ready" || die "$svc failed. Run: kubectl logs deployment/$svc"; }
done

# ── Step 6: Port-forward ──────────────────────────────────────────────────────
log_step "Step 6/7 -- Setting up port-forwards"
for port in 8080 8081 8082 8083; do
  PIDS=$(lsof -ti tcp:$port 2>/dev/null || true); [[ -n "$PIDS" ]] && kill $PIDS 2>/dev/null && sleep 1 || true
done

wait_pod() { local label="$1"; for i in $(seq 1 20); do local pod; pod=$(kubectl get pod -l "$label" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); [[ -n "$pod" ]] && echo "$pod" && return 0; sleep 3; done; die "No running pod: $label"; }

kubectl port-forward "pod/$(wait_pod app=spring-producer-ui)"         8080:8080 >/tmp/pf-spring-prod.log  2>&1 &
kubectl port-forward "pod/$(wait_pod app=spring-order-service)"       8081:8081 >/tmp/pf-spring-ord.log   2>&1 &
kubectl port-forward "pod/$(wait_pod app=spring-customer-portal)"     8082:8082 >/tmp/pf-spring-portal.log 2>&1 &
kubectl port-forward "pod/$(wait_pod app=spring-inventory-service)"   8083:8083 >/tmp/pf-spring-inv.log   2>&1 &
sleep 4

for pair in "8080:Producer UI" "8081:Admin Dashboard" "8082:Customer Portal" "8083:Inventory API"; do
  port="${pair%%:*}"; name="${pair##*:}"
  curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/" 2>/dev/null | grep -q "200" \
    && log_ok "$name -> http://localhost:$port" || log_warn "$name :$port not responding -- check /tmp/pf-spring-*.log"
done

# ── Step 7: Seed ──────────────────────────────────────────────────────────────
log_step "Step 7/7 -- Seeding data"
if [[ "$SKIP_SEED" == true ]]; then
  log_warn "Skipping seed (--skip-seed)"
else
  SEED_DIR="$ORIG_DIR/kafka-producer"
  [[ -d "$SEED_DIR" ]] || die "Seed scripts not found at $SEED_DIR. Clone capitec-kafka-platform alongside this repo."
  log_info "Seeding 1000 customers..."
  bash "$SEED_DIR/create-customers.sh" 2>&1 | grep -E "Done|failed" || true
  log_ok "Customers seeded"
  log_info "Restarting portal to sync sequence..."
  kubectl rollout restart deployment/spring-customer-portal &>/dev/null
  kubectl rollout status deployment/spring-customer-portal --timeout=60s &>/dev/null && log_ok "Portal restarted" || log_warn "Portal restart timed out"
  kill $(lsof -ti tcp:8082 2>/dev/null) 2>/dev/null || true; sleep 2
  kubectl port-forward "pod/$(wait_pod app=spring-customer-portal)" 8082:8082 >/tmp/pf-spring-portal.log 2>&1 &
  sleep 3
  log_info "Seeding orders..."
  bash "$SEED_DIR/create-orders.sh" 2>&1 | grep -E "Done|failed" || true
  log_ok "Orders seeded"
  log_info "Stocking inventory..."
  bash "$SEED_DIR/stock-inventory.sh" 2>&1 | grep -E "Done|failed" || true
  log_ok "Inventory stocked"
  sleep 5
  CUSTOMERS=$(curl -s "http://localhost:8081/api/customers?size=1" 2>/dev/null | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('total',0))" 2>/dev/null || echo "?")
  ORDERS=$(curl -s "http://localhost:8081/api/orders?size=1" 2>/dev/null | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('total',0))" 2>/dev/null || echo "?")
  log_ok "DB: $CUSTOMERS customers, $ORDERS orders"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
TEST_CELL=""; TEST_NAME=""; TEST_CNUM=""
if curl -s "http://localhost:8081/api/customers?size=1" &>/dev/null; then
  TEST_JSON=$(curl -s "http://localhost:8081/api/customers?size=1" 2>/dev/null)
  TEST_CELL=$(echo "$TEST_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);c=d.get('customers',[]);print(c[0]['cell'] if c else '')" 2>/dev/null || echo "")
  TEST_NAME=$(echo "$TEST_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);c=d.get('customers',[]);print(c[0]['firstName']+' '+c[0]['lastName'] if c else '')" 2>/dev/null || echo "")
  TEST_CNUM=$(echo "$TEST_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);c=d.get('customers',[]);print(c[0]['customerNumber'] if c else '')" 2>/dev/null || echo "")
fi
TEST_CELL="${TEST_CELL:-0601000700}"; TEST_NAME="${TEST_NAME:-Thabo Dlamini}"; TEST_CNUM="${TEST_CNUM:-1000000000}"

echo ""
echo -e "${BOLD}${GREEN}================================================${NC}"
echo -e "${BOLD}${GREEN}  Capitec Kafka Platform (Spring Boot) Ready!   ${NC}"
echo -e "${BOLD}${GREEN}================================================${NC}"
echo ""
echo -e "  ${BOLD}${CYAN}Portals${NC}"
echo -e "  +-------------------------------------------------------------+"
echo -e "  |  ${BOLD}Customer Order Portal${NC}   ${GREEN}http://localhost:8082${NC}             |"
echo -e "  |  ${BOLD}Admin Dashboard${NC}         ${GREEN}http://localhost:8081${NC}             |"
echo -e "  +-------------------------------------------------------------+"
echo ""
echo -e "  ${BOLD}${CYAN}Test Customer${NC}"
echo -e "  +-------------------------------------------------------------+"
echo -e "  |  ${BOLD}Name:${NC}         $TEST_NAME"
echo -e "  |  ${BOLD}Customer #:${NC}   $TEST_CNUM"
echo -e "  |  ${BOLD}Cell:${NC}         ${YELLOW}$TEST_CELL${NC}"
echo -e "  |  ${BOLD}Password:${NC}     ${YELLOW}Capitec@01${NC}"
echo -e "  +-------------------------------------------------------------+"
echo ""
echo -e "  ${BOLD}Options:${NC}"
echo -e "    ${CYAN}bash runbook.sh --skip-build${NC}   # reuse existing images"
echo -e "    ${CYAN}bash runbook.sh --skip-seed${NC}    # skip seeding"
echo ""

if command -v open &>/dev/null; then open http://localhost:8082; open http://localhost:8081; log_ok "Portals opened in browser"
elif grep -qi microsoft /proc/version 2>/dev/null; then explorer.exe http://localhost:8082 2>/dev/null||true; explorer.exe http://localhost:8081 2>/dev/null||true; log_ok "Portals opened in browser"
elif command -v xdg-open &>/dev/null; then xdg-open http://localhost:8082 &>/dev/null &; xdg-open http://localhost:8081 &>/dev/null &; log_ok "Portals opened in browser"
else log_warn "Open the URLs above manually"; fi
