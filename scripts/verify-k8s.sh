#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} != --simulated ]]; then
  echo 'Usage: bash scripts/verify-k8s.sh --simulated'
  echo 'Run only against the local demo using SimulatedProvider.'
  exit 2
fi

base_url=${BASE_URL:-http://127.0.0.1:18080}
api_key=${API_KEY:-key-acme}
context=${KUBE_CONTEXT:-kind-relaynest}
namespace=notification-service
run_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
evidence_tmp=$(mktemp -d)
trap 'rm -f "$evidence_tmp/before.prom" "$evidence_tmp/after.prom" "$evidence_tmp/response.json"; rmdir "$evidence_tmp"' EXIT
metrics_auth=(-H "X-Management-Token: ${MANAGEMENT_API_TOKEN:-}")

kubectl --context "$context" -n "$namespace" rollout status deployment/notification-service --timeout=120s
kubectl --context "$context" -n "$namespace" get pods -l app.kubernetes.io/name=notification-service \
  -o 'custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount,IMAGE:.spec.containers[0].image,IMAGE_ID:.status.containerStatuses[0].imageID'

for endpoint in health health/liveness health/readiness; do
  result=$(curl -fsS --max-time 15 "$base_url/actuator/$endpoint")
  jq -e '.status == "UP"' <<< "$result" >/dev/null
  printf '%s %s\n' "$endpoint" "$result"
done

code=$(curl -sS --max-time 15 -o /dev/null -w '%{http_code}' "$base_url/notifications")
[[ $code == 401 ]]
echo 'Unauthenticated notification access: 401'
curl -fsS --max-time 15 "${metrics_auth[@]}" "$base_url/actuator/prometheus" > "$evidence_tmp/before.prom"

ids=()
verify_notification() {
  local recipient=$1 expected=$2 attempts=$3 body code id result status
  body=$(jq -nc --arg recipient "$recipient" '{recipient:$recipient,channel:"EMAIL",message:"DevOps verification"}')
  code=$(curl -sS --max-time 15 -o "$evidence_tmp/response.json" -w '%{http_code}' \
    -H 'Content-Type: application/json' -H "X-API-Key: $api_key" \
    -H "Idempotency-Key: $run_id-$expected-$attempts" --data "$body" "$base_url/notify")
  [[ $code == 202 ]]
  id=$(jq -er '.id' "$evidence_tmp/response.json")
  ids+=("$id")
  printf 'POST %s HTTP %s id=%s\n' "$recipient" "$code" "$id"
  for ((poll=0; poll<30; poll++)); do
    result=$(curl -fsS --max-time 15 -H "X-API-Key: $api_key" "$base_url/notify/$id")
    status=$(jq -r '.status' <<< "$result")
    if [[ $status == SENT || $status == DEAD ]]; then break; fi
    sleep 2
  done
  jq -e --arg expected "$expected" --argjson attempts "$attempts" \
    '.status == $expected and .attempts == $attempts' <<< "$result" >/dev/null
  jq '{id,status,attempts,failureReason,createdAt,sentAt}' <<< "$result"
}

verify_notification evidence@example.invalid SENT 1
verify_notification flaky@example.com SENT 3
verify_notification fail@example.com DEAD 4

curl -fsS --max-time 15 "${metrics_auth[@]}" "$base_url/actuator/prometheus" > "$evidence_tmp/after.prom"
metric_total() {
  awk -v metric="$1" '$1 ~ "^" metric "(\\{|$)" {sum += $2} END {print sum+0}' "$2"
}
for item in notifications_submitted_total:3 notifications_sent_total:2 notifications_failed_attempt_total:6 notifications_dead_total:1; do
  metric=${item%:*}
  minimum=${item#*:}
  before=$(metric_total "$metric" "$evidence_tmp/before.prom")
  after=$(metric_total "$metric" "$evidence_tmp/after.prom")
  awk -v metric="$metric" -v before="$before" -v after="$after" -v minimum="$minimum" \
    'BEGIN {printf "%s before=%g after=%g delta=%g\n",metric,before,after,after-before; exit (after-before < minimum)}'
done
grep -E '^(notifications_(delivery|end_to_end)_latency_seconds_(count|sum)|http_server_requests_seconds_count)' "$evidence_tmp/after.prom"
pattern=$(IFS='|'; echo "${ids[*]}")
kubectl --context "$context" -n "$namespace" logs deployment/notification-service --since=5m | grep -E "$pattern"
echo 'PASS: health, auth, success, retry recovery, permanent failure, metrics and matching logs'
