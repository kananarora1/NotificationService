#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
context=kind-relaynest
namespace=notification-service
deployment=notification-service
image_name=ghcr.io/kananarora1/notification-service
remote=https://github.com/kananarora1/NotificationService.git
state_dir="$HOME/Library/Application Support/RelayNest/kind-deployer"

for tool in git docker kind kubectl jq; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }
done

sha=$(GIT_TERMINAL_PROMPT=0 git ls-remote "$remote" refs/heads/main | awk '{print $1}')
[[ $sha =~ ^[0-9a-f]{40}$ ]] || { echo 'Could not resolve origin/main' >&2; exit 1; }

if ! kind get clusters | grep -qx relaynest; then
  echo 'kind cluster relaynest is not running; will retry later'
  exit 0
fi

kubectl --context "$context" -n "$namespace" get secret notification-service-secrets -o name >/dev/null
current=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
  -o jsonpath='{.spec.template.metadata.labels.app\.kubernetes\.io/version}')
if [[ $current == "$sha" ]]; then
  echo "Already deployed $sha"
  exit 0
fi

install -d -m 700 "$state_dir"
if [[ -f "$state_dir/$sha.failed" ]]; then
  echo "Skipping $sha after a failed rollout; remove $state_dir/$sha.failed to retry"
  exit 1
fi

image="$image_name:$sha"
platform=$(kubectl --context "$context" get nodes \
  -o jsonpath='{.items[0].status.nodeInfo.operatingSystem}/{.items[0].status.nodeInfo.architecture}')
if ! docker pull --platform "$platform" "$image"; then
  echo "CI image $image is not available yet; will retry later"
  exit 0
fi
revision=$(docker image inspect "$image" \
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
[[ $revision == "$sha" ]] || { echo "Image revision mismatch: $revision" >&2; exit 1; }

bash "$script_dir/load-kind-image.sh" "$image" relaynest
patch=$(jq -nc --arg image "$image" --arg version "$sha" '{
  metadata: {
    labels: {"app.kubernetes.io/version": $version},
    annotations: {"kubernetes.io/change-cause": ("Deploy " + $image)}
  },
  spec: {template: {
    metadata: {labels: {"app.kubernetes.io/version": $version}},
    spec: {containers: [{name: "notification-service", image: $image}]}
  }}
}')

kubectl --context "$context" -n "$namespace" patch deployment "$deployment" \
  --type strategic -p "$patch"
if ! kubectl --context "$context" -n "$namespace" rollout status \
  "deployment/$deployment" --timeout=180s; then
  echo "Rollout failed for $sha; restoring previous revision" >&2
  touch "$state_dir/$sha.failed"
  kubectl --context "$context" -n "$namespace" rollout undo "deployment/$deployment"
  kubectl --context "$context" -n "$namespace" rollout status \
    "deployment/$deployment" --timeout=180s
  exit 1
fi

deployed=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')
[[ $deployed == "$image" ]] || { echo "Deployed image mismatch: $deployed" >&2; exit 1; }
echo "Deployed $image"
