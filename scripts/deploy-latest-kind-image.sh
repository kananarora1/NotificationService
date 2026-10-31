#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
context=kind-relaynest
namespace=notification-service
deployment=notification-service
image_name=ghcr.io/kananarora1/notification-service
remote=https://github.com/kananarora1/NotificationService.git
state_dir="$HOME/Library/Application Support/RelayNest/kind-deployer"
template="$script_dir/deployment-template.yaml"
if [[ ! -f "$template" ]]; then
  template="$script_dir/../k8s/deployment-template.yaml"
fi
[[ -f "$template" ]] || { echo 'Missing deployment template' >&2; exit 1; }

for tool in git docker kind kubectl shasum; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }
done

install -d -m 700 "$state_dir"
if [[ -f "$state_dir/paused" ]]; then
  echo 'Automatic kind deployment is paused'
  exit 0
fi

sha=$(GIT_TERMINAL_PROMPT=0 git ls-remote "$remote" refs/heads/main | awk '{print $1}')
[[ $sha =~ ^[0-9a-f]{40}$ ]] || { echo 'Could not resolve origin/main' >&2; exit 1; }
template_checksum=$(shasum -a 256 "$template" | awk '{print $1}')

if ! kind get clusters | grep -qx relaynest; then
  echo 'kind cluster relaynest is not running; will retry later'
  exit 0
fi

kubectl --context "$context" -n "$namespace" get secret notification-service-secrets -o name >/dev/null
had_previous=false
current=
deployed_template_checksum=
if kubectl --context "$context" -n "$namespace" get deployment "$deployment" -o name >/dev/null 2>&1; then
  had_previous=true
  current=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
    -o jsonpath='{.spec.template.metadata.labels.app\.kubernetes\.io/version}')
  deployed_template_checksum=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
    -o jsonpath='{.metadata.annotations.relaynest\.tech/release-template-sha256}')
fi
if [[ $current == "$sha" && $deployed_template_checksum == "$template_checksum" ]]; then
  echo "Already deployed $sha"
  exit 0
fi

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
sed -e "s/RELEASE_SHA/$sha/g" -e "s/TEMPLATE_SHA256/$template_checksum/g" "$template" |
  kubectl --context "$context" -n "$namespace" apply -f -
if ! kubectl --context "$context" -n "$namespace" rollout status \
  "deployment/$deployment" --timeout=180s; then
  echo "Rollout failed for $sha; restoring previous revision" >&2
  touch "$state_dir/$sha.failed"
  if [[ $had_previous == true ]]; then
    kubectl --context "$context" -n "$namespace" rollout undo "deployment/$deployment"
    kubectl --context "$context" -n "$namespace" rollout status \
      "deployment/$deployment" --timeout=180s
  fi
  exit 1
fi

deployed=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')
[[ $deployed == "$image" ]] || { echo "Deployed image mismatch: $deployed" >&2; exit 1; }
echo "Deployed $image"
