#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
context=kind-relaynest
namespace=notification-service
deployment=notification-service
image_name=ghcr.io/kananarora1/notification-service
agent_deployment=incident-agent
agent_image_name=ghcr.io/kananarora1/incident-agent
remote=https://github.com/kananarora1/NotificationService.git
state_dir="$HOME/Library/Application Support/RelayNest/kind-deployer"
template="$script_dir/deployment-template.yaml"
if [[ ! -f "$template" ]]; then
  template="$script_dir/../k8s/deployment-template.yaml"
fi
agent_template="$script_dir/incident-agent-deployment-template.yaml"
if [[ ! -f "$agent_template" ]]; then
  agent_template="$script_dir/../k8s/incident-agent-deployment-template.yaml"
fi
manifest_dir="$script_dir/k8s"
if [[ ! -f "$manifest_dir/kustomization.yaml" ]]; then
  manifest_dir="$script_dir/../k8s"
fi
[[ -f "$template" ]] || { echo 'Missing deployment template' >&2; exit 1; }
[[ -f "$agent_template" ]] || { echo 'Missing incident agent template' >&2; exit 1; }
[[ -f "$manifest_dir/kustomization.yaml" ]] || { echo 'Missing Kubernetes manifests' >&2; exit 1; }

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
agent_template_checksum=$(shasum -a 256 "$agent_template" | awk '{print $1}')

if ! kind get clusters | grep -qx relaynest; then
  echo 'kind cluster relaynest is not running; will retry later'
  exit 0
fi

kubectl --context "$context" -n "$namespace" get secret notification-service-secrets -o name >/dev/null
kubectl --context "$context" -n "$namespace" get secret alertmanager-gmail -o name >/dev/null
kubectl --context "$context" -n "$namespace" get secret incident-agent-webhook -o name >/dev/null
had_previous=false
had_previous_agent=false
current=
current_agent=
deployed_template_checksum=
deployed_agent_template_checksum=
if kubectl --context "$context" -n "$namespace" get deployment "$deployment" -o name >/dev/null 2>&1; then
  had_previous=true
  current=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
    -o jsonpath='{.spec.template.metadata.labels.app\.kubernetes\.io/version}')
  deployed_template_checksum=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
    -o jsonpath='{.metadata.annotations.relaynest\.tech/release-template-sha256}')
fi
if kubectl --context "$context" -n "$namespace" get deployment "$agent_deployment" -o name >/dev/null 2>&1; then
  had_previous_agent=true
  current_agent=$(kubectl --context "$context" -n "$namespace" get deployment "$agent_deployment" \
    -o jsonpath='{.spec.template.metadata.labels.app\.kubernetes\.io/version}')
  deployed_agent_template_checksum=$(kubectl --context "$context" -n "$namespace" get deployment "$agent_deployment" \
    -o jsonpath='{.metadata.annotations.relaynest\.tech/release-template-sha256}')
fi
if [[ $current == "$sha" && $deployed_template_checksum == "$template_checksum" && \
      $current_agent == "$sha" && $deployed_agent_template_checksum == "$agent_template_checksum" ]]; then
  echo "Already deployed $sha"
  exit 0
fi

if [[ -f "$state_dir/$sha.failed" ]]; then
  echo "Skipping $sha after a failed rollout; remove $state_dir/$sha.failed to retry"
  exit 1
fi

image="$image_name:$sha"
agent_image="$agent_image_name:$sha"
platform=$(kubectl --context "$context" get nodes \
  -o jsonpath='{.items[0].status.nodeInfo.operatingSystem}/{.items[0].status.nodeInfo.architecture}')
for release_image in "$image" "$agent_image"; do
  if ! docker pull --platform "$platform" "$release_image"; then
    echo "CI image $release_image is not available yet; will retry later"
    exit 0
  fi
  revision=$(docker image inspect "$release_image" \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
  [[ $revision == "$sha" ]] || { echo "Image revision mismatch: $revision" >&2; exit 1; }
done

bash "$script_dir/load-kind-image.sh" "$image" relaynest
bash "$script_dir/load-kind-image.sh" "$agent_image" relaynest
kubectl --context "$context" apply -k "$manifest_dir"
for component in prometheus alertmanager; do
  config_checksum=$(shasum -a 256 "$manifest_dir/$component.yaml" | awk '{print $1}')
  current_checksum=$(kubectl --context "$context" -n "$namespace" get deployment "$component" \
    -o jsonpath='{.spec.template.metadata.annotations.relaynest\.tech/config-sha256}')
  if [[ $current_checksum != "$config_checksum" ]]; then
    kubectl --context "$context" -n "$namespace" patch deployment "$component" --type=merge \
      -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"relaynest.tech/config-sha256\":\"$config_checksum\"}}}}}"
  fi
done
sed -e "s/RELEASE_SHA/$sha/g" -e "s/TEMPLATE_SHA256/$template_checksum/g" "$template" |
  kubectl --context "$context" -n "$namespace" apply -f -
sed -e "s/RELEASE_SHA/$sha/g" -e "s/TEMPLATE_SHA256/$agent_template_checksum/g" "$agent_template" |
  kubectl --context "$context" -n "$namespace" apply -f -
if ! kubectl --context "$context" -n "$namespace" rollout status \
    "deployment/$deployment" --timeout=180s || \
   ! kubectl --context "$context" -n "$namespace" rollout status \
    "deployment/$agent_deployment" --timeout=180s || \
   ! kubectl --context "$context" -n "$namespace" rollout status \
    deployment/prometheus --timeout=180s || \
   ! kubectl --context "$context" -n "$namespace" rollout status \
    deployment/alertmanager --timeout=180s; then
  echo "Rollout failed for $sha; restoring previous revision" >&2
  touch "$state_dir/$sha.failed"
  if [[ $had_previous == true && ( $current != "$sha" || $deployed_template_checksum != "$template_checksum" ) ]]; then
    kubectl --context "$context" -n "$namespace" rollout undo "deployment/$deployment"
    kubectl --context "$context" -n "$namespace" rollout status \
      "deployment/$deployment" --timeout=180s
  fi
  if [[ $had_previous_agent == true && ( $current_agent != "$sha" || $deployed_agent_template_checksum != "$agent_template_checksum" ) ]]; then
    kubectl --context "$context" -n "$namespace" rollout undo "deployment/$agent_deployment"
    kubectl --context "$context" -n "$namespace" rollout status \
      "deployment/$agent_deployment" --timeout=180s
  fi
  exit 1
fi

deployed=$(kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')
[[ $deployed == "$image" ]] || { echo "Deployed image mismatch: $deployed" >&2; exit 1; }
deployed_agent=$(kubectl --context "$context" -n "$namespace" get deployment "$agent_deployment" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')
[[ $deployed_agent == "$agent_image" ]] || { echo "Deployed agent image mismatch: $deployed_agent" >&2; exit 1; }
echo "Deployed $image and $agent_image"
