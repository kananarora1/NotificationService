#!/usr/bin/env bash
set -euo pipefail

image=${1:?Usage: bash scripts/load-kind-image.sh IMAGE [CLUSTER]}
cluster=${2:-relaynest}
platform=$(kubectl --context "kind-$cluster" get nodes -o jsonpath='{.items[0].status.nodeInfo.operatingSystem}/{.items[0].status.nodeInfo.architecture}')
docker pull --platform "$platform" "$image"

# Export only the cluster's architecture.
while IFS= read -r node; do
  docker image save --platform "$platform" "$image" |
    docker exec -i "$node" ctr --namespace=k8s.io images import --all-platforms --digests --snapshotter=overlayfs -
done < <(kind get nodes --name "$cluster")

docker image inspect "$image" --format '{{.Os}}/{{.Architecture}} revision={{index .Config.Labels "org.opencontainers.image.revision"}}'
