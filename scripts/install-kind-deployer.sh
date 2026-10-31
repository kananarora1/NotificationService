#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
label=com.relaynest.kind-deployer
plist="$HOME/Library/LaunchAgents/$label.plist"
log_file="$HOME/Library/Logs/relaynest-kind-deployer.log"
install_dir="$HOME/Library/Application Support/RelayNest/kind-deployer"
path=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

mkdir -p "$(dirname "$plist")" "$(dirname "$log_file")"
install -d -m 700 "$install_dir"
install -m 700 "$repo_dir/scripts/deploy-latest-kind-image.sh" "$install_dir/deploy-latest-kind-image.sh"
install -m 700 "$repo_dir/scripts/load-kind-image.sh" "$install_dir/load-kind-image.sh"
install -m 600 "$repo_dir/k8s/deployment-template.yaml" "$install_dir/deployment-template.yaml"
launchctl bootout "gui/$(id -u)" "$plist" 2>/dev/null || true

plutil -create xml1 "$plist"
plutil -insert Label -string "$label" "$plist"
plutil -insert ProgramArguments -json \
  "$(jq -nc --arg script "$install_dir/deploy-latest-kind-image.sh" '["/bin/bash", $script]')" "$plist"
plutil -insert WorkingDirectory -string "$install_dir" "$plist"
plutil -insert RunAtLoad -bool YES "$plist"
plutil -insert StartInterval -integer 120 "$plist"
plutil -insert StandardOutPath -string "$log_file" "$plist"
plutil -insert StandardErrorPath -string "$log_file" "$plist"
plutil -insert EnvironmentVariables -json \
  "$(jq -nc --arg path "$path" '{PATH: $path}')" "$plist"
plutil -lint "$plist"
launchctl bootstrap "gui/$(id -u)" "$plist"
echo "Installed $label; log: $log_file"
