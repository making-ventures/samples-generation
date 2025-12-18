#!/usr/bin/env bash
# Runs Renovate locally and shows a readable summary of available updates.
# Exits with code 1 if any outdated dependencies are found.
set -euo pipefail

echo "[renovate-check] Checking for dependency updates..."
echo ""

# Run renovate with JSON output and debug level to get version info
OUTPUT=$(LOG_FORMAT=json LOG_LEVEL=debug npx -y renovate --platform=local --dry-run --require-config=ignored 2>&1 || true)

# Extract the packageFiles message which contains all dependency info
PACKAGES_JSON=$(echo "$OUTPUT" | grep '"msg":"packageFiles with updates"' | head -1)

if [[ -z "$PACKAGES_JSON" ]]; then
  echo "⚠ Could not parse Renovate output"
  exit 1
fi

# Parse updates using jq - include datasource for proper categorization
UPDATES=$(echo "$PACKAGES_JSON" | jq -r '
  .config | to_entries[] | .value[] | .deps[] |
  select(.updates | length > 0) |
  .updates[] as $update |
  "\(.datasource)|\(.depName)|\(.currentVersion)|\($update.newVersion)"
' 2>/dev/null || true)

if [[ -z "$UPDATES" ]]; then
  echo "✓ All dependencies are up to date!"
  exit 0
fi

echo "Outdated dependencies:"
echo ""

# Docker images: datasource = "docker"
DOCKER_UPDATES=$(echo "$UPDATES" | grep "^docker|" || true)
if [[ -n "$DOCKER_UPDATES" ]]; then
  echo "📦 Docker images:"
  echo "$DOCKER_UPDATES" | while read -r line; do
    DEP=$(echo "$line" | cut -d'|' -f2)
    CURRENT=$(echo "$line" | cut -d'|' -f3)
    NEW=$(echo "$line" | cut -d'|' -f4)
    echo "  ✗ $DEP: $CURRENT → $NEW"
  done
  echo ""
fi

# NPM packages: datasource = "npm"
NPM_UPDATES=$(echo "$UPDATES" | grep "^npm|" || true)
if [[ -n "$NPM_UPDATES" ]]; then
  echo "📦 NPM packages:"
  echo "$NPM_UPDATES" | while read -r line; do
    DEP=$(echo "$line" | cut -d'|' -f2)
    CURRENT=$(echo "$line" | cut -d'|' -f3)
    NEW=$(echo "$line" | cut -d'|' -f4)
    echo "  ✗ $DEP: $CURRENT → $NEW"
  done
  echo ""
fi

# Other datasources (if any)
OTHER_UPDATES=$(echo "$UPDATES" | grep -vE "^(docker|npm)\|" || true)
if [[ -n "$OTHER_UPDATES" ]]; then
  echo "📦 Other:"
  echo "$OTHER_UPDATES" | while read -r line; do
    DS=$(echo "$line" | cut -d'|' -f1)
    DEP=$(echo "$line" | cut -d'|' -f2)
    CURRENT=$(echo "$line" | cut -d'|' -f3)
    NEW=$(echo "$line" | cut -d'|' -f4)
    echo "  ✗ [$DS] $DEP: $CURRENT → $NEW"
  done
  echo ""
fi

OUTDATED_COUNT=$(echo "$UPDATES" | wc -l)

echo "Found $OUTDATED_COUNT outdated dependencies"
exit 1
