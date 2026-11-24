#!/usr/bin/env bash
# Create a release/<network> branch for every Ethereum devnet that lacks one.
#
# A devnet's network name is <class>-<dir>: <class> is the repository name
# <org>/<class>-devnets without "-devnets", and <dir> is each directory under
# kubernetes/ on that repository's default branch (glamsterdam-devnets with
# kubernetes/devnet-8 gives glamsterdam-devnet-8). template-devnets and archived
# repositories are skipped.
#
# Each missing branch is created at the current tip of BASE_BRANCH through the
# REST API. Creating a ref fails when it already exists, so an existing branch
# is never moved, rewritten or deleted. Pushing the new branch runs
# .github/workflows/docker.yml, which publishes the release-<network> image.
#
# Environment:
#   READ_TOKEN    token that lists ORG's repositories and reads their contents
#                 (default: GH_TOKEN, then `gh auth token`)
#   WRITE_TOKEN   token with contents: write on TARGET_REPO; its pushes must
#                 trigger workflows, so not the Actions GITHUB_TOKEN
#                 (default: READ_TOKEN). Unused in dry-run mode.
#   ORG           default ethpandaops
#   TARGET_REPO   default ethpandaops/xatu-cbt
#   BASE_BRANCH   default master
#   DRY_RUN       "true" only reports the branches it would create
#
# Usage: devnet-release-branches.sh
set -euo pipefail

ORG="${ORG:-ethpandaops}"
TARGET_REPO="${TARGET_REPO:-ethpandaops/xatu-cbt}"
BASE_BRANCH="${BASE_BRANCH:-master}"
DRY_RUN="${DRY_RUN:-false}"
READ_TOKEN="${READ_TOKEN:-${GH_TOKEN:-$(gh auth token)}}"
WRITE_TOKEN="${WRITE_TOKEN:-$READ_TOKEN}"

SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"
failed=0

warn() {
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then echo "::warning::$*"; else echo "warning: $*" >&2; fi
}
error() {
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then echo "::error::$*"; else echo "error: $*" >&2; fi
  failed=1
}
read_api() { GH_TOKEN="$READ_TOKEN" gh api "$@"; }

# Discover networks.
repos=$(read_api --paginate "orgs/${ORG}/repos?type=all&per_page=100" \
  --jq '.[] | select(.archived | not) | select(.name | endswith("-devnets")) | select(.name != "template-devnets") | .name' | sort)

networks=()
for repo in $repos; do
  class="${repo%-devnets}"
  if ! out=$(read_api "repos/${ORG}/${repo}/contents/kubernetes" --jq '.[] | select(.type == "dir") | .name' 2>&1); then
    # No kubernetes/ directory: this repository has no networks.
    if [[ $out == *"HTTP 404"* ]]; then continue; fi
    error "reading ${ORG}/${repo}/kubernetes: ${out}"
    continue
  fi
  for dir in $out; do
    network="${class}-${dir}"
    # Must be a valid branch name and Docker tag (release-<network>).
    if ! [[ $network =~ ^[a-z0-9][a-z0-9._-]{0,99}$ ]]; then
      warn "skipping ${ORG}/${repo} kubernetes/${dir}: ${network} is not a valid network name"
      continue
    fi
    networks+=("$network")
  done
done

# Find the networks without a release branch.
existing=$(read_api --paginate "repos/${TARGET_REPO}/git/matching-refs/heads/release/" --jq '.[].ref')
base_sha=$(read_api "repos/${TARGET_REPO}/git/ref/heads/${BASE_BRANCH}" --jq '.object.sha')

echo "Networks: ${networks[*]:-none}"
echo "${BASE_BRANCH} is at ${base_sha}"
{
  echo "### Devnet release branches (dry run: ${DRY_RUN})"
  echo
  echo "Base: \`${BASE_BRANCH}\` at \`${base_sha}\`"
  echo
} >> "$SUMMARY"

for network in "${networks[@]}"; do
  branch="release/${network}"
  if grep -qxF "refs/heads/${branch}" <<<"$existing"; then
    echo "exists: ${branch}"
    echo "- exists: \`${branch}\`" >> "$SUMMARY"
    continue
  fi
  if [ "$DRY_RUN" = "true" ]; then
    echo "would create: ${branch}"
    echo "- would create: \`${branch}\`" >> "$SUMMARY"
    continue
  fi
  # Creating a ref that already exists fails (HTTP 422) instead of moving it.
  if out=$(GH_TOKEN="$WRITE_TOKEN" gh api -X POST "repos/${TARGET_REPO}/git/refs" \
    -f "ref=refs/heads/${branch}" -f "sha=${base_sha}" --jq '.ref' 2>&1); then
    echo "created: ${branch}"
    echo "- created: \`${branch}\`" >> "$SUMMARY"
  else
    error "creating ${branch}: ${out}"
    echo "- failed: \`${branch}\`" >> "$SUMMARY"
  fi
done

exit "$failed"
