#!/usr/bin/env bash
# Create a release/<network> branch for every Ethereum devnet that lacks one.
#
# Each directory <dir> under kubernetes/ on the default branch of a repository
# <org>/<class>-devnets is a network named <class>-<dir>, or <dir> when it
# already starts with "<class>-": glamsterdam-devnets/kubernetes/devnet-8 is
# glamsterdam-devnet-8 and pk-devnets/kubernetes/pk-devnet-0 is pk-devnet-0.
# template-devnets and archived repositories are skipped.
#
# Each missing branch is created at the current tip of BASE_BRANCH through the
# REST API. Creating a ref fails when it already exists, so an existing branch
# is never moved, rewritten or deleted. The script then dispatches
# .github/workflows/docker.yml on the new branch, which publishes the
# release-<network> image: a branch created with the Actions GITHUB_TOKEN
# doesn't start push workflows, but a workflow_dispatch from it does.
#
# gh authenticates as usual (GH_TOKEN in Actions). Creating needs contents:
# write and dispatching actions: write on TARGET_REPO.
#
# Environment:
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

SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"
failed=0

warn() {
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then echo "::warning::$*"; else echo "warning: $*" >&2; fi
}
error() {
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then echo "::error::$*"; else echo "error: $*" >&2; fi
  failed=1
}
# TEMPORARY test hook.
if [ -n "${NETWORKS:-}" ]; then
  repos=""
  read -ra networks <<<"$NETWORKS"
else
networks=()
# Discover networks.
repos=$(gh api --paginate "orgs/${ORG}/repos?type=all&per_page=100" \
  --jq '.[] | select(.archived | not) | select(.name | endswith("-devnets")) | select(.name != "template-devnets") | .name' | sort)

for repo in $repos; do
  class="${repo%-devnets}"
  if ! out=$(gh api "repos/${ORG}/${repo}/contents/kubernetes" --jq '.[] | select(.type == "dir") | .name' 2>&1); then
    # No kubernetes/ directory: this repository has no networks.
    if [[ $out == *"HTTP 404"* ]]; then continue; fi
    error "reading ${ORG}/${repo}/kubernetes: ${out}"
    continue
  fi
  for dir in $out; do
    # Directories that already carry the class (pk-devnets/kubernetes/pk-devnet-0)
    # are the network name as is.
    if [[ $dir == "${class}-"* ]]; then network="$dir"; else network="${class}-${dir}"; fi
    # Must be a valid branch name and Docker tag (release-<network>).
    if ! [[ $network =~ ^[a-z0-9][a-z0-9._-]{0,99}$ ]]; then
      warn "skipping ${ORG}/${repo} kubernetes/${dir}: ${network} is not a valid network name"
      continue
    fi
    networks+=("$network")
  done
done
fi

# Find the networks without a release branch.
existing=$(gh api --paginate "repos/${TARGET_REPO}/git/matching-refs/heads/release/" --jq '.[].ref')
base_sha=$(gh api "repos/${TARGET_REPO}/git/ref/heads/${BASE_BRANCH}" --jq '.object.sha')

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
  if ! out=$(gh api -X POST "repos/${TARGET_REPO}/git/refs" \
    -f "ref=refs/heads/${branch}" -f "sha=${base_sha}" --jq '.ref' 2>&1); then
    error "creating ${branch}: ${out}"
    echo "- failed: \`${branch}\`" >> "$SUMMARY"
    continue
  fi
  echo "created: ${branch}"
  # Later runs skip the existing branch, so a failed dispatch needs a manual one.
  if ! out=$(gh workflow run docker.yml -R "$TARGET_REPO" --ref "$branch" 2>&1); then
    error "dispatching docker.yml on ${branch}: ${out}. Run: gh workflow run docker.yml -R ${TARGET_REPO} --ref ${branch}"
    echo "- created, build dispatch failed: \`${branch}\`" >> "$SUMMARY"
    continue
  fi
  echo "dispatched docker.yml on ${branch}"
  echo "- created and dispatched docker.yml: \`${branch}\`" >> "$SUMMARY"
done

exit "$failed"
