#!/usr/bin/env bash
#
# Detect which test models are impacted by changes in a PR.
# Outputs either "all" (run full suite) or a comma-separated list of model names.
#
# Usage:
#   ./scripts/detect_impacted_models.sh --network <network> [--base <base_ref>]
#
# Examples:
#   ./scripts/detect_impacted_models.sh --network mainnet
#   ./scripts/detect_impacted_models.sh --network sepolia --base origin/master
#

set -euo pipefail

NETWORK=""
BASE_REF="origin/master"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --network) NETWORK="$2"; shift 2 ;;
        --base) BASE_REF="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$NETWORK" ]]; then
    echo "Usage: $0 --network <network> [--base <base_ref>]" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MODELS_DIR="$REPO_ROOT/models"
TESTS_DIR="$REPO_ROOT/tests/$NETWORK/models"

if [[ ! -d "$TESTS_DIR" ]]; then
    echo "none"
    exit 0
fi

# Get changed files relative to base
CHANGED_FILES=$(git -C "$REPO_ROOT" diff --name-only "$BASE_REF"...HEAD 2>/dev/null || \
                git -C "$REPO_ROOT" diff --name-only "$BASE_REF" HEAD 2>/dev/null || \
                echo "")

if [[ -z "$CHANGED_FILES" ]]; then
    echo "all"
    exit 0
fi

# Check for non-model changes that should trigger a full run
HAS_NON_MODEL_CHANGES=false
while IFS= read -r file; do
    case "$file" in
        models/external/*|models/transformations/*) ;; # model files - handled below
        tests/*)                                       ;; # test files - handled below
        overrides*.yaml)                               ;; # overrides - handled below
        .github/*)                                     ;; # CI files - don't affect models
        *.md)                                          ;; # docs - don't affect models
        *)
            HAS_NON_MODEL_CHANGES=true
            break
            ;;
    esac
done <<< "$CHANGED_FILES"

if [[ "$HAS_NON_MODEL_CHANGES" == "true" ]]; then
    echo "all"
    exit 0
fi

# Collect directly changed model names
declare -A CHANGED_MODELS

while IFS= read -r file; do
    case "$file" in
        models/external/*)
            model=$(basename "$file" | sed 's/\.[^.]*$//')
            CHANGED_MODELS["$model"]=1
            ;;
        models/transformations/*)
            model=$(basename "$file" | sed 's/\.[^.]*$//')
            CHANGED_MODELS["$model"]=1
            ;;
        tests/*/models/*)
            model=$(basename "$file" | sed 's/\.[^.]*$//')
            CHANGED_MODELS["$model"]=1
            ;;
        overrides*.yaml)
            # Parse overrides to find which models are affected
            # For simplicity, trigger all tests if overrides change
            echo "all"
            exit 0
            ;;
    esac
done <<< "$CHANGED_FILES"

if [[ ${#CHANGED_MODELS[@]} -eq 0 ]]; then
    echo "all"
    exit 0
fi

# Build reverse dependency graph: for each model, find all models that depend on it
# Parse dependencies from transformation model frontmatter
declare -A REVERSE_DEPS

for model_file in "$MODELS_DIR"/transformations/*.sql "$MODELS_DIR"/transformations/*.yml; do
    [[ -f "$model_file" ]] || continue
    model_name=$(basename "$model_file" | sed 's/\.[^.]*$//')

    # Extract dependencies from YAML frontmatter (between --- markers for .sql, or directly for .yml)
    in_frontmatter=false
    in_deps=false
    while IFS= read -r line; do
        if [[ "$line" == "---" ]]; then
            if [[ "$in_frontmatter" == "true" ]]; then
                break # end of frontmatter
            fi
            in_frontmatter=true
            continue
        fi

        # For .yml files, no --- delimiters needed
        if [[ "$model_file" == *.yml ]]; then
            in_frontmatter=true
        fi

        if [[ "$line" =~ ^dependencies: ]]; then
            in_deps=true
            continue
        fi

        if [[ "$in_deps" == "true" ]]; then
            # Check if line is a dependency entry (starts with -)
            if [[ "$line" =~ ^[[:space:]]*-[[:space:]] ]]; then
                # Extract the table name from patterns like:
                # - "{{external}}.canonical_beacon_block"
                # - "{{transformation}}.int_block_canonical"
                dep_name=$(echo "$line" | sed -E 's/.*\{\{(external|transformation)\}\}\.([a-zA-Z0-9_]+).*/\2/')
                if [[ -n "$dep_name" && "$dep_name" != "$line" ]]; then
                    # Add to reverse deps: dep_name -> model_name depends on it
                    if [[ -n "${REVERSE_DEPS[$dep_name]:-}" ]]; then
                        REVERSE_DEPS["$dep_name"]="${REVERSE_DEPS[$dep_name]} $model_name"
                    else
                        REVERSE_DEPS["$dep_name"]="$model_name"
                    fi
                fi
            else
                in_deps=false
            fi
        fi
    done < "$model_file"
done

# BFS to find all downstream impacted models
declare -A IMPACTED
queue=()
for model in "${!CHANGED_MODELS[@]}"; do
    IMPACTED["$model"]=1
    queue+=("$model")
done

while [[ ${#queue[@]} -gt 0 ]]; do
    current="${queue[0]}"
    queue=("${queue[@]:1}")

    if [[ -n "${REVERSE_DEPS[$current]:-}" ]]; then
        for dependent in ${REVERSE_DEPS[$current]}; do
            if [[ -z "${IMPACTED[$dependent]:-}" ]]; then
                IMPACTED["$dependent"]=1
                queue+=("$dependent")
            fi
        done
    fi
done

# Filter to only models that have test files in the target network
TESTABLE=()
for model in "${!IMPACTED[@]}"; do
    if [[ -f "$TESTS_DIR/$model.yaml" ]]; then
        TESTABLE+=("$model")
    fi
done

if [[ ${#TESTABLE[@]} -eq 0 ]]; then
    echo "none"
    exit 0
fi

# Sort for deterministic output
IFS=$'\n' SORTED=($(sort <<< "${TESTABLE[*]}")); unset IFS

# Join with commas
RESULT=$(IFS=,; echo "${SORTED[*]}")
echo "$RESULT"
