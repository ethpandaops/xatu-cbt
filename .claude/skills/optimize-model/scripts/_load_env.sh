#!/usr/bin/env bash
# Load the project .env so optimize-model picks up ClickHouse credentials (and
# any endpoint/database overrides) without manual exports.
#
# Precedence: already-exported environment variables WIN over .env. This means a
# per-invocation override such as `TRANSFORM_PASS=secret ./run_benchmark.sh ...`
# still takes effect even if .env also defines TRANSFORM_PASS.
#
# Source this near the top of a wrapper, after SCRIPT_DIR is defined:
#   . "$SCRIPT_DIR/_load_env.sh"
#
# Override the file location with ENV_FILE=/path/to/.env if needed.
# Compatible with Bash 3.2+ (macOS default bash).

# Resolve repo root: scripts/ -> optimize-model/ -> skills/ -> .claude/ -> repo
__om_env_file="${ENV_FILE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)/.env}"

if [ -f "$__om_env_file" ]; then
  while IFS= read -r __om_line || [ -n "$__om_line" ]; do
    # Skip blanks and comments; require KEY=VALUE form.
    case "$__om_line" in
      ''|'#'*) continue ;;
      *=*) : ;;
      *) continue ;;
    esac
    # Strip an optional leading "export ".
    case "$__om_line" in
      'export '*) __om_line="${__om_line#export }" ;;
    esac
    __om_key="${__om_line%%=*}"
    # Only accept valid shell identifiers (guards the eval below).
    case "$__om_key" in
      ''|[!A-Za-z_]*|*[!A-Za-z0-9_]*) continue ;;
    esac
    # Only set if the variable is not already present in the environment.
    eval "__om_cur=\${$__om_key+x}"
    if [ -z "${__om_cur:-}" ]; then
      export "$__om_line"
    fi
  done < "$__om_env_file"
  unset __om_line __om_key __om_cur
fi
unset __om_env_file
