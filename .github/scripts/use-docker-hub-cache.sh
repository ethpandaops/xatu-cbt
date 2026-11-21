#!/usr/bin/env bash
#
# Routes this runner's Docker Hub pulls through the ethquokkaops pull-through
# cache, which is anonymous, so tests need no Docker Hub credentials. Covers
# pulls by docker compose, `docker pull` and `docker run`, whose image names
# can't take a registry prefix.
#
# dockerd's registry-mirrors setting can't point at the cache: it serves Docker
# Hub under a path (docker.ethquokkaops.io/dh/<namespace>/<image>), and dockerd
# only uses mirrors at a registry's root. The containerd image store (Docker's
# default since 29) reads hosts.toml on every pull, so no restart is needed.
# If the cache can't serve an image, the pull falls back to Docker Hub.
#
# Usage:
#   ./.github/scripts/use-docker-hub-cache.sh

set -euo pipefail

if ! docker info 2>/dev/null | grep -q 'io.containerd.snapshotter'; then
    echo "::warning::dockerd isn't using the containerd image store, so Docker Hub pulls won't go through docker.ethquokkaops.io"
fi

sudo mkdir -p /etc/docker/certs.d/docker.io
sudo tee /etc/docker/certs.d/docker.io/hosts.toml >/dev/null <<'EOF'
server = "https://registry-1.docker.io"

[host."https://docker.ethquokkaops.io/v2/dh"]
  capabilities = ["pull", "resolve"]
  override_path = true
EOF
