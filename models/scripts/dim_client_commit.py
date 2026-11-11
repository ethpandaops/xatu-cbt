#!/usr/bin/env python3
"""Maps every commit on each client's default and release branches to the release it builds on, like git describe,
so graffiti commits from builds between tags still resolve to a version (--dry-run prints a summary).

Fetches commits only (git protocol v2 with filter tree:0, no trees or blobs), a few MB per repository."""

import hashlib
import json
import os
import re
import sys
import urllib.request
import zlib
from datetime import datetime, timezone

from dim_client_release import CLIENTS, HTTP_TIMEOUT, USER_AGENT, VERSION_RE, clickhouse, pkt_line

# Branches builds come from besides the default one, e.g. release/1.17, release-v7.1, stable, hotfix/2.1.1.
RELEASE_BRANCH_RE = re.compile(r"^(release|releases|stable|hotfix|backport)([/_.-]|$)|^v?\d+\.\d+")
# Graffiti only exists since the beacon chain launched, so older commits are left out.
MIN_COMMIT_TIME = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
# A base tag committed this long before the commit is not the release it builds on: the branch carries no recent
# tags (erigon tags only release branches, so main describes to v3.0.0-beta1), so the commit has no base.
MAX_BASE_AGE = 120 * 86400


def upload_pack(repo, body):
    req = urllib.request.Request(
        f"https://github.com/{repo}.git/git-upload-pack",
        data=body,
        headers={
            "Git-Protocol": "version=2",
            "Content-Type": "application/x-git-upload-pack-request",
            "Accept": "application/x-git-upload-pack-result",
            "User-Agent": f"git/2.45.0 {USER_AGENT}",
        },
        method="POST",
    )
    return urllib.request.urlopen(req, timeout=HTTP_TIMEOUT * 10).read()


def pkt_lines(raw):
    """Yields pkt-line payloads, None for flush and delimiter packets."""
    offset = 0
    while offset < len(raw):
        size = int(raw[offset:offset + 4], 16)
        if size < 4:
            offset += 4
            yield None
            continue
        yield raw[offset + 4:offset + size]
        offset += size


def list_refs(repo):
    """Returns (default branch commit, {branch: commit}, {tag: peeled commit}) via ls-refs."""
    body = (pkt_line("command=ls-refs\n") + b"0001" + pkt_line("peel\n") + pkt_line("symrefs\n")
            + pkt_line("ref-prefix HEAD\n") + pkt_line("ref-prefix refs/heads/\n") + pkt_line("ref-prefix refs/tags/\n") + b"0000")
    head, branches, tags = None, {}, {}
    for line in pkt_lines(upload_pack(repo, body)):
        if line is None:
            continue
        fields = line.decode().rstrip("\n").split(" ")
        oid, name = fields[0], fields[1]
        peeled = next((f.split(":", 1)[1] for f in fields[2:] if f.startswith("peeled:")), oid)
        if name == "HEAD":
            head = oid
        elif name.startswith("refs/heads/"):
            branches[name[len("refs/heads/"):]] = oid
        elif name.startswith("refs/tags/"):
            tags[name[len("refs/tags/"):]] = peeled.lower()
    return head, branches, tags


def fetch_pack(repo, wants):
    """Fetches the commits reachable from `wants` without trees or blobs and returns the raw packfile."""
    body = pkt_line("command=fetch\n") + b"0001" + pkt_line("no-progress\n") + pkt_line("ofs-delta\n") + pkt_line("filter tree:0\n")
    for oid in sorted(wants):
        body += pkt_line(f"want {oid}\n")
    body += pkt_line("done\n") + b"0000"
    pack, in_pack = bytearray(), False
    for line in pkt_lines(upload_pack(repo, body)):
        if line is None:
            continue
        if not in_pack:
            in_pack = line.rstrip(b"\n") == b"packfile"
            continue
        if line[0] == 1:
            pack += line[1:]
        elif line[0] == 3:
            raise RuntimeError(f"{repo}: fetch error: {line[1:].decode(errors='replace')}")
    return bytes(pack)


def apply_delta(base, delta):
    def varint(pos):
        value = shift = 0
        while True:
            byte = delta[pos]
            pos += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return value, pos

    _, pos = varint(0)
    _, pos = varint(pos)
    out = bytearray()
    while pos < len(delta):
        op = delta[pos]
        pos += 1
        if op & 0x80:
            offset = size = 0
            for i in range(4):
                if op & (1 << i):
                    offset |= delta[pos] << (8 * i)
                    pos += 1
            for i in range(3):
                if op & (0x10 << i):
                    size |= delta[pos] << (8 * i)
                    pos += 1
            out += base[offset:offset + (size or 0x10000)]
        else:
            out += delta[pos:pos + op]
            pos += op
    return bytes(out)


def parse_pack(pack):
    """Returns {commit sha: (committer unix time, [parent shas])} for every commit in the packfile."""
    if pack[:4] != b"PACK":
        raise RuntimeError("not a packfile")
    count = int.from_bytes(pack[8:12], "big")
    pos = 12
    by_offset, by_sha = {}, {}
    pending = []  # deltas whose base is not resolved yet
    for _ in range(count):
        start = pos
        byte = pack[pos]
        pos += 1
        kind = (byte >> 4) & 7
        shift = 4
        while byte & 0x80:
            byte = pack[pos]
            pos += 1
            shift += 7
        base = None
        if kind == 6:
            byte = pack[pos]
            pos += 1
            offset = byte & 0x7F
            while byte & 0x80:
                byte = pack[pos]
                pos += 1
                offset = ((offset + 1) << 7) | (byte & 0x7F)
            base = ("ofs", start - offset)
        elif kind == 7:
            base = ("ref", pack[pos:pos + 20].hex())
            pos += 20
        inflater = zlib.decompressobj()
        data = bytearray()
        while not inflater.eof:
            chunk = pack[pos:pos + 65536]
            data += inflater.decompress(chunk)
            pos += len(chunk) - len(inflater.unused_data)
        if base is None:
            obj = (kind, bytes(data))
            by_offset[start] = obj
            by_sha[hashlib.sha1(b"%s %d\0" % (TYPES[kind], len(data)) + data).hexdigest()] = obj
        else:
            pending.append((start, base, bytes(data)))

    # Delta chains resolve in pack order for ofs deltas; ref deltas may need several passes.
    while pending:
        unresolved = []
        for start, (how, key), delta in pending:
            base_obj = by_offset.get(key) if how == "ofs" else by_sha.get(key)
            if base_obj is None:
                unresolved.append((start, (how, key), delta))
                continue
            data = apply_delta(base_obj[1], delta)
            obj = (base_obj[0], data)
            by_offset[start] = obj
            by_sha[hashlib.sha1(b"%s %d\0" % (TYPES[obj[0]], len(data)) + data).hexdigest()] = obj
        if len(unresolved) == len(pending):
            raise RuntimeError(f"{len(unresolved)} deltas without a base in the pack")
        pending = unresolved

    commits = {}
    for sha, (kind, data) in by_sha.items():
        if kind != 1:
            continue
        header = data.split(b"\n\n", 1)[0].split(b"\n")
        parents = [line[7:].decode() for line in header if line.startswith(b"parent ")]
        committer = next(line for line in header if line.startswith(b"committer "))
        commits[sha] = (int(committer.rsplit(b" ", 2)[1]), parents)
    return commits


TYPES = {1: b"commit", 2: b"tree", 3: b"blob", 4: b"tag"}


def version_rank(version):
    """Stable before pre-release, then the numerically highest version."""
    return ("-" not in version.split("+")[0], [int(n) for n in re.findall(r"\d+", version)])


def describe(commits, tagged):
    """Returns {commit: (base tag, commits since it)}: the release tag reachable in the fewest commits, as git describe
    picks it. `tagged` maps tagged commits to their best tag."""
    children = {}
    for sha, (_, parents) in commits.items():
        for parent in parents:
            children.setdefault(parent, []).append(sha)
    # Kahn's algorithm from the oldest commits, so every parent is described before its children.
    waiting = {sha: sum(1 for p in parents if p in commits) for sha, (_, parents) in commits.items()}
    ready = [sha for sha, n in waiting.items() if n == 0]
    bases = {}
    while ready:
        sha = ready.pop()
        if sha in tagged:
            bases[sha] = (tagged[sha], 0)
        else:
            known = [bases[p] for p in commits[sha][1] if p in bases and bases[p][0]]
            bases[sha] = min(((t, d + 1) for t, d in known), key=lambda b: b[1], default=("", 0))
        for child in children.get(sha, []):
            waiting[child] -= 1
            if waiting[child] == 0:
                ready.append(child)
    return bases


def build_rows(task_start):
    rows = []
    by_repo = {}
    for client, layer, repo, prefix in CLIENTS:
        if repo not in by_repo:
            head, branches, tags = list_refs(repo)
            wants = {oid for name, oid in branches.items() if RELEASE_BRANCH_RE.match(name)}
            if head:
                wants.add(head)
            commits = parse_pack(fetch_pack(repo, wants | set(tags.values())))
            by_repo[repo] = (tags, commits)
        tags, commits = by_repo[repo]

        # Best release tag per tagged commit for this client (a release and its rc can share a commit).
        tagged = {}
        for tag, sha in tags.items():
            version = tag[len(prefix):] if tag.startswith(prefix) else None
            if version is None or not VERSION_RE.match(version) or sha not in commits:
                continue
            if sha not in tagged or version_rank(version) > version_rank(tagged[sha][len(prefix):]):
                tagged[sha] = tag
        bases = describe(commits, tagged)

        count = 0
        for sha, (committed, _) in commits.items():
            if committed < MIN_COMMIT_TIME:
                continue
            base_tag, distance = bases.get(sha, ("", 0))
            if base_tag and committed - commits[tags[base_tag]][0] > MAX_BASE_AGE:
                base_tag, distance = "", 0
            rows.append({
                "updated_date_time": task_start,
                "client": client,
                "layer": layer,
                "repository": repo,
                "commit_sha": sha,
                "commit_date": datetime.fromtimestamp(committed, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "base_tag": base_tag,
                "base_version": base_tag[len(prefix):] if base_tag else "",
                "commits_since_base": distance,
                "is_tagged": sha in tagged,
            })
            count += 1
        print(f"  {client}: {count} commits from {repo} ({len(tagged)} tagged)")
    return rows


def main():
    dry_run = "--dry-run" in sys.argv
    task_start = datetime.fromtimestamp(int(os.environ.get("TASK_START", datetime.now(timezone.utc).timestamp())), timezone.utc)
    task_start = task_start.strftime("%Y-%m-%d %H:%M:%S")

    print("=== Client commit collection ===")
    rows = build_rows(task_start)
    if not rows:
        print("ERROR: no commits collected", file=sys.stderr)
        sys.exit(1)

    if dry_run:
        untagged = [r for r in rows if not r["is_tagged"]]
        print(f"{len(rows)} commits, {len(untagged)} untagged, {sum(1 for r in untagged if r['base_tag'])} with a base release")
        for row in rows[:5]:
            print(json.dumps(row))
        return

    url, database, table = os.environ["CLICKHOUSE_URL"], os.environ["SELF_DATABASE"], os.environ["SELF_TABLE"]
    for i in range(0, len(rows), 50000):
        batch = rows[i:i + 50000]
        clickhouse(url, f"INSERT INTO `{database}`.`{table}` FORMAT JSONEachRow", "\n".join(json.dumps(r) for r in batch))
    print(f"Inserted {len(rows)} commits into {database}.{table}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
