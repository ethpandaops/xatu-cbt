#!/usr/bin/env python3
"""Maps client release tags to their commits so graffiti commit prefixes resolve to versions (--dry-run prints rows)."""

import base64
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# (client, layer, repository, tag prefix); names match fct_block_graffiti, caplin ships inside erigon.
CLIENTS = [
    ("lighthouse", "consensus", "sigp/lighthouse", ""),
    ("prysm", "consensus", "OffchainLabs/prysm", ""),
    ("teku", "consensus", "Consensys-Incorporated/teku", ""),
    ("nimbus", "consensus", "status-im/nimbus-eth2", ""),
    ("lodestar", "consensus", "ChainSafe/lodestar", ""),
    ("grandine", "consensus", "grandinetech/grandine", ""),
    ("caplin", "consensus", "erigontech/erigon", ""),
    ("geth", "execution", "ethereum/go-ethereum", ""),
    ("nethermind", "execution", "NethermindEth/nethermind", ""),
    ("besu", "execution", "besu-eth/besu", ""),
    ("erigon", "execution", "erigontech/erigon", ""),
    ("reth", "execution", "paradigmxyz/reth", ""),
    ("ethrex", "execution", "lambdaclass/ethrex", ""),
    ("nimbusel", "execution", "status-im/nimbus-eth1", ""),
    ("ethereumjs", "execution", "ethereumjs/ethereumjs-monorepo", "@ethereumjs/client@"),
    ("trin-execution", "execution", "ethereum/trin", ""),
]

VERSION_RE = re.compile(r"^v?\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")
HTTP_TIMEOUT = 60
USER_AGENT = "xatu-cbt-dim-client-release"


def pkt_line(text):
    """Encodes one git pkt-line."""
    data = text.encode()
    return f"{len(data) + 4:04x}".encode() + data


def list_tags(repo):
    """Returns {tag: peeled commit sha} via git protocol v2 ls-refs, which the REST rate limit does not cover."""
    body = pkt_line("command=ls-refs\n") + b"0001" + pkt_line("peel\n") + pkt_line("ref-prefix refs/tags/\n") + b"0000"
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
    raw = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read()

    tags = {}
    offset = 0
    while offset < len(raw):
        size = int(raw[offset:offset + 4], 16)
        if size < 4:
            offset += 4
            continue
        fields = raw[offset + 4:offset + size].decode().rstrip("\n").split(" ")
        offset += size
        if len(fields) < 2 or not fields[1].startswith("refs/tags/"):
            continue
        peeled = next((f.split(":", 1)[1] for f in fields[2:] if f.startswith("peeled:")), fields[0])
        tags[fields[1][len("refs/tags/"):]] = peeled.lower()
    return tags


def api_release_dates(repo, token):
    """Returns {tag: (published datetime, prerelease)} for the latest 100 releases from the REST API."""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}/releases?per_page=100", headers=headers)
    releases = json.loads(urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read())
    dates = {}
    for release in releases:
        if release.get("published_at"):
            published = datetime.strptime(release["published_at"], "%Y-%m-%dT%H:%M:%SZ")
            dates[release["tag_name"]] = (published, bool(release.get("prerelease")))
    return dates


def atom_release_dates(repo):
    """Returns {tag: (updated datetime, None)} for the latest releases from the releases Atom feed."""
    req = urllib.request.Request(f"https://github.com/{repo}/releases.atom", headers={"User-Agent": USER_AGENT})
    root = ET.fromstring(urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read())
    ns = {"a": "http://www.w3.org/2005/Atom"}
    dates = {}
    for entry in root.findall("a:entry", ns):
        link = entry.find("a:link", ns)
        updated = entry.findtext("a:updated", default="", namespaces=ns)
        if link is None or not updated:
            continue
        tag = urllib.parse.unquote(link.get("href", "").rsplit("/releases/tag/", 1)[-1])
        dates[tag] = (datetime.fromisoformat(updated.replace("Z", "+00:00")).replace(tzinfo=None), None)
    return dates


def release_dates(repo, token):
    """Returns (dates, exact): REST API publish dates, or Atom feed update dates when the API is unavailable."""
    try:
        return api_release_dates(repo, token), True
    except (urllib.error.URLError, ValueError) as e:
        print(f"  {repo}: releases API unavailable ({e}), using the Atom feed")
    try:
        return atom_release_dates(repo), False
    except (urllib.error.URLError, ET.ParseError, ValueError) as e:
        print(f"  {repo}: releases feed unavailable ({e})")
        return {}, False


def clickhouse(url, query, data=None):
    """Runs a query over the ClickHouse HTTP interface, sending `data` as the request body after the query."""
    parsed = urllib.parse.urlparse(url)
    netloc = parsed.hostname + (f":{parsed.port}" if parsed.port else "")
    params = dict(urllib.parse.parse_qsl(parsed.query))
    body = query.encode()
    if data is not None:
        params["query"] = query
        body = data.encode()
    target = urllib.parse.urlunparse((parsed.scheme, netloc, parsed.path or "/", "", urllib.parse.urlencode(params), ""))
    req = urllib.request.Request(target, data=body, method="POST")
    if parsed.username:
        credentials = f"{urllib.parse.unquote(parsed.username)}:{urllib.parse.unquote(parsed.password or '')}"
        req.add_header("Authorization", "Basic " + base64.b64encode(credentials.encode()).decode())
    return urllib.request.urlopen(req, timeout=300).read().decode()


def stored_dates(url, database, table):
    """Returns {(client, tag): release datetime} already stored, so a failed date lookup keeps them."""
    out = clickhouse(
        url,
        f"SELECT client, tag, toString(release_date) FROM `{database}`.`{table}` FINAL "
        f"WHERE release_date IS NOT NULL FORMAT JSONCompact",
    )
    rows = json.loads(out).get("data", []) if out.strip() else []
    return {(c, t): datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for c, t, d in rows}


def build_rows(task_start, token, previous_dates):
    """Collects one row per client release tag."""
    tags_by_repo = {}
    dates_by_repo = {}
    rows = []
    for client, layer, repo, prefix in CLIENTS:
        if repo not in tags_by_repo:
            tags_by_repo[repo] = list_tags(repo)
            dates_by_repo[repo] = release_dates(repo, token)
        tags, (dates, exact) = tags_by_repo[repo], dates_by_repo[repo]

        count = 0
        for tag, commit in tags.items():
            if not tag.startswith(prefix):
                continue
            version = tag[len(prefix):]
            if not VERSION_RE.match(version):
                continue
            published, prerelease_flag = dates.get(tag, (None, None))
            # Atom feed dates are update times, so a stored API date wins over them.
            if not exact and (client, tag) in previous_dates:
                published = previous_dates[(client, tag)]
            elif published is None:
                published = previous_dates.get((client, tag))
            prerelease = prerelease_flag if prerelease_flag is not None else "-" in version.split("+")[0]
            rows.append({
                "updated_date_time": task_start,
                "client": client,
                "layer": layer,
                "repository": repo,
                "version": version,
                "tag": tag,
                "commit_sha": commit,
                "release_date": published.strftime("%Y-%m-%d %H:%M:%S") if published else None,
                "prerelease": prerelease,
            })
            count += 1
        print(f"  {client}: {count} release tags from {repo}")
    return rows


def main():
    dry_run = "--dry-run" in sys.argv
    task_start = datetime.fromtimestamp(int(os.environ.get("TASK_START", datetime.now(timezone.utc).timestamp())), timezone.utc)
    task_start = task_start.strftime("%Y-%m-%d %H:%M:%S")
    token = os.environ.get("GITHUB_TOKEN", "")

    previous_dates = {}
    if not dry_run:
        url, database, table = os.environ["CLICKHOUSE_URL"], os.environ["SELF_DATABASE"], os.environ["SELF_TABLE"]
        previous_dates = stored_dates(url, database, table)

    print("=== Client release collection ===")
    rows = build_rows(task_start, token, previous_dates)
    if not rows:
        print("ERROR: no release tags collected", file=sys.stderr)
        sys.exit(1)

    if dry_run:
        for row in rows:
            print(json.dumps(row))
        return

    clickhouse(url, f"INSERT INTO `{database}`.`{table}` FORMAT JSONEachRow", "\n".join(json.dumps(r) for r in rows))
    dated = sum(1 for r in rows if r["release_date"])
    print(f"Inserted {len(rows)} release tags ({dated} with a release date) into {database}.{table}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
