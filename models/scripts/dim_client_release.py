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
    ("silver", "consensus", "gattaca-com/silver", ""),
    ("ethlambda", "consensus", "lambdaclass/ethlambda", ""),
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


def api_release_dates(repo, token, known_tags=frozenset()):
    """Returns {tag: (published datetime, prerelease, name, html_url)} for published releases from the REST API, newest
    first, stopping after a page whose releases are all in `known_tags` (stored already, so older pages are too)."""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    dates = {}
    url = f"https://api.github.com/repos/{repo}/releases?per_page=100"
    while url:
        resp = urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=HTTP_TIMEOUT)
        page = json.loads(resp.read())
        for release in page:
            if release.get("published_at"):
                published = datetime.strptime(release["published_at"], "%Y-%m-%dT%H:%M:%SZ")
                dates[release["tag_name"]] = (
                    published, bool(release.get("prerelease")), release.get("name") or "", release.get("html_url") or "",
                )
        if all(release["tag_name"] in known_tags for release in page):
            break
        url = next_page(resp.headers.get("Link", ""))
    return dates


def api_latest_tag(repo, token):
    """Returns the tag of the release GitHub marks as latest, or None when the API is unavailable."""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}/releases/latest", headers=headers)
    try:
        return json.loads(urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read()).get("tag_name")
    except (urllib.error.URLError, ValueError) as e:
        print(f"  {repo}: latest release unavailable ({e})")
        return None


def next_page(link_header):
    """Returns the rel="next" URL from a GitHub Link header, or None on the last page."""
    for part in link_header.split(","):
        url, _, rel = part.partition(";")
        if rel.strip() == 'rel="next"':
            return url.strip().strip("<>")
    return None


def atom_release_dates(repo):
    """Returns {tag: (updated datetime, None, title, link)} for the latest releases from the releases Atom feed."""
    req = urllib.request.Request(f"https://github.com/{repo}/releases.atom", headers={"User-Agent": USER_AGENT})
    root = ET.fromstring(urllib.request.urlopen(req, timeout=HTTP_TIMEOUT).read())
    ns = {"a": "http://www.w3.org/2005/Atom"}
    dates = {}
    for entry in root.findall("a:entry", ns):
        link = entry.find("a:link", ns)
        updated = entry.findtext("a:updated", default="", namespaces=ns)
        if link is None or not updated:
            continue
        href = link.get("href", "")
        tag = urllib.parse.unquote(href.rsplit("/releases/tag/", 1)[-1])
        title = entry.findtext("a:title", default="", namespaces=ns)
        dates[tag] = (datetime.fromisoformat(updated.replace("Z", "+00:00")).replace(tzinfo=None), None, title, href)
    return dates


def release_dates(repo, token, known_tags=frozenset()):
    """Returns (releases, exact): REST API releases, or Atom feed releases (update dates) when the API is unavailable."""
    try:
        return api_release_dates(repo, token, known_tags), True
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


def stored_releases(url, database, table):
    """Returns {(client, tag): (release datetime or None, name, html_url)} already stored, so a failed lookup keeps them."""
    out = clickhouse(
        url,
        f"SELECT client, tag, toString(release_date), name, html_url FROM `{database}`.`{table}` FINAL "
        f"WHERE release_date IS NOT NULL OR html_url != '' FORMAT JSONCompact",
    )
    rows = json.loads(out).get("data", []) if out.strip() else []
    return {
        (c, t): (datetime.strptime(d, "%Y-%m-%d %H:%M:%S") if d else None, n, h)
        for c, t, d, n, h in rows
    }


def build_rows(task_start, token, previous):
    """Collects one row per client release tag."""
    tags_by_repo = {}
    releases_by_repo = {}
    latest_by_repo = {}
    rows = []
    for client, layer, repo, prefix in CLIENTS:
        if repo not in tags_by_repo:
            tags_by_repo[repo] = list_tags(repo)
            known = {t for (c, t), (d, _, h) in previous.items() if d and h and c in {x[0] for x in CLIENTS if x[2] == repo}}
            releases_by_repo[repo] = release_dates(repo, token, known)
            latest_by_repo[repo] = api_latest_tag(repo, token)
        tags, (releases, exact) = tags_by_repo[repo], releases_by_repo[repo]

        client_rows = []
        for tag, commit in tags.items():
            if not tag.startswith(prefix):
                continue
            version = tag[len(prefix):]
            if not VERSION_RE.match(version):
                continue
            published, prerelease_flag, name, html_url = releases.get(tag, (None, None, "", ""))
            stored_date, stored_name, stored_url = previous.get((client, tag), (None, "", ""))
            # Atom feed dates are update times, so a stored API date wins over them.
            if (not exact and stored_date) or published is None:
                published = stored_date or published
            prerelease = prerelease_flag if prerelease_flag is not None else "-" in version.split("+")[0]
            client_rows.append({
                "updated_date_time": task_start,
                "client": client,
                "layer": layer,
                "repository": repo,
                "version": version,
                "tag": tag,
                "commit_sha": commit,
                "release_date": published.strftime("%Y-%m-%d %H:%M:%S") if published else None,
                "prerelease": prerelease,
                "name": name or stored_name,
                "html_url": html_url or stored_url,
                "is_latest": False,
            })

        # GitHub's latest release for the repository, else (a monorepo whose latest is another package, or no API)
        # the client's most recently published stable release, else its most recently published release, else (no dates
        # at all) its highest stable version.
        latest = [r for r in client_rows if r["tag"] == latest_by_repo[repo]]
        dated = [r for r in client_rows if r["release_date"]]
        stable = [r for r in dated if not r["prerelease"]]
        if latest or dated:
            (latest or [max(stable or dated, key=lambda r: r["release_date"])])[0]["is_latest"] = True
        elif client_rows:
            max(client_rows, key=lambda r: (not r["prerelease"], [int(n) for n in re.findall(r"\d+", r["version"])]))["is_latest"] = True
        rows.extend(client_rows)
        print(f"  {client}: {len(client_rows)} release tags from {repo}")
    return rows


def main():
    dry_run = "--dry-run" in sys.argv
    task_start = datetime.fromtimestamp(int(os.environ.get("TASK_START", datetime.now(timezone.utc).timestamp())), timezone.utc)
    task_start = task_start.strftime("%Y-%m-%d %H:%M:%S")
    token = os.environ.get("GITHUB_TOKEN", "")

    previous = {}
    if not dry_run:
        url, database, table = os.environ["CLICKHOUSE_URL"], os.environ["SELF_DATABASE"], os.environ["SELF_TABLE"]
        previous = stored_releases(url, database, table)

    print("=== Client release collection ===")
    rows = build_rows(task_start, token, previous)
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
