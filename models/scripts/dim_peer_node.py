#!/usr/bin/env python3
"""
Node ID and custody group order for each consensus peer in int_peer_daily.

A consensus peer ID is the identity multihash of its secp256k1 public key. The node ID is the keccak256 of
the uncompressed key, as discv5 derives it, and the node ID fixes which custody groups the peer custodies
(get_custody_groups in the Fulu spec): a peer with custody group count n custodies the first n groups of
custody_group_order. With 128 groups and 128 columns, group g is column g.

Each run reads the peers in the days its bounds touch and writes those without a row yet. Peers whose
key is not secp256k1 (libp2p tools) get an empty node ID, so they are not retried.

Standard library only: the CBT image ships python3 without third-party packages, so keccak256 is here too.
"""

import base64
import hashlib
import json
import os
import sys
import urllib.parse
import urllib.request

DEP_TABLE = 'int_peer_daily'
NUMBER_OF_CUSTODY_GROUPS = 128

# secp256k1 field prime
P = 2**256 - 2**32 - 977
UINT256_MAX = 2**256 - 1
B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
# identity multihash (code 0, length 37) of the protobuf PublicKey {Type: Secp256k1, Data: 33 bytes}
SECP256K1_PEER_ID_PREFIX = bytes([0x00, 0x25, 0x08, 0x02, 0x12, 0x21])

KECCAK_RC = [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000, 0x000000000000808B,
    0x0000000080000001, 0x8000000080008081, 0x8000000000008009, 0x000000000000008A, 0x0000000000000088,
    0x0000000080008009, 0x000000008000000A, 0x000000008000808B, 0x800000000000008B, 0x8000000000008089,
    0x8000000000008003, 0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
]
KECCAK_ROT = [[0, 36, 3, 41, 18], [1, 44, 10, 45, 2], [62, 6, 43, 15, 61], [28, 55, 25, 21, 56], [27, 20, 39, 8, 14]]
M64 = 2**64 - 1


def keccak256(data):
    """Keccak-256 with the original 0x01 padding (as Ethereum uses), not SHA3-256."""
    rate = 136
    msg = bytearray(data) + b'\x01'
    msg += b'\x00' * (-len(msg) % rate)
    msg[-1] |= 0x80
    a = [[0] * 5 for _ in range(5)]
    for off in range(0, len(msg), rate):
        for i in range(rate // 8):
            a[i % 5][i // 5] ^= int.from_bytes(msg[off + 8 * i:off + 8 * i + 8], 'little')
        for rc in KECCAK_RC:
            c = [a[x][0] ^ a[x][1] ^ a[x][2] ^ a[x][3] ^ a[x][4] for x in range(5)]
            d = [c[(x - 1) % 5] ^ (((c[(x + 1) % 5] << 1) | (c[(x + 1) % 5] >> 63)) & M64) for x in range(5)]
            a = [[a[x][y] ^ d[x] for y in range(5)] for x in range(5)]
            b = [[0] * 5 for _ in range(5)]
            for x in range(5):
                for y in range(5):
                    r = KECCAK_ROT[x][y]
                    b[y][(2 * x + 3 * y) % 5] = ((a[x][y] << r) | (a[x][y] >> (64 - r))) & M64 if r else a[x][y]
            a = [[b[x][y] ^ ((~b[(x + 1) % 5][y]) & b[(x + 2) % 5][y]) for y in range(5)] for x in range(5)]
            a[0][0] ^= rc
    return b''.join(a[i % 5][i // 5].to_bytes(8, 'little') for i in range(4))


def node_id(peer_id):
    """discv5 node ID of a secp256k1 peer ID, None for other key types or malformed IDs."""
    n = 0
    for ch in peer_id:
        i = B58.find(ch)
        if i < 0:
            return None
        n = n * 58 + i
    raw = b'\x00' * (len(peer_id) - len(peer_id.lstrip('1'))) + n.to_bytes((n.bit_length() + 7) // 8, 'big')
    if len(raw) != 39 or raw[:6] != SECP256K1_PEER_ID_PREFIX or raw[6] not in (2, 3):
        return None
    x = int.from_bytes(raw[7:], 'big')
    if x >= P:
        return None
    y = pow((x * x * x + 7) % P, (P + 1) // 4, P)
    if (y * y - x * x * x - 7) % P:
        return None
    if y & 1 != raw[6] & 1:
        y = P - y
    return keccak256(x.to_bytes(32, 'big') + y.to_bytes(32, 'big'))


def custody_group_order(nid):
    """Every custody group in the order get_custody_groups assigns them from the node ID."""
    out, seen, cur = [], set(), int.from_bytes(nid, 'big')
    while len(out) < NUMBER_OF_CUSTODY_GROUPS:
        digest = hashlib.sha256(cur.to_bytes(32, 'little')).digest()
        group = int.from_bytes(digest[:8], 'little') % NUMBER_OF_CUSTODY_GROUPS
        if group not in seen:
            seen.add(group)
            out.append(group)
        cur = 0 if cur == UINT256_MAX else cur + 1
    return out


def _request(url, body):
    parsed = urllib.parse.urlparse(url)
    netloc = parsed.hostname + (f':{parsed.port}' if parsed.port else '')
    clean = urllib.parse.urlunparse((parsed.scheme, netloc, parsed.path or '/', '', parsed.query, ''))
    req = urllib.request.Request(clean, data=body.encode('utf-8'), method='POST')
    if parsed.username:
        auth = f'{urllib.parse.unquote(parsed.username)}:{urllib.parse.unquote(parsed.password or "")}'
        req.add_header('Authorization', 'Basic ' + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=300) as resp:
        return resp.read().decode('utf-8')


def query(url, sql):
    out = _request(url, sql + '\nFORMAT JSONEachRow')
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def insert(url, database, table, rows):
    for i in range(0, len(rows), 5000):
        body = f'INSERT INTO `{database}`.`{table}` FORMAT JSONEachRow\n'
        body += '\n'.join(json.dumps(r) for r in rows[i:i + 5000])
        _request(url, body)


def main():
    url = os.environ['CLICKHOUSE_URL']
    db = os.environ['SELF_DATABASE']
    table = os.environ['SELF_TABLE']
    start = int(os.environ['BOUNDS_START'])
    end = int(os.environ['BOUNDS_END'])
    task_start = int(os.environ['TASK_START'])

    dep_db = db
    for key, value in os.environ.items():
        if key.endswith(DEP_TABLE.upper() + '_DATABASE'):
            dep_db = value

    day_peers = f"""
        SELECT peer_id_unique_key
        FROM `{dep_db}`.`{DEP_TABLE}` FINAL
        WHERE day_start_date >= toDate(fromUnixTimestamp({start}))
          AND day_start_date <= toDate(fromUnixTimestamp({end}))
          AND peer_id != ''
    """
    peers = query(url, f"""
        SELECT DISTINCT peer_id_unique_key, peer_id
        FROM `{dep_db}`.`{DEP_TABLE}` FINAL
        WHERE day_start_date >= toDate(fromUnixTimestamp({start}))
          AND day_start_date <= toDate(fromUnixTimestamp({end}))
          AND peer_id != ''
          AND peer_id_unique_key GLOBAL NOT IN (
              SELECT peer_id_unique_key
              FROM `{db}`.`{table}`
              WHERE peer_id_unique_key GLOBAL IN ({day_peers})
          )
    """)

    rows = []
    for p in peers:
        nid = node_id(p['peer_id'])
        rows.append({
            'updated_date_time': task_start,
            'peer_id_unique_key': int(p['peer_id_unique_key']),
            'peer_id': p['peer_id'],
            'node_id': nid.hex() if nid else '',
            'custody_group_order': custody_group_order(nid) if nid else [],
        })
    insert(url, db, table, rows)
    print(f'{db}.{table}: {len(rows)} new peers, {sum(1 for r in rows if not r["node_id"])} without a secp256k1 key')
    return 0


if __name__ == '__main__':
    sys.exit(main())
