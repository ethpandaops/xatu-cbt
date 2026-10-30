#!/usr/bin/env python3
"""
Execution cost per opcode group, by execution client.

Fits block execution time against gas per opcode group from the hourly normal equations in
int_engine_new_payload_opcode_gram_hourly, and writes one row per opcode group:

- OPCODE_COST_MODE=daily: one fit per day and client, into fct_opcode_cost_by_el_client_daily.
  Intervals come from resampling the day's hours.
- OPCODE_COST_MODE=version: one fit per client version over every hour it ran, into
  fct_opcode_cost_by_el_client_version. Intervals come from resampling 6-hour blocks.

The model, per block observation:

    median engine_newPayload ms = b0 + b1 * txs/100 + b2 * non-opcode Mgas + sum_g bg * Mgas_g

b_g is in ms per Mgas, which is ns per gas. Coefficients are constrained to be non-negative and
ridge-penalised towards the client's average ns per gas, so an opcode group the data can't
separate (rare, or always executed alongside another) is reported as priced in line with the
rest of execution rather than as noise. The penalty grows with the number of blocks, so its
weight relative to the data is the same for a day and for a long-running release.

price_ratio = b_g / client average ns per gas. 1 means the opcode group costs as much time per
gas as the client's execution does on average.

Standard library only: the CBT image ships python3 without numpy.
"""

import base64
import json
import os
import random
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

GRAM_TABLE = 'int_engine_new_payload_opcode_gram_hourly'

# Fixed (unpenalised) features before the opcode groups: INTERCEPT, TX_COUNT, NON_OPCODE_GAS
FIXED_FEATURES = 3
# Ridge strength per block observation; 10,000 over a 36,000-block window
RIDGE_PER_BLOCK = 10000 / 36000
# Refits of the prior's centre, the client average ns per gas
PRIOR_ITERATIONS = 4
BOOTSTRAP_SAMPLES = 50
MIN_BLOCKS = 500
MIN_HOURS = 3
SIX_HOURS = 6 * 3600


# --- ClickHouse -------------------------------------------------------------------------------

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
        chunk = rows[i:i + 5000]
        body = f'INSERT INTO `{database}`.`{table}` FORMAT JSONEachRow\n'
        body += '\n'.join(json.dumps(r) for r in chunk)
        _request(url, body)


# --- Linear algebra on small dense matrices ---------------------------------------------------

def unpack_upper(upper, k):
    """Row-major upper triangle (with diagonal) to a full symmetric k x k matrix."""
    m = [[0.0] * k for _ in range(k)]
    idx = 0
    for i in range(k):
        for j in range(i, k):
            m[i][j] = m[j][i] = upper[idx]
            idx += 1
    return m


def cholesky_solve(a, b):
    """Solve a x = b for symmetric positive definite a."""
    n = len(b)
    low = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            s = a[i][j] - sum(low[i][k] * low[j][k] for k in range(j))
            if i == j:
                if s <= 0:
                    raise ValueError('matrix is not positive definite')
                low[i][i] = s ** 0.5
            else:
                low[i][j] = s / low[j][j]
    y = [0.0] * n
    for i in range(n):
        y[i] = (b[i] - sum(low[i][k] * y[k] for k in range(i))) / low[i][i]
    x = [0.0] * n
    for i in reversed(range(n)):
        x[i] = (y[i] - sum(low[k][i] * x[k] for k in range(i + 1, n))) / low[i][i]
    return x


def nnls(a, q, start=None, max_iter=200):
    """
    Minimise 1/2 x'ax - q'x subject to x >= 0 (Lawson-Hanson active set, on the normal equations).
    `start` is an optional feasible warm start, such as the point estimate when bootstrapping.
    """
    n = len(q)
    x = list(start) if start else [0.0] * n
    passive = {i for i in range(n) if x[i] > 0}
    tol = 1e-12 * max(1.0, max(abs(v) for v in q))

    def gradient():
        return [q[i] - sum(a[i][j] * x[j] for j in range(n)) for i in range(n)]

    def solve_passive():
        idx = sorted(passive)
        sub = cholesky_solve([[a[i][j] for j in idx] for i in idx], [q[i] for i in idx])
        s = [0.0] * n
        for i, v in zip(idx, sub):
            s[i] = v
        return s

    for _ in range(max_iter):
        if passive:
            # Inner loop: keep the passive set's unconstrained solution feasible
            while True:
                s = solve_passive()
                negative = [i for i in passive if s[i] <= 0]
                if not negative:
                    break
                alpha = min(x[i] / (x[i] - s[i]) for i in negative)
                x = [x[i] + alpha * (s[i] - x[i]) for i in range(n)]
                passive -= {i for i in passive if x[i] <= tol}
                if not passive:
                    s = [0.0] * n
                    break
            x = s
        w = gradient()
        candidates = [i for i in range(n) if i not in passive and w[i] > tol]
        if not candidates:
            return x
        passive.add(max(candidates, key=lambda i: w[i]))
    return x


# --- Model ------------------------------------------------------------------------------------

class Gram:
    """Summed normal equations X'X, X'y, y'y over some hours."""

    def __init__(self, k, groups):
        self.k = k
        self.xtx = [[0.0] * k for _ in range(k)]
        self.xty = [0.0] * k
        self.yty = 0.0
        self.blocks = 0
        self.counts = [0] * groups

    def add(self, other, weight=1):
        for i in range(self.k):
            row, orow = self.xtx[i], other.xtx[i]
            for j in range(self.k):
                row[j] += weight * orow[j]
            self.xty[i] += weight * other.xty[i]
        self.yty += weight * other.yty
        self.blocks += weight * other.blocks
        for i, c in enumerate(other.counts):
            self.counts[i] += weight * c

    @classmethod
    def from_row(cls, row):
        k = len(row['feature_names'])
        g = cls(k, k - FIXED_FEATURES)
        g.xtx = unpack_upper(row['xtx'], k)
        g.xty = list(row['xty'])
        g.yty = row['yty']
        g.blocks = row['block_count']
        g.counts = [int(c) for c in row['opcode_count']]
        return g


def fit(gram, start=None):
    """Returns (coefficients, client average ns per gas)."""
    k = gram.k
    col = gram.xtx[0]  # sum of each feature, since feature 0 is the intercept
    gas_total = sum(col[FIXED_FEATURES - 1:])
    lam = RIDGE_PER_BLOCK * gram.blocks
    # Start the prior's centre at the plain average: total ms over total Mgas
    centre = gram.xty[0] / gas_total if gas_total > 0 else 0.0
    b = start
    for _ in range(PRIOR_ITERATIONS if start is None else 2):
        a = [row[:] for row in gram.xtx]
        q = gram.xty[:]
        for j in range(FIXED_FEATURES, k):
            a[j][j] += lam
            q[j] += lam * centre
        b = nnls(a, q, start=b)
        centre = sum(b[j] * col[j] for j in range(FIXED_FEATURES - 1, k)) / gas_total
    return b, centre


def r_squared(gram, b):
    n = gram.blocks
    sse = gram.yty - 2 * sum(b[i] * gram.xty[i] for i in range(gram.k)) + sum(
        b[i] * b[j] * gram.xtx[i][j] for i in range(gram.k) for j in range(gram.k)
    )
    sst = gram.yty - gram.xty[0] ** 2 / n
    return 1 - sse / sst if sst > 0 else 0.0


def quantile(values, q):
    s = sorted(values)
    pos = (len(s) - 1) * q
    lo, hi = int(pos), min(int(pos) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def estimate(units, names, seed):
    """
    Fit the sum of `units` (a list of Gram) and bootstrap by resampling units with replacement.
    Returns one dict per opcode group, or None when there is too little data.
    """
    total = Gram(units[0].k, len(units[0].counts))
    for u in units:
        total.add(u)
    if total.blocks < MIN_BLOCKS:
        return None

    b, centre = fit(total)
    if centre <= 0:
        return None

    rng = random.Random(seed)
    samples = []
    for _ in range(BOOTSTRAP_SAMPLES if len(units) > 1 else 0):
        draw = Gram(total.k, len(total.counts))
        for u in (rng.choice(units) for _ in units):
            draw.add(u)
        db, dcentre = fit(draw, start=b)
        if dcentre > 0:
            samples.append([db[j] / dcentre for j in range(FIXED_FEATURES, total.k)])

    col = total.xtx[0]
    gas_total = sum(col[FIXED_FEATURES - 1:])
    r2 = r_squared(total, b)
    rows = []
    for gi, name in enumerate(names[FIXED_FEATURES:]):
        j = FIXED_FEATURES + gi
        gas_per_op = col[j] * 1e6 / total.counts[gi] if total.counts[gi] else 0.0
        ratio = b[j] / centre
        lo = quantile([s[gi] for s in samples], 0.05) if samples else ratio
        hi = quantile([s[gi] for s in samples], 0.95) if samples else ratio
        rows.append({
            'opcode_group': name,
            'block_count': int(total.blocks),
            'gas_share': col[j] / gas_total if gas_total else 0.0,
            'gas_per_op': gas_per_op,
            'ns_per_gas': b[j],
            'ns_per_op': b[j] * gas_per_op,
            'ns_per_op_lower': lo * centre * gas_per_op,
            'ns_per_op_upper': hi * centre * gas_per_op,
            'price_ratio': ratio,
            'price_ratio_lower': lo,
            'price_ratio_upper': hi,
            'client_ns_per_gas': centre,
            'overhead_ms': b[0],
            'per_tx_us': b[1] * 10,
            'r_squared': r2,
        })
    return rows


# --- Runs -------------------------------------------------------------------------------------

GRAM_COLUMNS = """
    toUnixTimestamp(hour_start_date_time) AS hour,
    node_class,
    meta_execution_implementation,
    meta_execution_version,
    block_count,
    feature_names,
    xtx,
    xty,
    yty,
    opcode_count
"""


def run_daily(url, db, gram_table, start, end, task_start, out_table):
    days = [r['day'] for r in query(url, f"""
        SELECT DISTINCT toString(toDate(hour_start_date_time)) AS day
        FROM `{db}`.`{gram_table}` FINAL
        WHERE max_block_number >= {start} AND min_block_number <= {end}
    """)]
    if not days:
        print('No hourly sums in range')
        return 0
    day_list = ','.join(f"'{d}'" for d in days)
    rows = query(url, f"""
        SELECT {GRAM_COLUMNS}, toString(toDate(hour_start_date_time)) AS day
        FROM `{db}`.`{gram_table}` FINAL
        WHERE toDate(hour_start_date_time) IN ({day_list})
    """)

    groups = {}
    for r in rows:
        key = (r['day'], r['node_class'], r['meta_execution_implementation'])
        entry = groups.setdefault(key, {'hours': {}, 'versions': {}, 'names': r['feature_names']})
        hour = entry['hours'].get(r['hour'])
        g = Gram.from_row(r)
        if hour is None:
            entry['hours'][r['hour']] = g
        else:
            hour.add(g)
        entry['versions'][r['meta_execution_version']] = entry['versions'].get(r['meta_execution_version'], 0) + r['block_count']

    out = []
    for (day, node_class, impl), entry in sorted(groups.items()):
        units = list(entry['hours'].values())
        if len(units) < MIN_HOURS:
            continue
        fitted = estimate(units, entry['names'], seed=f'{day}|{node_class}|{impl}')
        if not fitted:
            continue
        versions = [v for v, _ in sorted(entry['versions'].items(), key=lambda kv: -kv[1])]
        for row in fitted:
            row.update({
                'updated_date_time': task_start,
                'day_start_date': day,
                'node_class': node_class,
                'meta_execution_implementation': impl,
                'meta_execution_versions': versions,
                'hour_count': len(units),
            })
            out.append(row)
        print(f'  {day} {impl}: {entry["hours"] and sum(u.blocks for u in units)} blocks over {len(units)} hours')

    insert(url, db, out_table, out)
    return len(out)


def run_version(url, db, gram_table, start, end, task_start, out_table):
    versions = query(url, f"""
        SELECT DISTINCT node_class, meta_execution_implementation, meta_execution_version
        FROM `{db}`.`{gram_table}` FINAL
        WHERE max_block_number >= {start} AND min_block_number <= {end}
    """)
    out = []
    for v in versions:
        esc = {k: str(val).replace("\\", "\\\\").replace("'", "\\'") for k, val in v.items()}
        rows = query(url, f"""
            SELECT {GRAM_COLUMNS}
            FROM `{db}`.`{gram_table}` FINAL
            WHERE node_class = '{esc['node_class']}'
              AND meta_execution_implementation = '{esc['meta_execution_implementation']}'
              AND meta_execution_version = '{esc['meta_execution_version']}'
        """)
        if len(rows) < MIN_HOURS:
            continue
        blocks = {}
        for r in rows:
            g = Gram.from_row(r)
            unit = blocks.get(r['hour'] // SIX_HOURS)
            if unit is None:
                blocks[r['hour'] // SIX_HOURS] = g
            else:
                unit.add(g)
        key = f"{v['node_class']}|{v['meta_execution_implementation']}|{v['meta_execution_version']}"
        fitted = estimate(list(blocks.values()), rows[0]['feature_names'], seed=key)
        if not fitted:
            continue
        hours = [r['hour'] for r in rows]
        for row in fitted:
            row.update({
                'updated_date_time': task_start,
                'node_class': v['node_class'],
                'meta_execution_implementation': v['meta_execution_implementation'],
                'meta_execution_version': v['meta_execution_version'],
                'first_seen_date_time': min(hours),
                'last_seen_date_time': max(hours),
                'hour_count': len(rows),
            })
            out.append(row)
        print(f'  {key}: {fitted[0]["block_count"]} blocks over {len(rows)} hours')

    insert(url, db, out_table, out)
    return len(out)


def main():
    url = os.environ['CLICKHOUSE_URL']
    db = os.environ['SELF_DATABASE']
    table = os.environ['SELF_TABLE']
    start = int(os.environ['BOUNDS_START'])
    end = int(os.environ['BOUNDS_END'])
    task_start = int(os.environ.get('TASK_START', datetime.now(timezone.utc).timestamp()))
    mode = os.environ.get('OPCODE_COST_MODE', 'daily')

    gram_db, gram_table = db, GRAM_TABLE
    for key, value in os.environ.items():
        if key.endswith(GRAM_TABLE.upper() + '_DATABASE'):
            gram_db = value

    print(f'=== Opcode cost ({mode}) {db}.{table}, blocks {start}-{end} ===')
    if mode == 'daily':
        n = run_daily(url, gram_db, gram_table, start, end, task_start, table)
    elif mode == 'version':
        n = run_version(url, gram_db, gram_table, start, end, task_start, table)
    else:
        print(f'Unknown OPCODE_COST_MODE {mode}', file=sys.stderr)
        return 1
    print(f'Inserted {n} rows')
    return 0


if __name__ == '__main__':
    sys.exit(main())
