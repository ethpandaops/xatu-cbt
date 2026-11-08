#!/usr/bin/env python3
"""Fail on transformation SQL that CBT would split into broken statements.

CBT splits a rendered transformation on every ';' (strings.Split in cbt's
pkg/worker/executor.go) and runs each piece on its own. So a ';' inside a
comment or a string literal cuts a statement in two, and a piece that holds
only comments fails with "Code: 62 Empty query".

Usage: scripts/lint-model-statements.py <dir>...
"""
import pathlib
import sys


def body(text):
    """Drops the YAML frontmatter CBT strips before rendering."""
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) == 3:
            return parts[2]
    return text


def scan(sql):
    """Returns (problems, pieces): misplaced ';' as (offset, where), and the text of each statement without comments."""
    problems, pieces, code = [], [], []
    i, n = 0, len(sql)
    while i < n:
        c, two = sql[i], sql[i:i + 2]
        if two == "--":
            end = sql.find("\n", i)
            end = n if end < 0 else end
            problems += [(i + k, "comment") for k, ch in enumerate(sql[i:end]) if ch == ";"]
            i = end
        elif two == "/*":
            end = sql.find("*/", i + 2)
            end = n if end < 0 else end + 2
            problems += [(i + k, "comment") for k, ch in enumerate(sql[i:end]) if ch == ";"]
            i = end
        elif two == "{{":
            # A Go template action; quoted arguments may contain '}}', as in "{{transformation}}".
            j, quoted = i + 2, False
            while j < n and (quoted or sql[j:j + 2] != "}}"):
                if sql[j] == '"' and sql[j - 1] != "\\":
                    quoted = not quoted
                j += 1
            code.append(sql[i:j + 2])
            i = j + 2
        elif c in "'\"`":
            j = i + 1
            while j < n:
                if sql[j] == "\\":
                    j += 2
                    continue
                if sql[j] == c:
                    if sql[j + 1:j + 2] == c:
                        j += 2
                        continue
                    break
                j += 1
            problems += [(i + k, "string literal") for k, ch in enumerate(sql[i:j + 1]) if ch == ";"]
            code.append(sql[i:j + 1])
            i = j + 1
        elif c == ";":
            pieces.append("".join(code))
            code = []
            i += 1
        else:
            code.append(c)
            i += 1
    pieces.append("".join(code))
    return problems, pieces


def main(dirs):
    failed = False
    for d in dirs:
        for path in sorted(pathlib.Path(d).rglob("*.sql")):
            text = path.read_text()
            sql = body(text)
            offset = len(text) - len(sql)
            for pos, where in scan(sql)[0]:
                line = text.count("\n", 0, offset + pos) + 1
                print(f"{path}:{line}: ';' inside a {where}; CBT splits statements on every ';'")
                failed = True
            # Split exactly as CBT does: a piece with comments but no code is sent as an empty query.
            start = offset
            for piece in sql.split(";"):
                if piece.strip() and not "".join(scan(piece)[1]).strip():
                    line = text.count("\n", 0, start + len(piece) - len(piece.lstrip())) + 1
                    print(f"{path}:{line}: statement holds only comments; CBT would send it as an empty query")
                    failed = True
                start += len(piece) + 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["models/transformations"]))
