#!/usr/bin/env python3
"""Generate docs/protocol-coverage.md: which parts of the Spark Connect protocol this client
implements, which public function builds each one, and whether the tests exercise it.

    dune build
    python3 scripts/coverage_matrix.py \
        --remote 4.1.3=sc://localhost:15241 --remote 4.2.0=sc://localhost:15242

Protocol variants are read from the OCaml modules ocaml-protoc generates into
_build/default/src/proto, so the matrix follows whatever protos are vendored.
Each --remote runs the integration suite against that server and adds a column
to the server-version table; without --remote that table is omitted.
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GEN = ROOT / "_build/default/src/proto"
SRC = [p for p in (ROOT / "src").rglob("*.ml") if "proto" not in p.parts]
TESTS = list((ROOT / "test").glob("*.ml"))
TEST_EXE = ROOT / "_build/default/test/test_spark.exe"

# (section title, generated module file, variant type, module aliases used in src)
SECTIONS = [
    ("Relations (DataFrame operations)", "relations.ml", "relation_rel_type", "R|Relations"),
    ("Expressions", "expressions.ml", "expression_expr_type", "E|Expressions"),
    ("Literal types", "expressions.ml", "expression_literal_literal_type", "E|Expressions"),
    ("Commands", "commands.ml", "command", "C|Commands"),
    ("Catalog", "catalog.ml", "catalog", "Cat|Catalog"),
    ("AnalyzePlan requests", "base.ml", "analyze_plan_request_analyze", "B|Base"),
    ("Config operations", "base.ml", "config_request_operation", "B|Base"),
]


def variants(module_file, type_name):
    lines = (GEN / module_file).read_text().splitlines()
    start = next(i for i, l in enumerate(lines) if re.match(rf"^(type|and) {type_name} =", l))
    out = []
    for l in lines[start + 1:]:
        m = re.match(r"^\s*\| ([A-Z][A-Za-z0-9_]*)", l)
        if not m:
            break
        out.append(m.group(1))
    return out


def src_lines():
    for path in SRC:
        for i, line in enumerate(path.read_text().splitlines()):
            yield path, i, line


SRC_CACHE = {p: p.read_text().splitlines() for p in SRC}
TEST_TEXT = "\n".join(p.read_text() for p in TESTS)


def definition_indent(lines):
    """For each line, the indentation at which definitions live there: 0 at top level,
    k+2 inside `module X = struct` opened at indent k (so inner lets aren't mistaken for definitions)."""
    stack, out = [0], []
    for l in lines:
        ind = len(l) - len(l.lstrip())
        if re.match(r"^\s*end\b", l) and len(stack) > 1 and ind == stack[-1] - 2:
            stack.pop()
        out.append(stack[-1])
        if re.match(r"^\s*module [A-Z][A-Za-z0-9_]* *(:[^=]*)?= *struct\b", l):
            stack.append(ind + 2)
    return out


DEF_INDENT = {p: definition_indent(ls) for p, ls in SRC_CACHE.items()}


def is_definition(path, j):
    l = SRC_CACHE[path][j]
    m = re.match(r"^(\s*)let (?:rec )?([a-z_][A-Za-z0-9_']*)", l)
    return m.group(2) if m and len(m.group(1)) == DEF_INDENT[path][j] else None


def enclosing_function(path, idx):
    for j in range(idx, -1, -1):
        name = is_definition(path, j)
        if name:
            return name
    return None


def module_name(path):
    rel = path.relative_to(ROOT / "src")
    name = rel.stem
    return {"functions": "F"}.get(name, name.capitalize())


def find_uses(name, aliases):
    pat = re.compile(rf"\b(?:{aliases})\.{name}\b")
    uses = []
    for path, lines in SRC_CACHE.items():
        for i, line in enumerate(lines):
            if pat.search(line):
                fn = enclosing_function(path, i)
                if fn:
                    uses.append((module_name(path), fn))
    seen, out = set(), []
    for u in uses:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def tested(fn):
    return re.search(rf"\.{re.escape(fn)}\b", TEST_TEXT) is not None


def functions_in(path):
    """Definition-level let bindings in a file, as (name, start, end) line ranges."""
    lines = SRC_CACHE[path]
    starts = [(i, n) for i in range(len(lines)) if (n := is_definition(path, i))]
    return [(n, s, starts[k + 1][0] if k + 1 < len(starts) else len(lines)) for k, (s, n) in enumerate(starts)]


def callers(module, fn):
    """(module, function) pairs whose body calls module.fn: same file unqualified, files that
    `open` the module unqualified, and any file using `Module.fn` qualified."""
    out = []
    for path in SRC:
        lines = SRC_CACHE[path]
        text = "\n".join(lines)
        same = module_name(path) == module
        opened = re.search(rf"^open {module}\b", text, re.M) is not None
        pats = [rf"\b{module}\.{re.escape(fn)}\b"]
        if same or opened:
            pats.append(rf"(?<![.A-Za-z0-9_']){re.escape(fn)}\b")
        pat = re.compile("|".join(pats))
        for name, s, e in functions_in(path):
            if same and name == fn:
                continue
            body = [lines[s].split("=", 1)[-1]] + lines[s + 1:e]  # include one-line definitions
            if any(pat.search(l) for l in body):
                out.append((module_name(path), name))
    return list(dict.fromkeys(out))


def tested_in(module, fn):
    if tested(fn) and module != "F":
        return True
    return re.search(rf"\b(?:F|Functions)\.{re.escape(fn)}\b", TEST_TEXT) is not None if module == "F" else False


def public_entry_points(module, fn, depth=3):
    """(module, fn) itself if a test calls it, else the nearest callers (up to `depth` levels)
    that tests call; falls back to the direct callers so the API column still names something."""
    if tested_in(module, fn):
        return [(module, fn)]
    frontier, seen = [(module, fn)], {(module, fn)}
    for _ in range(depth):
        nxt = []
        for m, f in frontier:
            for c in callers(m, f):
                if c not in seen:
                    seen.add(c)
                    nxt.append(c)
        found = [c for c in nxt if tested_in(*c)]
        if found:
            return found
        frontier = nxt
    return callers(module, fn) or [(module, fn)]


def run_suite(label, remote):
    env = {"SPARK_REMOTE": remote, "PATH": "/usr/bin:/bin"}
    r = subprocess.run([str(TEST_EXE)], capture_output=True, text=True, env=env, timeout=900)
    results = {}
    for line in r.stdout.splitlines():
        m = re.search(r"\[(OK|FAIL|SKIP)\]\s+(\S+)\s+\d+\s+(.+?)\.\s*$", line)
        if m and not line.startswith(("│", "└", "┌")):
            results[(m.group(2), m.group(3))] = m.group(1)
    if not results:
        sys.exit(f"{label}: no test results parsed (is {remote} up?)\n{r.stdout[-2000:]}{r.stderr[-2000:]}")
    return results


def main():
    ap = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    ap.add_argument("--remote", action="append", default=[], help="LABEL=sc://host:port")
    ap.add_argument("--out", default=str(ROOT / "docs/protocol-coverage.md"))
    args = ap.parse_args()
    if not GEN.is_dir():
        sys.exit("run `dune build` first")

    md = ["# Spark Connect protocol coverage", "",
          "Generated by `scripts/coverage_matrix.py` from the vendored protos and the client source; "
          "do not edit by hand.", "",
          "**Implemented** means the client builds the message. **API** is the public function that "
          "builds it. **Tested** means a test calls that function (unit or integration).", ""]
    summary = []
    body = []
    for title, mod, typ, aliases in SECTIONS:
        names = variants(mod, typ)
        rows, impl, tst = [], 0, 0
        for n in names:
            uses = find_uses(n, aliases)
            if uses:
                impl += 1
                # internal helpers (no direct test call) are credited to the public functions calling them
                expanded = []
                for m, f in uses:
                    expanded += public_entry_points(m, f)
                expanded = list(dict.fromkeys(expanded))
                api = ", ".join(f"`{m}.{f}`" for m, f in expanded[:3]) + (" …" if len(expanded) > 3 else "")
                t = any(tested_in(m, f) for m, f in expanded)
                tst += t
                rows.append(f"| `{n}` | yes | {api} | {'yes' if t else '**no**'} |")
            else:
                rows.append(f"| `{n}` | no | | |")
        summary.append(f"| {title} | {impl} / {len(names)} | {tst} / {impl} |")
        body += [f"## {title}", "", "| Message | Implemented | API | Tested |", "|---|---|---|---|", *rows, ""]

    md += ["## Summary", "", "| Area | Implemented | Of those, tested |", "|---|---|---|", *summary, ""]

    if args.remote:
        runs = []
        for spec in args.remote:
            label, remote = spec.split("=", 1)
            runs.append((label, run_suite(label, remote)))
        cases = list(dict.fromkeys(k for _, r in runs for k in r))
        md += ["## Integration suite by Spark server version", "",
               "| Test | " + " | ".join(l for l, _ in runs) + " |",
               "|---|" + "---|" * len(runs)]
        for c in cases:
            md.append(f"| {c[0]}: {c[1]} | " + " | ".join(r.get(c, "n/a") for _, r in runs) + " |")
        md.append("")

    md += body
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(md))
    print("\n".join(summary))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
