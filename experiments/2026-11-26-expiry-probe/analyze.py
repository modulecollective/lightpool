#!/usr/bin/env python3
"""Apply the declared rejection rules in README.md to cells.tsv.

Usage: analyze.py CELLS_TSV RESULTS_MD RESULTS_TEX
Standard library only. Writes new files; refuses to overwrite.
"""
import csv
import os
import sys
from fractions import Fraction

ARMS = ["base", "probe", "smax8", "smax12", "sm5"]
RETUNES = ["smax8", "smax12", "sm5"]
AGED = "correlated-dense-64-aged"
CRASH = ["crash", "small-double-crash"]
NULLS = {"healthy", "loss", "correlated", "partial-partition"}


def load(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return {(r["arm"], r["case"], int(r["replicate"])): r for r in rows}


def ok(row):
    return row is not None and row["status"] == "complete"


def cell_l(row):
    """A cell's L: mean of its crashes' maxNS; None if any crash lacks one."""
    values = row["crashMaxNS"].split(",")
    if row["crashMaxNS"] == "-" or "-" in values:
        return None
    return Fraction(sum(int(v) for v in values), len(values))


def common(cells, case, arms):
    reps = sorted({k[2] for k in cells if k[1] == case})
    good, bad = [], []
    for r in reps:
        present = [cells.get((a, case, r)) for a in arms]
        if all(ok(c) for c in present):
            good.append(r)
        elif any(c is not None for c in present):
            bad.append(r)
    return good, bad


def mean(values):
    return Fraction(sum(values), len(values)) if values else None


def sec(ns):
    return "-" if ns is None else f"{float(ns) / 1e9:.3f}"


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    cells = load(sys.argv[1])
    for out in sys.argv[2:]:
        if os.path.exists(out):
            sys.exit(f"analyze.py: {out} exists")
    md, tex, reasons, notes = [], [], [], []

    # W on the aged fixture.
    w = {}
    md.append("## Working-path expiry deaths (W), `correlated-dense-64-aged`\n")
    md.append("| Arm | Replicates | Complete | W | Cells with W > 0 | Running suspect s | Running dead s |")
    md.append("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
    for a in ARMS:
        reps = sorted(k[2] for k in cells if k[0] == a and k[1] == AGED)
        done = [r for r in reps if ok(cells[(a, AGED, r)])]
        wa = sum(int(cells[(a, AGED, r)]["workingPathExpiryDeaths"]) for r in done)
        hit = sum(1 for r in done if int(cells[(a, AGED, r)]["workingPathExpiryDeaths"]) > 0)
        sus = sum(int(cells[(a, AGED, r)]["runningSuspectNS"]) for r in done)
        dead = sum(int(cells[(a, AGED, r)]["runningDeadNS"]) for r in done)
        w[a] = {r: int(cells[(a, AGED, r)]["workingPathExpiryDeaths"]) for r in done}
        span = f"{reps[0]}–{reps[-1]}" if reps else "-"
        md.append(f"| `{a}` | {span} | {len(done)} | {wa} | {hit} | {sec(sus)} | {sec(dead)} |")
        tex.append(f"\\expandafter\\def\\csname W{a}\\endcsname{{{wa}}}")
        tex.append(f"\\expandafter\\def\\csname N{a}\\endcsname{{{len(done)}}}")
    md.append("")

    # Rule 1.
    good, bad = common(cells, AGED, ["base", "probe"])
    wb = sum(w["base"][r] for r in good)
    wp = sum(w["probe"][r] for r in good)
    only_b = sum(1 for r in good if w["base"][r] > 0 and w["probe"][r] == 0)
    only_p = sum(1 for r in good if w["probe"][r] > 0 and w["base"][r] == 0)
    md.append(f"Base and probe both complete on {len(good)} aged replicates"
              f"{' (inconclusive: ' + ', '.join(map(str, bad)) + ')' if bad else ''}: "
              f"W {wb} against {wp}. Replicates with a death in base only: {only_b}; in probe only: {only_p}.\n")
    tex.append(f"\\def\\Wcommonbase{{{wb}}}\\def\\Wcommonprobe{{{wp}}}\\def\\Wonlybase{{{only_b}}}\\def\\Wonlyprobe{{{only_p}}}\\def\\Wcommon{{{len(good)}}}")
    if wb < 3:
        rule1 = "inconclusive"
        notes.append(f"Rule 1 is inconclusive: baseline has {wb} W deaths, fewer than 3.")
    elif 2 * wp > wb:
        rule1 = "reject"
        reasons.append(f"Rule 1 (no benefit): probe W {wp} is more than half of baseline W {wb}.")
    else:
        rule1 = "pass"

    # L per crash fixture.
    lmean = {}
    md.append("## Crash detection (L)\n")
    md.append("| Fixture | Arm | Complete | Mean L s | Mean p50 s | Completed / eligible | Full dissemination |")
    md.append("| --- | --- | ---: | ---: | ---: | --- | --- |")
    for case in CRASH:
        for a in ARMS:
            reps = sorted(k[2] for k in cells if k[0] == a and k[1] == case)
            done = [r for r in reps if ok(cells[(a, case, r)]) and cell_l(cells[(a, case, r)]) is not None]
            lmean[(case, a)] = {r: cell_l(cells[(a, case, r)]) for r in done}
            p50 = [Fraction(sum(int(v) for v in cells[(a, case, r)]["crashP50NS"].split(",")), len(cells[(a, case, r)]["crashP50NS"].split(","))) for r in done]
            comp = sum(sum(int(v) for v in cells[(a, case, r)]["crashCompleted"].split(",")) for r in done)
            elig = sum(sum(int(v) for v in cells[(a, case, r)]["crashEligible"].split(",")) for r in done)
            full = {}
            for r in reps:
                for s in cells[(a, case, r)]["crashFullStatus"].split(","):
                    full[s] = full.get(s, 0) + 1
            fs = ", ".join(f"{k} {v}" for k, v in sorted(full.items()))
            m = mean(list(lmean[(case, a)].values()))
            md.append(f"| `{case}` | `{a}` | {len(done)}/{len(reps)} | {sec(m)} | {sec(mean(p50))} | {comp} / {elig} | {fs} |")
            key = case.replace("-", "")
            tex.append(f"\\expandafter\\def\\csname L{key}{a}\\endcsname{{{sec(m)}}}")
    md.append("")

    def lcompare(case, a, b):
        reps = sorted(set(lmean[(case, a)]) & set(lmean[(case, b)]))
        return mean([lmean[(case, a)][r] for r in reps]), mean([lmean[(case, b)][r] for r in reps]), reps

    # Rule 2.
    md.append("## Retunes against the probe\n")
    md.append("| Retune | Common aged | W retune | W probe | Only retune | Only probe | L crash ratio | L double-crash ratio | Ties or beats probe |")
    md.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    rule2 = "pass"
    for x in RETUNES:
        g, _ = common(cells, AGED, [x, "probe"])
        wx = sum(w[x][r] for r in g)
        wpx = sum(w["probe"][r] for r in g)
        ox = sum(1 for r in g if w[x][r] > 0 and w["probe"][r] == 0)
        op = sum(1 for r in g if w["probe"][r] > 0 and w[x][r] == 0)
        within = True
        ratios = []
        for case in CRASH:
            lx, lp, reps = lcompare(case, x, "probe")
            if lx is None or lp is None:
                within = False
                ratios.append("-")
                continue
            ratios.append(f"{float(lx / lp):.4f}")
            within = within and lx <= Fraction(105, 100) * lp
        ties = bool(g) and wx <= wpx and within
        md.append(f"| `{x}` | {len(g)} | {wx} | {wpx} | {ox} | {op} | {ratios[0]} | {ratios[1]} | {'yes' if ties else 'no'} |")
        if ties:
            rule2 = "reject"
            reasons.append(f"Rule 2 (a retune does as well): `{x}` has W {wx} against probe {wpx} on {len(g)} common aged replicates, with L within 5% in both crash fixtures.")
    md.append("")

    # Rule 3.
    rule3 = "pass"
    md.append("## Harm\n")
    md.append("| Fixture | Base mean datagrams | Probe mean datagrams | Ratio | Base running dead | Probe running dead | Null |")
    md.append("| --- | ---: | ---: | ---: | ---: | ---: | --- |")
    cases = sorted({k[1] for k in cells if k[0] == "probe"})
    for case in cases:
        g, _ = common(cells, case, ["base", "probe"])
        db = mean([int(cells[("base", case, r)]["datagramCount"]) for r in g])
        dp = mean([int(cells[("probe", case, r)]["datagramCount"]) for r in g])
        rb = sum(int(cells[("base", case, r)]["runningDeadDeclarations"]) for r in g)
        rp = sum(int(cells[("probe", case, r)]["runningDeadDeclarations"]) for r in g)
        nulls = sorted({cells[("probe", case, r)]["null"] for r in sorted(k[2] for k in cells if k[0] == "probe" and k[1] == case)})
        ratio = "-" if not db else f"{float(dp / db):.4f}"
        md.append(f"| `{case}` | {float(db):.1f} | {float(dp):.1f} | {ratio} | {rb} | {rp} | {', '.join(nulls)} |")
        if db and dp > Fraction(102, 100) * db:
            rule3 = "reject"
            reasons.append(f"Rule 3 (harm): probe mean datagram count in `{case}` is {ratio} of baseline, above 1.02.")
        for r in g:
            if int(cells[("probe", case, r)]["runningDeadDeclarations"]) > 0 and int(cells[("base", case, r)]["runningDeadDeclarations"]) == 0:
                rule3 = "reject"
                reasons.append(f"Rule 3 (harm): probe declares a running, unpaused target dead in `{case}` replicate {r}, where baseline declares none.")
        if case in NULLS and nulls != ["same"]:
            rule3 = "reject"
            reasons.append(f"Rule 3 (harm): null control differs in `{case}`.")
    for case in CRASH:
        lp, lb, reps = lcompare(case, "probe", "base")
        if lp is not None and lb is not None and lp > Fraction(105, 100) * lb:
            rule3 = "reject"
            reasons.append(f"Rule 3 (harm): probe mean L in `{case}` is {float(lp / lb):.4f} of baseline, above 1.05.")
        md.append("" if case != CRASH[0] else "")
        md.append(f"Mean L in `{case}`, probe against base over {len(reps)} replicates: "
                  f"{sec(lp)} s against {sec(lb)} s.")
    md.append("")
    incomplete = sorted(f"{k[0]}-{k[1]}-r{k[2]}: {v['status']}" for k, v in cells.items() if v["status"] != "complete")
    if incomplete:
        notes.append("Incomplete cells, retained and excluded from their comparison: " + "; ".join(incomplete) + ".")

    verdict = "rejected" if reasons else ("not rejected; rule 1 inconclusive" if rule1 == "inconclusive" else "not rejected")
    head = [f"# Results\n", f"Cells: {len(cells)}. Verdict: **{verdict}**.\n",
            f"Rule 1: {rule1}. Rule 2: {rule2}. Rule 3: {rule3}.\n"]
    head += [f"- {r}" for r in reasons + notes]
    head.append("")
    tex.append(f"\\def\\Verdict{{{verdict}}}\\def\\RuleOne{{{rule1}}}\\def\\RuleTwo{{{rule2}}}\\def\\RuleThree{{{rule3}}}")
    with open(sys.argv[2], "x") as f:
        f.write("\n".join(head + md) + "\n")
    with open(sys.argv[3], "x") as f:
        f.write("% Generated by analyze.py from cells.tsv; do not edit.\n" + "\n".join(tex) + "\n")


if __name__ == "__main__":
    main()
