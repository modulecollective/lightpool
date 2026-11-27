#!/usr/bin/env python3
"""Apply the declared decision rule in README.md to cells.tsv.

Usage: analyze.py CELLS_TSV RESULTS_MD RESULTS_TEX
Standard library only. Writes new files; refuses to overwrite.
"""
import csv
import os
import sys
from fractions import Fraction
from math import comb

FIXED = [
    "correlated-dense",
    "correlated-dense-latency-100us",
    "correlated-dense-latency-10ms",
    "correlated-dense-service-100us",
    "correlated-dense-service-1ms",
]
CONTROLS = FIXED[:1] + ["correlated-dense-healthy"] + FIXED[1:]
HELD_OUT = range(201, 231)
DESCRIBED = ["failedProbes", "localSuspicions", "eligibleEpisodes", "harmful"]


def load(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return {(r["case"], int(r["replicate"])): r for r in rows}


def valid(row):
    return row is not None and row["exitCode"] == "0" and row["admissionStatus"] == "complete"


def witness(row):
    return valid(row) and int(row["witnesses"]) > 0


def tail(b, c):
    """One-sided exact sign test: P(Bin(b + c, 1/2) >= b)."""
    n = b + c
    return Fraction(sum(comb(n, k) for k in range(b, n + 1)), 2**n)


def describe(rows):
    good = [r for r in rows if valid(r)]
    out = {"cells": len(good), "witnessCells": sum(witness(r) for r in good), "witnesses": sum(int(r["witnesses"]) for r in good)}
    for k in DESCRIBED:
        out[k] = sum(int(r[k]) for r in good)
    return out


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    for path in sys.argv[2:]:
        if os.path.exists(path):
            sys.exit(f"analyze.py: {path} exists")
    cells = load(sys.argv[1])
    md, tex = [], []

    gate_rows = [cells.get((c, r)) for c in CONTROLS for r in (1, 2, 3)]
    gate = all(row is not None and row["controlGate"] == "same" for row in gate_rows)
    differing = [f"{c}-r{r}" for c in CONTROLS for r in (1, 2, 3) if (cells.get((c, r)) or {}).get("controlGate") != "same"]

    pairs, dropped = [], []
    for i in HELD_OUT:
        ctl, sil = cells.get(("correlated-dense", i)), cells.get(("correlated-dense-silent", i))
        if valid(ctl) and valid(sil):
            pairs.append((i, ctl, sil))
        else:
            why = []
            for arm, row in (("control", ctl), ("silent", sil)):
                if row is None:
                    why.append(f"{arm} not recorded")
                elif not valid(row):
                    why.append(f"{arm} {row['status']}/{row['admissionStatus']}")
            dropped.append((i, "; ".join(why)))
    R = sum(witness(c) for _, c, _ in pairs)
    S = sum(witness(s) for _, _, s in pairs)
    b = sum(witness(c) and not witness(s) for _, c, s in pairs)
    c_ = sum(witness(s) and not witness(c) for _, c, s in pairs)
    p = tail(b, c_)
    fixed_silent = [cells.get((f + "-silent", r)) for f in FIXED for r in (1, 2, 3)]
    fixed_witness = sum(witness(r) for r in fixed_silent)
    silent_total = S + fixed_witness

    if not gate:
        verdict = "control gate failed; silent arm not read"
    elif R < 5 or len(dropped) > 5:
        verdict = "underpowered"
    elif silent_total == 0:
        verdict = "eliminated"
    elif S > 0 and p <= Fraction(1, 20):
        verdict = "reduced"
    else:
        verdict = "not explained"

    md += ["# Results", "", f"Cells: {len(cells)}. Verdict: **{verdict}**.", ""]
    md.append(f"- Control gate: {'pass, all 18 fixed control cells reproduce the 2026-11-20 virtual-cells.tsv row in the seven gate columns' if gate else 'FAILED: ' + ', '.join(differing)}.")
    if not gate:
        md.append("- The rule stops here; the silent arm is not tabulated.")
        write(sys.argv[2], md)
        write(sys.argv[3], [r"\textbf{Verdict:} control gate failed."])
        return
    md.append(f"- Held-out pairs (indices 201–230): {len(pairs)} valid, {len(dropped)} dropped" + (": " + "; ".join(f"{i} ({w})" for i, w in dropped) if dropped else "") + ".")
    md.append(f"- R = {R} control witness cells, S = {S} silent witness cells; b = {b} control-only pairs, c = {c_} silent-only pairs; P(Bin({b + c_}, 1/2) ≥ {b}) = {float(p):.4f}.")
    recorded = sum(r is not None for r in fixed_silent) + sum(cells.get(("correlated-dense-silent", i)) is not None for i in HELD_OUT)
    md.append(f"- Silent witness cells: {silent_total} of {recorded} recorded silent cells ({S} held-out, {fixed_witness} fixed); 45 were declared.")
    md.append("")

    md += ["## Fixed cells, control beside silent", "", "| Case | Index | Control witnesses | Silent witnesses | Control failed probes / suspicions / eligible | Silent failed probes / suspicions / eligible | Control stop | Silent stop |", "| --- | ---: | ---: | ---: | --- | --- | --- | --- |"]
    tex_fixed = []
    for f in FIXED:
        for r in (1, 2, 3):
            ctl, sil = cells[(f, r)], cells.get((f + "-silent", r))
            sw = sil["witnesses"] if valid(sil) else (sil["admissionStatus"] if sil else "not launched")
            sd = f"{sil['failedProbes']} / {sil['localSuspicions']} / {sil['eligibleEpisodes']}" if valid(sil) else "-"
            ss = sil["stop"] if valid(sil) else "-"
            md.append(f"| `{f}` | {r} | {ctl['witnesses']} | {sw} | {ctl['failedProbes']} / {ctl['localSuspicions']} / {ctl['eligibleEpisodes']} | {sd} | {ctl['stop']} | {ss} |")
            short = f.replace("correlated-dense", "dense")
            tex_fixed.append(f"\\texttt{{{short}}} & {r} & {ctl['witnesses']} & {sw} & {ctl['eligibleEpisodes']} & {sil['eligibleEpisodes'] if valid(sil) else '--'} \\\\")
    md.append("")

    md += ["## Held-out pairs, `correlated-dense` against `correlated-dense-silent`", "", "| Index | Control witnesses | Silent witnesses | Control failed probes / suspicions / eligible / harmful | Silent failed probes / suspicions / eligible / harmful |", "| ---: | ---: | ---: | --- | --- |"]
    for i in HELD_OUT:
        ctl, sil = cells.get(("correlated-dense", i)), cells.get(("correlated-dense-silent", i))

        def cols(row):
            if not valid(row):
                return "-", (row["admissionStatus"] if row else "not launched")
            return row["witnesses"], f"{row['failedProbes']} / {row['localSuspicions']} / {row['eligibleEpisodes']} / {row['harmful']}"

        cw, cd = cols(ctl)
        sw, sd = cols(sil)
        md.append(f"| {i} | {cw} | {sw} | {cd} | {sd} |")
    md.append("")

    md += ["## Description by arm (valid held-out pairs only)", "", "| Arm | Cells | Witness cells | Witnesses | Failed probes | Local suspicions | Eligible episodes | Harmful |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    tex_arm = []
    for arm, rows in (("control", [c for _, c, _ in pairs]), ("silent", [s for _, _, s in pairs])):
        d = describe(rows)
        md.append(f"| {arm} | {d['cells']} | {d['witnessCells']} | {d['witnesses']} | {d['failedProbes']} | {d['localSuspicions']} | {d['eligibleEpisodes']} | {d['harmful']} |")
        tex_arm.append(f"{arm} & {d['cells']} & {d['witnessCells']} & {d['witnesses']} & {d['failedProbes']} & {d['localSuspicions']} & {d['eligibleEpisodes']} & {d['harmful']} \\\\")
    md.append("")
    write(sys.argv[2], md)

    tex += [
        f"\\newcommand{{\\Verdict}}{{{verdict}}}",
        f"\\newcommand{{\\Rcount}}{{{R}}}",
        f"\\newcommand{{\\Scount}}{{{S}}}",
        f"\\newcommand{{\\Bcount}}{{{b}}}",
        f"\\newcommand{{\\Ccount}}{{{c_}}}",
        f"\\newcommand{{\\Pvalue}}{{{float(p):.4f}}}",
        f"\\newcommand{{\\Pairs}}{{{len(pairs)}}}",
        f"\\newcommand{{\\Dropped}}{{{len(dropped)}}}",
        f"\\newcommand{{\\FixedSilentWitness}}{{{fixed_witness}}}",
        f"\\newcommand{{\\SilentTotal}}{{{silent_total}}}",
        "\\newcommand{\\FixedRows}{" + "\n".join(tex_fixed) + "}",
        "\\newcommand{\\ArmRows}{" + "\n".join(tex_arm) + "}",
    ]
    write(sys.argv[3], tex)


def write(path, lines):
    with open(path, "x") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
