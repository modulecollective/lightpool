#!/usr/bin/env python3
"""Derive report tables from retained originals; never simulate or tune arms."""
from collections import defaultdict
from fractions import Fraction
import json
from pathlib import Path
import sys

import analysis


def derive(record, out):
    out.mkdir()
    inventory = json.loads((record / 'inventory.json').read_text())
    declaration = json.loads((record / 'declaration.json').read_text())
    progress = json.loads((record / 'progress.json').read_text())
    attempts = []
    grouped = defaultdict(list)
    for entry in inventory:
        directory = record / 'evidence' / entry['cell']
        path = directory / 'summary.json'
        value = dict(cell=entry['cell'], task={k: entry[k] for k in ['phase', 'nodes', 'latencyMS', 'loss', 'exchange', 'replicate', 'arm']}, exitCode=entry['exitCode'])
        if path.exists():
            s = json.loads(path.read_text())
            u = s['updateDelivery']
            value.update(metrics=analysis.metrics(s), status=s['status'], reason=s['reason'],
                         delivery={k: u[k] for k in ['status', 'generated', 'failed', 'incompleteUpdates', 'eligiblePairs', 'delivered', 'missed', 'generationFailedPairs', 'incompletePairs', 'pairLatencies', 'allReceiverTimes']},
                         traffic=s['trafficCost'],
                         nonAlive=next(p for p in s['nonAlive']['populations'] if p['label'] == 'running-target-exclusions'),
                         exclusions=next(p for p in s['populations'] if p['label'] == 'running-target-exclusions'),
                         suspicionEpisodes=sum(1 for e in s['nonAlive']['episodes'] if 'running-target-exclusions' in e['labels'] and any(segment['state'] == 'suspect' for segment in e['segments'])))
        else:
            value.update(status='missing-summary', metrics=None)
        attempts.append(value)
        key = tuple(entry[k] for k in ['phase', 'nodes', 'latencyMS', 'loss', 'exchange', 'arm'])
        grouped[key].append(value)
    selection_path = record / 'comparators.json'
    selection = json.loads(selection_path.read_text()) if selection_path.exists() else None
    result = dict(declaration=declaration, progress=progress, comparators=selection, attempts=attempts,
                  originalCosts=dict(attempts=len(inventory), rawBytes=sum(i['rawBytes'] for i in inventory),
                      storedBytes=sum(i['storedBytes'] for i in inventory),
                      summedOriginalElapsedNS=sum(int(i['originalResources'].get('elapsedNS', 0)) for i in inventory),
                      maximumOriginalPeakRSSBytes=max((int(i['originalResources'].get('peakRSSBytes') or 0) for i in inventory), default=0)))
    (out / 'results.json').write_text(json.dumps(result, default=analysis.encoded, indent=2, sort_keys=True)+'\n')
    costs = result['originalCosts']
    disposition = selection['disposition'] if selection else 'resource-limited inconclusive'
    counts = defaultdict(int)
    for i in inventory:
        counts[i['phase']] += 1
    text = [r'\subsection*{Recorded outcome}',
            f"Disposition: \\textbf{{{disposition.replace('-', ' ')}}}. "
            f"Recorded {len(inventory)} of at most 354 original attempts; "
            f"{counts['dev']} development and {len(inventory)-counts['dev']} held-out. "
            'Every launched original, its diagnostics and host observations is retained.']
    if selection and not selection['advance']:
        primary = next(s for s in selection['strata'] if s['loss']==30 and s['exchange']=='off')
        text += [f"The primary 32-node development stratum was {primary['status'].replace('-', ' ')}. "
                 f"Its lower/upper comparators were \\texttt{{{primary['L'] or 'unavailable'}}} / \\texttt{{{primary['H'] or 'unavailable'}}}. "
                 f"Practical threshold 1: {primary.get('threshold1', 'unavailable')}; "
                 f"threshold 2: {primary.get('threshold2', 'unavailable')}. "
                 'The frozen screen therefore did not admit any held-out attempt. '
                 'This does not establish equivalence or disprove a smaller effect.']
    elif selection and selection['advance']:
        text += ['Development admitted the frozen held-out plan. Held-out benefit inference must be reported separately; no positive conclusion follows from development.']
    else:
        text += ['The resource stopping rule interrupted development. No development comparator set or held-out inference is available.']
    text += [f"Originals total {costs['rawBytes']:,} raw bytes and {costs['storedBytes']:,} stored bytes. "
             f"Summed original recording/initial-reduction time was {costs['summedOriginalElapsedNS']/1e9:.3f} seconds; "
             f"maximum original process RSS was {costs['maximumOriginalPeakRSSBytes']:,} bytes. "
             'These host observations are separate from deterministic outcomes and exclude later verification and rendering.']
    # A frontier is shown only after both declared development replicates are
    # valid. Partial rows remain inspectable in results.json and the originals.
    for loss in [0, 10, 30]:
        for mode in ['on','off']:
            text += [r'\subsection*{Development: '+f'{loss}\\% loss, exchange {mode}'+r'}',
                     r'\begin{center}\small',r'\begin{tabular}{lrrrrrr}',
                     r'Arm & Misses (r2,r3) & $\bar M$ (\%) & $\bar T$ (ms) & $\bar F$ (KiB) & $\bar U$ (KiB) & Suspect ns \\ \hline']
            for arm in declaration['arms']:
                key=('dev',32,1,loss,mode,arm['id'])
                values=sorted(grouped.get(key,[]),key=lambda v:v['task']['replicate'])
                valid=len(values)==2 and all(v['exitCode']==0 and v['metrics'] and v['metrics']['valid'] for v in values)
                if valid:
                    means={k:sum(Fraction(v['metrics'][k]) for v in values)/2 for k in ['M','T','F','U']}
                    misses=','.join(v['delivery']['missed'] for v in values)
                    suspect=sum(int(v['nonAlive']['suspectNS']) for v in values)
                    text.append(f"{arm['id']} & {misses} & {float(means['M'])*100:.3f} & {float(means['T'])/1e6:.3f} & {float(means['F'])/1024:.1f} & {float(means['U'])/1024:.1f} & {suspect} \\\\")
                else:
                    text.append(f"{arm['id']} & \\multicolumn{{6}}{{l}}{{Incomplete or unlaunched; no survivor mean}} \\\\")
            text += [r'\end{tabular}\end{center}']
    # Counts and per-replicate quantiles have no independence claim.
    generated=sum(int(v['delivery']['generated']) for v in attempts if 'delivery' in v)
    failed=sum(int(v['delivery']['failed']) for v in attempts if 'delivery' in v)
    incomplete=sum(int(v['delivery']['incompletePairs']) for v in attempts if 'delivery' in v)
    exclusions=sum(int(v['exclusions']['transitions']) for v in attempts if 'exclusions' in v)
    non_alive=sum(int(v['nonAlive']['notAliveNS']) for v in attempts if 'nonAlive' in v)
    exposure=sum(int(v['nonAlive']['nominalPairNS']) for v in attempts if 'nonAlive' in v)
    suspicions=sum(v.get('suspicionEpisodes',0) for v in attempts)
    text += [r'\subsection*{Completeness and secondary outcomes}',
             f"Across the recorded attempts, {generated} updates generated; {failed} generation failures and {incomplete} incomplete receiver pairs. "
             f"Running-target exclusions: {exclusions}; suspicion episodes: {suspicions}. "
             f"Non-alive exposure: {non_alive:,} of {exposure:,} fixed running-pair nanoseconds. "
             'These sums describe the retained record, not an inference pooling strata or receiver pairs. '
             r'\texttt{results.json} retains each replicate separately: pair and all-receiver latency distributions (including non-completion counts), attempted/sent traffic over measured/whole-run windows, eligibility, suspicions, exclusions and non-alive exposure. '
             'Complete original summaries retain every update/receiver acceptance reference.']
    (out / 'paper-results.tex').write_text('\n'.join(text)+'\n')
    print(f'{len(attempts)} originals -> results.json and paper-results.tex')


if __name__=='__main__':
    if len(sys.argv)!=3:
        raise SystemExit('usage: results.py SAVED_RECORD NEW_OUTPUT_DIRECTORY')
    derive(Path(sys.argv[1]).resolve(),Path(sys.argv[2]).resolve())
