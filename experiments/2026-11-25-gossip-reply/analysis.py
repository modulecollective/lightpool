#!/usr/bin/env python3
"""Frozen development selection. Replicates, never pairs, are observations."""
from fractions import Fraction
import json
from pathlib import Path
import sys


def metrics(summary):
    u, cost = summary['updateDelivery'], summary['trafficCost']['measured']['attempted']
    valid = (summary['status'] == 'complete' and u['status'] == 'complete'
             and int(u['generated']) == 20 and int(u['failed']) == 0
             and int(u['incompletePairs']) == 0 and int(u['generationFailedPairs']) == 0
             and u['pairLatencies']['p95NS'] is not None)
    return dict(valid=valid, F=int(cost['datagramBytes']) + int(cost['streamBytes']),
                U=int(cost['ipv4UDPBytes']) + int(cost['streamBytes']),
                M=Fraction(int(u['missed']), int(u['eligiblePairs'])) if int(u['eligiblePairs']) else None,
                T=int(u['pairLatencies']['p95NS']) if u['pairLatencies']['p95NS'] is not None else None)


def means(root, loss, mode, arm):
    values = []
    for replicate in [2, 3]:
        directory = root / 'evidence' / f'dev-32-loss-{loss}-exchange-{mode}-r{replicate}-{arm}'
        if not (directory / 'summary.json').exists() or (directory / 'exit-code').read_text().strip() != '0':
            return None
        value = metrics(json.loads((directory / 'summary.json').read_text()))
        if not value['valid']:
            return None
        values.append(value)
    return {key: sum(Fraction(v[key]) for v in values) / 2 for key in ['F', 'U', 'M', 'T']}


def select(root, declaration):
    selections = []
    for loss in [0, 10, 30]:
        for mode in ['on', 'off']:
            candidate = means(root, loss, mode, 'reply')
            points = [(arm, means(root, loss, mode, arm['id'])) for arm in declaration['arms'] if arm['id'] != 'reply']
            selected = dict(loss=loss, exchange=mode, L=None, H=None, candidate=candidate,
                            frontier=[dict(arm=a['id'], metrics=m) for a, m in points])
            # A missing development replicate invalidates the stratum, including
            # every grid point. Never average a survivor or narrow the grid.
            if candidate is None or any(m is None for _, m in points):
                selected['status'] = 'incomplete-development'
            else:
                lower = [(a, m) for a, m in points if m['F'] <= candidate['F'] and m['U'] <= candidate['U']]
                upper = [(a, m) for a, m in points if m['F'] >= candidate['F'] and m['U'] >= candidate['U']]
                if lower:
                    selected['L'] = min(lower, key=lambda p: (p[1]['M'], p[1]['T'], p[1]['U'], p[1]['F'], p[0]['RetransmitMult'], p[0]['GossipNodes']))[0]['id']
                if upper:
                    selected['H'] = min(upper, key=lambda p: (p[1]['U'], p[1]['F'], p[1]['M'], p[1]['T'], p[0]['RetransmitMult'], p[0]['GossipNodes']))[0]['id']
                selected['status'] = 'bracketed' if lower and upper else 'unbracketed'
                dominated = [a['id'] for a, m in points if m['F'] <= candidate['F'] and m['U'] <= candidate['U'] and m['M'] <= candidate['M'] and m['T'] <= candidate['T']]
                selected['pointDominators'] = dominated
                if selected['status'] == 'bracketed':
                    lo, hi = means(root, loss, mode, selected['L']), means(root, loss, mode, selected['H'])
                    selected['threshold1'] = (lo['M'] > 0 and lo['M'] - candidate['M'] >= Fraction(1, 1000)
                                              and candidate['M'] <= lo['M'] * Fraction(3, 4)
                                              and candidate['T'] <= lo['T'] * Fraction(9, 10))
                    selected['threshold2'] = all(candidate[k] <= hi[k] for k in ['M', 'T', 'F', 'U'])
            selections.append(selected)
    primary = next(s for s in selections if s['loss'] == 30 and s['exchange'] == 'off')
    advance = (primary['status'] == 'bracketed' and primary['threshold1'] and primary['threshold2']
               and not primary['pointDominators'])
    # A mean-only dominator does not establish equivalence if paired
    # replicate order reverses. Keep that uncertainty explicit at the screen.
    consistent = []
    for arm in primary.get('pointDominators', []):
        ordered = []
        for replicate in [2, 3]:
            baseline_path = root / 'evidence' / f'dev-32-loss-30-exchange-off-r{replicate}-{arm}' / 'summary.json'
            candidate_path = root / 'evidence' / f'dev-32-loss-30-exchange-off-r{replicate}-reply' / 'summary.json'
            baseline = metrics(json.loads(baseline_path.read_text()))
            candidate = metrics(json.loads(candidate_path.read_text()))
            ordered.append(all(baseline[k] <= candidate[k] for k in ['F', 'U', 'M', 'T']))
        if all(ordered):
            consistent.append(arm)
    primary['replicateConsistentDominators'] = consistent
    uncertain_dominance = bool(primary.get('pointDominators')) and not consistent
    return dict(advance=advance, stage='development-screen', strata=selections,
                disposition='advance-to-frozen-holdout' if advance else
                ('inconclusive' if primary['status'] != 'bracketed' or uncertain_dominance else 'screen-rejection'))


def encoded(value):
    if isinstance(value, Fraction):
        return str(value)
    raise TypeError(type(value).__name__)


if __name__ == '__main__':
    record = Path(__file__).resolve().parent
    root = Path(sys.argv[1]).resolve()
    result = select(root, json.loads((record / 'declaration.json').read_text()))
    print(json.dumps(result, default=encoded, indent=2, sort_keys=True))
