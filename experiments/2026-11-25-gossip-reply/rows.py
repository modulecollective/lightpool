#!/usr/bin/env python3
"""One deterministic row per retained attempt; host observations stay separate."""
import hashlib
import json
from pathlib import Path
import sys

fields = ['cell', 'case', 'replicate', 'identity', 'exitCode', 'status',
          'specSHA256', 'scenarioSHA256', 'traceSHA256', 'summarySHA256',
          'deliveryStatus', 'scheduled', 'generated', 'failed', 'incompleteUpdates',
          'nominalPairs', 'eligiblePairs', 'delivered', 'missed',
          'generationFailedPairs', 'incompletePairs',
          'pairCompleted', 'pairP50NS', 'pairP95NS', 'pairMaxNS',
          'allCompleted', 'allP50NS', 'allP95NS', 'allMaxNS']
for interval in ['measured', 'wholeRun']:
    for stage in ['attempted', 'sent']:
        for field in ['datagramCount', 'datagramBytes', 'streamBytes', 'ipv4UDPBytes']:
            fields.append(f'{interval}_{stage}_{field}')


def row(directory):
    d = Path(directory)
    spec = json.loads((d / 'spec.json').read_text())
    status = json.loads((d / 'status.json').read_text())
    values = [d.name, spec['case'], spec['replicate'], spec['code'],
              (d / 'exit-code').read_text().strip(), status['status']]
    for name in ['spec.json', 'scenario.json', 'trace.ndjson', 'summary.json']:
        p = d / name
        h = hashlib.sha256()
        if p.exists():
            with p.open('rb') as f:
                for block in iter(lambda: f.read(1 << 20), b''):
                    h.update(block)
            values.append(h.hexdigest())
        else:
            values.append('-')
    if (d / 'summary.json').exists():
        s = json.loads((d / 'summary.json').read_text())
        u = s['updateDelivery']
        values.extend(u[k] for k in ['status', 'scheduled', 'generated', 'failed',
                      'incompleteUpdates', 'nominalPairs', 'eligiblePairs',
                      'delivered', 'missed', 'generationFailedPairs', 'incompletePairs'])
        for key in ['pairLatencies', 'allReceiverTimes']:
            values.extend(u[key][k] for k in ['completed', 'p50NS', 'p95NS', 'maxNS'])
        for interval in ['measured', 'wholeRun']:
            for stage in ['attempted', 'sent']:
                values.extend(s['trafficCost'][interval][stage][k] for k in
                              ['datagramCount', 'datagramBytes', 'streamBytes', 'ipv4UDPBytes'])
    values.extend(['-'] * (len(fields) - len(values)))
    assert len(values) == len(fields)
    return '\t'.join('null' if v is None else str(v) for v in values)


if __name__ == '__main__':
    print('\t'.join(fields) if sys.argv[1:] == ['--header'] else row(sys.argv[1]))
