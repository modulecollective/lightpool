#!/usr/bin/env python3
"""Inventory observed originals separately from deterministic measurements."""
import csv
import json
from pathlib import Path
import sys

raw_root, packed_root = map(Path, sys.argv[1:])
w = csv.writer(sys.stdout, delimiter='\t', lineterminator='\n')
w.writerow(['attempt', 'producer', 'exitCode', 'status', 'rawBytes', 'storedBytes',
            'elapsedNS', 'peakRSSBytes'])
for raw in sorted(raw_root.iterdir()):
    packed = packed_root / raw.name
    spec = json.loads((raw / 'spec.json').read_text())
    status = json.loads((raw / 'status.json').read_text())
    resources = json.loads((raw / 'resources.json').read_text())
    w.writerow([raw.name, spec['code'], (raw / 'exit-code').read_text().strip(),
                status['status'], sum(p.stat().st_size for p in raw.iterdir()),
                sum(p.stat().st_size for p in packed.iterdir()),
                resources['elapsedNS'], resources['peakRSSBytes']])
