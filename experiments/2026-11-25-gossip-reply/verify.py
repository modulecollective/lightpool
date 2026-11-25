#!/usr/bin/env python3
"""Restore committed originals individually; replay the pinned offline reducer."""
import csv
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

import rows

RECORD = Path(__file__).resolve().parent
REPO = RECORD.parent.parent
RELATIVE = RECORD.relative_to(REPO)


def verify(out):
    out.mkdir()
    subprocess.run(['bash', str(RECORD / 'prepare.sh'), str(out / 'tools')], check=True)
    with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git', '-C', str(REPO), 'archive', 'HEAD',
                          str(RELATIVE / 'inventory.json'), str(RELATIVE / 'cells.tsv')]))) as archive:
        archive.extractall(out / 'committed', filter='data')
    committed = out / 'committed' / RELATIVE
    inventory = json.loads((committed / 'inventory.json').read_text())
    expected = {r['cell']: r for r in csv.DictReader((committed / 'cells.tsv').open(), delimiter='\t')}
    actual_rows = ['\t'.join(rows.fields)]
    for entry in inventory:
        cell = entry['cell']
        payload = out / 'payload'
        with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git', '-C', str(REPO), 'archive', 'HEAD',
                                           str(RELATIVE / 'evidence' / cell)]))) as archive:
            archive.extractall(payload, filter='data')
        packed = payload / RELATIVE / 'evidence' / cell
        raw = out / cell
        subprocess.run(['bash', str(out / 'tools/baseline/experiments/evidence.sh'), 'unpack', str(packed), str(raw)], check=True)
        with (raw / 'attempt.json').open() as f:
            attempt = json.load(f)
        executable = out / 'tools' / ('candidate-experiment' if attempt['task']['arm'] == 'reply' else 'baseline-experiment')
        if (raw / 'summary.json').exists():
            reduced = out / (cell + '.reduced.json')
            with (out / (cell + '.reduce.stdout')).open('wb') as a, (out / (cell + '.reduce.stderr')).open('wb') as b:
                code = subprocess.run([str(executable), '-reduce', str(raw), '-out', str(reduced)], stdout=a, stderr=b).returncode
            if code != int((raw / 'exit-code').read_text()) or not reduced.exists() or reduced.read_bytes() != (raw / 'summary.json').read_bytes():
                raise RuntimeError(f'{cell}: reduction differs; all work retained')
            reduced.unlink()
        else:
            raise RuntimeError(f'{cell}: no reducible summary; original diagnostics retained')
        line = rows.row(raw)
        got = dict(zip(rows.fields, line.split('\t')))
        if got != expected[cell]:
            raise RuntimeError(f'{cell}: deterministic row differs; work retained')
        with (out / (cell + '.checksums')).open('wb') as a:
            subprocess.run(['sha256sum', '-c', str(packed / 'SHA256SUMS')], cwd=raw, stdout=a, check=True)
        actual_rows.append(line)
        print(cell + ': Git payload checksums, offline summary, exit code and deterministic row match', flush=True)
        shutil.rmtree(raw)
        shutil.rmtree(payload)
    if len(inventory) != len(expected):
        raise RuntimeError('inventory and deterministic rows disagree')
    (out / 'cells.tsv').write_text('\n'.join(actual_rows)+'\n')
    print(f'{len(inventory)} committed original attempts verified', flush=True)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: verify.py NEW_DIRECTORY')
    verify(Path(sys.argv[1]).resolve())
