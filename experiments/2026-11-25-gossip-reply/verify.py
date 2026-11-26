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


def verify(out, selected_cell=None):
    out.mkdir()
    subprocess.run(['bash', str(RECORD / 'prepare.sh'), str(out / 'tools')], check=True)
    with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git', '-C', str(REPO), 'archive', 'HEAD',
                          str(RELATIVE / 'inventory.json'), str(RELATIVE / 'cells.tsv')]))) as archive:
        archive.extractall(out / 'committed', filter='data')
    committed = out / 'committed' / RELATIVE
    inventory = json.loads((committed / 'inventory.json').read_text())
    expected = {r['cell']: r for r in csv.DictReader((committed / 'cells.tsv').open(), delimiter='\t')}
    if selected_cell is not None:
        inventory = [entry for entry in inventory if entry['cell'] == selected_cell]
        if not inventory:
            raise ValueError('cell is not in committed inventory')
        expected = {selected_cell: expected[selected_cell]}
    actual_rows = ['\t'.join(rows.fields)]
    for entry in inventory:
        cell = entry['cell']
        payload = out / 'payload'
        with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git', '-C', str(REPO), 'archive', 'HEAD',
                                           str(RELATIVE / 'evidence' / cell)]))) as archive:
            archive.extractall(payload, filter='data')
        packed = payload / RELATIVE / 'evidence' / cell
        # Git holds the attempt's SHA256SUMS; evidence-store.sh fetch puts the payloads in the tree.
        fetched = [p for p in (RECORD / 'evidence' / cell).iterdir() if p.name != 'SHA256SUMS']
        if not fetched:
            raise RuntimeError(f'{cell}: payloads absent; run experiments/evidence-store.sh fetch {RECORD.name} first')
        for p in fetched:
            shutil.copyfile(p, packed / p.name)
        raw = out / cell
        subprocess.run(['bash', str(out / 'tools/baseline/experiments/evidence.sh'), 'unpack', str(packed), str(raw)], check=True)
        with (raw / 'attempt.json').open() as f:
            attempt = json.load(f)
        executable = out / 'tools' / ('candidate-experiment' if attempt['task']['arm'] == 'reply' else 'baseline-experiment')
        before_files = sorted(p.name for p in raw.iterdir())
        if (raw / 'summary.json').exists():
            reduced = out / (cell + '.reduced.json')
            with (out / (cell + '.reduce.stdout')).open('wb') as a, (out / (cell + '.reduce.stderr')).open('wb') as b:
                code = subprocess.run([str(executable), '-reduce', str(raw), '-out', str(reduced)], stdout=a, stderr=b).returncode
            if code != int((raw / 'exit-code').read_text()) or not reduced.exists() or reduced.read_bytes() != (raw / 'summary.json').read_bytes():
                raise RuntimeError(f'{cell}: reduction differs; all work retained')
            reduced.unlink()
        elif entry.get('producerStatusAbsent') and not (raw / 'status.json').exists():
            # Absence is original evidence; never manufacture producer status.
            reduced = out / (cell + '.unavailable.json')
            result = subprocess.run([str(executable), '-reduce', str(raw), '-out', str(reduced)], capture_output=True, text=True)
            (out / (cell + '.refusal.stdout')).write_text(result.stdout)
            (out / (cell + '.refusal.stderr')).write_text(result.stderr)
            if result.returncode == 0 or reduced.exists() or 'status.json' not in result.stderr:
                raise RuntimeError(f'{cell}: missing-status refusal changed; work retained')
        else:
            raise RuntimeError(f'{cell}: no reducible summary; original diagnostics retained')
        line = rows.row(raw)
        got = dict(zip(rows.fields, line.split('\t')))
        if got != expected[cell]:
            raise RuntimeError(f'{cell}: deterministic row differs; work retained')
        with (out / (cell + '.checksums')).open('wb') as a:
            subprocess.run(['sha256sum', '-c', str(packed / 'SHA256SUMS')], cwd=raw, stdout=a, check=True)
        if sorted(p.name for p in raw.iterdir()) != before_files:
            raise RuntimeError(f'{cell}: reduction mutated original payload set')
        actual_rows.append(line)
        detail = 'offline summary and exit code match' if (raw / 'summary.json').exists() else 'missing producer status remains unavailable; reducer refuses'
        print(cell + ': Git payload checksums and deterministic row match; ' + detail, flush=True)
        shutil.rmtree(raw)
        shutil.rmtree(payload)
    if len(inventory) != len(expected):
        raise RuntimeError('inventory and deterministic rows disagree')
    (out / 'cells.tsv').write_text('\n'.join(actual_rows)+'\n')
    print(f'{len(inventory)} committed original attempts verified', flush=True)


if __name__ == '__main__':
    if len(sys.argv) not in [2, 3]:
        raise SystemExit('usage: verify.py NEW_DIRECTORY [CELL]')
    verify(Path(sys.argv[1]).resolve(), sys.argv[2] if len(sys.argv) == 3 else None)
