#!/usr/bin/env python3
"""Serial driver for this declared campaign; no retries or outcome-based seeds."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import analysis
import rows

RECORD = Path(__file__).resolve().parent
REPO = RECORD.parent.parent
DECLARATION = json.loads((RECORD / 'declaration.json').read_text())


def dump(path, value):
    path.write_text(json.dumps(value, default=analysis.encoded, indent=2, sort_keys=True) + '\n')


def development():
    for loss in [0, 10, 30]:
        for mode in ['on', 'off']:
            for replicate in [2, 3]:
                for arm in DECLARATION['arms']:
                    yield dict(phase='dev', nodes=32, loss=loss, exchange=mode, replicate=replicate, arm=arm['id'], latencyMS=1)


def holdout(selection):
    for phase in DECLARATION['holdout']:
        for loss in phase['loss']:
            for mode in ['on', 'off']:
                stratum = next(s for s in selection['strata'] if s['loss'] == loss and s['exchange'] == mode)
                if stratum['status'] != 'bracketed':
                    continue
                for replicate in phase['replicates']:
                    for arm in dict.fromkeys(['reply', stratum['L'], stratum['H']]):
                        yield dict(phase=phase['id'], nodes=phase['nodes'], loss=loss, exchange=mode,
                                   replicate=replicate, arm=arm, latencyMS=phase['latencyMS'])


def identity(task):
    return f"{task['phase']}-{task['nodes']}-loss-{task['loss']}-exchange-{task['exchange']}-r{task['replicate']}-{task['arm']}"


def run(out):
    out.mkdir()  # Never reuse a directory, failed or otherwise.
    started = time.monotonic()
    subprocess.run(['bash', str(RECORD / 'prepare.sh'), str(out / 'tools')], check=True)
    for directory in ['active', 'evidence', 'verification', 'overlays']:
        (out / directory).mkdir()
    for arm in DECLARATION['arms']:
        dump(out / 'overlays' / (arm['id'] + '.json'), {k: arm[k] for k in ['RetransmitMult', 'GossipNodes']})
    tasks = list(development())
    dump(out / 'development-plan.json', tasks)
    inventory = []
    deterministic_rows = ['\t'.join(rows.fields)]
    stop = None
    def checkpoint():
        dump(out / 'inventory.json', inventory)
        (out / 'cells.tsv').write_text('\n'.join(deterministic_rows) + '\n')
        dump(out / 'progress.json', dict(completed=len(inventory), elapsedSeconds=time.monotonic()-started, stop=stop,
             rawBytes=sum(i['rawBytes'] for i in inventory), storedBytes=sum(i['storedBytes'] for i in inventory)))
    def record(task):
        nonlocal stop
        cell = identity(task)
        # The accepted sizing probe supplies the as-yet-unobserved 128-node
        # estimate. Use observed means by size as soon as they exist.
        def estimate(nodes, fallback):
            observed = [i['storedBytes'] for i in inventory if i['nodes'] == nodes]
            return sum(observed)/len(observed) if observed else fallback
        projected = estimate(32, 45085093/6)*282 + estimate(128, 65287874)*72
        stored = sum(i['storedBytes'] for i in inventory)
        # Reserve Git's eventual copy of all projected packed evidence, plus
        # both capped raw/restored attempts and a 512 MiB working allowance.
        required = (max(stored, projected)-stored) + max(stored, projected) + 2*(2 << 30) + (512 << 20)
        if max(stored, projected) > DECLARATION['retainedBudgetBytes']:
            stop = 'resource-limited: projected retained bytes exceed 8 GiB'
        elif time.monotonic()-started >= DECLARATION['activeBudgetSeconds']:
            stop = 'resource-limited: campaign active work reached 24 hours'
        elif shutil.disk_usage(out).free < required:
            stop = 'resource-limited: insufficient free disk for evidence, Git and two capped attempts'
        if stop:
            checkpoint()
            return False
        raw, packed = out / 'active' / cell, out / 'evidence' / cell
        executable = out / 'tools' / ('candidate-experiment' if task['arm'] == 'reply' else 'baseline-experiment')
        case = f"delivery-{task['nodes']}-loss-{task['loss']}-exchange-{task['exchange']}"
        if task['latencyMS'] == 10:
            case += '-latency-10ms'
        patch = DECLARATION['replyPatchSHA256'] if task['arm'] == 'reply' else 'none'
        source = f"tools:{DECLARATION['toolsBase']};protocol:{DECLARATION['base']};patch:{patch}"
        command = [str(executable), '-case', case, '-replicate', str(task['replicate']), '-revision', source,
                   '-protocol', str(out / 'overlays' / (task['arm'] + '.json')), '-out', str(raw)]
        before = time.monotonic()
        free_before = shutil.disk_usage(out).free
        stdout, stderr = out / (cell + '.stdout'), out / (cell + '.stderr')
        with stdout.open('wb') as a, stderr.open('wb') as b:
            try:
                result = subprocess.run(command, stdout=a, stderr=b,
                                        timeout=max(1, DECLARATION['activeBudgetSeconds']-(time.monotonic()-started)))
                code = result.returncode
            except subprocess.TimeoutExpired:
                code = 124
                stop = 'resource-limited: in-flight attempt exceeded campaign active budget'
        raw.mkdir(exist_ok=True)
        stdout.rename(raw / 'command.stdout')
        stderr.rename(raw / 'command.stderr')
        (raw / 'exit-code').write_text(str(code)+'\n')
        dump(raw / 'attempt.json', dict(task=task, command=command, source=source, projectedStoredBytes=projected,
                                       freeBytesBefore=free_before))
        raw_bytes = sum(p.stat().st_size for p in raw.iterdir())
        subprocess.run(['bash', str(REPO / 'experiments/evidence.sh'), 'pack', str(raw), str(packed)], check=True)
        restored = out / 'verification' / cell
        subprocess.run(['bash', str(REPO / 'experiments/evidence.sh'), 'unpack', str(packed), str(restored)], check=True)
        verification = dict(cell=cell, checksumRestoration=True)
        if (raw / 'summary.json').exists():
            reduced = out / 'verification' / (cell + '.json')
            with (out / 'verification' / (cell + '.stdout')).open('wb') as a, (out / 'verification' / (cell + '.stderr')).open('wb') as b:
                result = subprocess.run([str(executable), '-reduce', str(restored), '-out', str(reduced)], stdout=a, stderr=b)
            verification['offlineSummaryMatch'] = reduced.exists() and reduced.read_bytes() == (raw / 'summary.json').read_bytes()
            verification['reductionExitMatch'] = result.returncode == code
            if not verification['offlineSummaryMatch'] or not verification['reductionExitMatch']:
                dump(out / 'verification' / (cell + '.verification.json'), verification)
                raise RuntimeError(f'{cell}: offline reduction differs; raw and restored evidence retained')
            reduced.unlink()
        else:
            verification['offlineSummaryMatch'] = None
            stop = 'incomplete evidence: attempt has no summary; diagnostics retained'
        if (raw / 'spec.json').exists() and (raw / 'status.json').exists():
            deterministic_rows.append(rows.row(raw))
        else:
            stop = 'incomplete evidence: attempt has no spec/status; diagnostics retained'
        resource_path = raw / 'resources.json'
        resources = json.loads(resource_path.read_text()) if resource_path.exists() else {}
        inventory.append(dict(cell=cell, **task, exitCode=code, rawBytes=raw_bytes,
                              storedBytes=sum(p.stat().st_size for p in packed.iterdir()),
                              originalResources=resources, totalWorkSeconds=time.monotonic()-before))
        dump(out / 'verification' / (cell + '.verification.json'), verification)
        checkpoint()
        # These two paths were created exclusively by this invocation and
        # checksummed against the retained original before removal.
        shutil.rmtree(raw)
        shutil.rmtree(restored)
        print(f'{len(inventory)} {cell}: exit={code}, exact restoration and offline reduction match', flush=True)
        return stop is None
    for task in tasks:
        if not record(task):
            break
    if len(inventory) == len(tasks) and stop is None:
        selection = analysis.select(out, DECLARATION)
        # This immutable file precedes every held-out launch.
        dump(out / 'comparators.json', selection)
        if selection['advance']:
            tasks = list(holdout(selection))
            dump(out / 'heldout-plan.json', tasks)
            for task in tasks:
                if not record(task):
                    break
        else:
            stop = selection['disposition'] + ': held-out plan not launched'
    checkpoint()
    launched = {i['cell'] for i in inventory}
    unlaunched = [t for t in list(development()) if identity(t) not in launched]
    if not (out / 'heldout-plan.json').exists():
        for phase in DECLARATION['holdout']:
            for loss in phase['loss']:
                for mode in ['on', 'off']:
                    for replicate in phase['replicates']:
                        for arm in ['reply', 'L', 'H']:
                            unlaunched.append(dict(phase=phase['id'], nodes=phase['nodes'], loss=loss,
                                exchange=mode, replicate=replicate, arm=arm, latencyMS=phase['latencyMS']))
    else:
        unlaunched.extend(t for t in tasks if identity(t) not in launched)
    dump(out / 'unlaunched.json', unlaunched)
    print(json.dumps(json.loads((out / 'progress.json').read_text()), sort_keys=True), flush=True)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: campaign.py NEW_DIRECTORY')
    run(Path(sys.argv[1]).resolve())
