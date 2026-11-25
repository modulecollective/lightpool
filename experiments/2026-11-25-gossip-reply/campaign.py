#!/usr/bin/env python3
"""Serial driver for this declared campaign; no retries or outcome-based seeds."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
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


def run(out, resume=False, maximum=None):
    previous = None
    if resume:
        previous = json.loads((out / 'progress.json').read_text())
        if previous['stop'] is not None:
            raise RuntimeError('campaign already stopped; no automatic recovery or retries')
        if any((out / 'active').iterdir()):
            raise RuntimeError('unaccounted in-flight work: preserve and reconcile it before continuation')
    else:
        out.mkdir()  # Fresh invocations never reuse existing output.
    # Conservatively include time between bounded invocations in the budget.
    prior_elapsed = previous['elapsedSeconds'] + max(0, time.time()-(out / 'progress.json').stat().st_mtime) if previous else 0
    started = time.monotonic()-prior_elapsed
    if not resume:
        subprocess.run(['bash', str(RECORD / 'prepare.sh'), str(out / 'tools')], check=True)
    for directory in ['active', 'evidence', 'verification', 'overlays']:
        if not resume:
            (out / directory).mkdir()
    for arm in DECLARATION['arms']:
        if not resume:
            dump(out / 'overlays' / (arm['id'] + '.json'), {k: arm[k] for k in ['RetransmitMult', 'GossipNodes']})
    tasks = list(development())
    if not resume:
        dump(out / 'development-plan.json', tasks)
    inventory = json.loads((out / 'inventory.json').read_text()) if resume else []
    deterministic_rows = (out / 'cells.tsv').read_text().splitlines() if resume else ['\t'.join(rows.fields)]
    stop = None
    launched = {i['cell'] for i in inventory}
    recorded = 0
    batch_complete = False
    def checkpoint():
        dump(out / 'inventory.json', inventory)
        (out / 'cells.tsv').write_text('\n'.join(deterministic_rows) + '\n')
        dump(out / 'progress.json', dict(completed=len(inventory), elapsedSeconds=time.monotonic()-started, stop=stop,
             rawBytes=sum(i['rawBytes'] for i in inventory), storedBytes=sum(i['storedBytes'] for i in inventory)))
    def record(task):
        nonlocal stop, recorded, batch_complete
        cell = identity(task)
        if cell in launched:
            return True
        if maximum is not None and recorded >= maximum:
            batch_complete = True
            return False
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
        subprocess.run(['bash', str(out / 'tools/baseline/experiments/evidence.sh'), 'pack', str(raw), str(packed)], check=True)
        restored = out / 'verification' / cell
        subprocess.run(['bash', str(out / 'tools/baseline/experiments/evidence.sh'), 'unpack', str(packed), str(restored)], check=True)
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
        launched.add(cell)
        recorded += 1
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
    if all(identity(t) in launched for t in tasks) and stop is None:
        selection_path = out / 'comparators.json'
        if selection_path.exists():
            selection = json.loads(selection_path.read_text())
        else:
            selection = analysis.select(out, DECLARATION)
            # Written once, before every held-out launch.
            dump(selection_path, selection)
        if selection['advance']:
            tasks = list(holdout(selection))
            if not (out / 'heldout-plan.json').exists():
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
    print(json.dumps(dict(json.loads((out / 'progress.json').read_text()), batchComplete=batch_complete), sort_keys=True), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--resume', action='store_true', help='continue only unlaunched cells of a reconciled campaign')
    parser.add_argument('--max-attempts', type=int, help='return after this many new attempts without changing the plan')
    args = parser.parse_args()
    if args.max_attempts is not None and args.max_attempts < 1:
        parser.error('--max-attempts must be positive')
    run(args.directory.resolve(), args.resume, args.max_attempts)
