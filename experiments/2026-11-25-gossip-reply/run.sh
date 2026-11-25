#!/usr/bin/env bash
# Regenerate one retained deterministic row. Original host costs are not replayed.
set -euo pipefail
[ "$#" -eq 2 ] || { echo 'usage: run.sh CELL NEW_DIRECTORY' >&2; exit 1; }
record=$(cd "$(dirname "$0")" && pwd)
cell=$1
[[ $cell =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]]
[ -f "$record/evidence/$cell/attempt.json" ]
mkdir "$2"
out=$(cd "$2" && pwd)
bash "$record/prepare.sh" "$out/tools"
python3 - "$record" "$cell" "$out" <<'PY'
import csv,json,subprocess,sys
from pathlib import Path
record,cell,out=Path(sys.argv[1]),sys.argv[2],Path(sys.argv[3])
sys.path.insert(0,str(record))
import rows
saved=record/'evidence'/cell
attempt=json.loads((saved/'attempt.json').read_text())
task=attempt['task']
executable=out/'tools'/('candidate-experiment' if task['arm']=='reply' else 'baseline-experiment')
spec=json.loads((saved/'spec.json').read_text())
raw=out/cell
with (out/'command.stdout').open('wb') as a,(out/'command.stderr').open('wb') as b:
 code=subprocess.run([str(executable),'-case',spec['case'],'-replicate',spec['replicate'],
   '-revision',spec['code'],'-protocol',str(record/'overlays'/(task['arm']+'.json')),'-out',str(raw)],stdout=a,stderr=b).returncode
raw.mkdir(exist_ok=True)
(out/'command.stdout').rename(raw/'command.stdout')
(out/'command.stderr').rename(raw/'command.stderr')
(raw/'exit-code').write_text(str(code)+'\n')
line=rows.row(raw)
expected=next(r for r in csv.reader((record/'cells.tsv').open(),delimiter='\t') if r[0]==cell)
(out/'row.tsv').write_text(line+'\n')
if line.split('\t') != expected:
 raise SystemExit('regenerated row differs; work retained')
print(cell+': regenerated deterministic row matches')
PY
