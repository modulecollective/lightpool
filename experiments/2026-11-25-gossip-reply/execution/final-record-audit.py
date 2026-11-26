from pathlib import Path
import hashlib,json,sys,subprocess
record=Path('experiments/2026-11-25-gossip-reply').resolve();sys.path.insert(0,str(record))
import analysis,campaign
inventory=json.loads((record/'inventory.json').read_text());declaration=json.loads((record/'declaration.json').read_text());comparators=json.loads((record/'comparators.json').read_text());result=json.loads((record/'results.json').read_text());unlaunched=json.loads((record/'unlaunched.json').read_text())
expected={campaign.identity(t) for t in campaign.development()};actual=[i['cell'] for i in inventory]
assert len(inventory)==len(set(actual))==120 and set(actual)==expected
assert json.loads(json.dumps(analysis.select(record,declaration),default=analysis.encoded))==comparators
assert comparators['disposition']=='screen-rejection' and not comparators['advance']
assert len(unlaunched)==234 and all(t['phase']!='dev' for t in unlaunched)
missing=[]
for i in inventory:
 p=record/'evidence'/i['cell']/'summary.json'
 if not p.exists(): missing.append(i['cell']);assert i['exitCode'] is None and not i['originalResources'];continue
 s=json.loads(p.read_text());u=s['updateDelivery']
 assert i['exitCode']==0 and s['status']=='complete' and analysis.metrics(s)['valid']
 assert int(u['generated'])==20 and int(u['eligiblePairs'])==620 and int(u['missed'])+int(u['delivered'])==620
assert missing==['dev-32-loss-0-exchange-off-r2-r6-g4']
assert not (record/'evidence'/missing[0]/'status.json').exists()
frozen=subprocess.check_output(['bash','experiments/revision.sh','593105a8074104bc7c835d835f42ba14789c910e'],text=True).strip()
for name in ['declaration.json','reply.patch','ordinary-test.patch','PATCHES.sha256']:
 assert (record/name).read_bytes()==subprocess.check_output(['git','show',f'{frozen}:experiments/2026-11-25-gossip-reply/{name}'])
assert result['originalCosts']['resourcesAvailable']==119 and result['originalCosts']['resourcesUnavailable']==1
assert result['originalCosts']['rawBytes']==9299945103 and result['originalCosts']['storedBytes']==912364757
assert len(result['attempts'])==120 and result['comparators']==comparators
primary=next(s for s in comparators['strata'] if s['loss']==30 and s['exchange']=='off')
assert primary['L']=='r4-g3' and primary['H']=='r4-g4' and not primary['threshold1'] and not primary['threshold2']
pdf_review=json.loads((record/'execution'/'pdf-review.json').read_text())
assert pdf_review['sha256']==hashlib.sha256((record/'paper.pdf').read_bytes()).hexdigest()
audit=dict(sourceRevision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),originals=120,complete=119,unavailable=missing,heldoutUnlaunched=234,allFrozenIdentitiesMatch=True,preOutcomeDeclarationAndPatchesIdentical=True,comparatorsRecomputedExactly=True,originalCosts=result['originalCosts'],pdf=pdf_review)
(record/'execution'/'final-record-audit.json').write_text(json.dumps(audit,indent=2)+'\n')
print('120 unique frozen originals; 119 complete / 1 unavailable; exact selector replay; 234 unlaunched holdouts; frozen declaration/patches unchanged; PDF checked')
