from pathlib import Path
import tempfile
import unittest

import campaign


class CampaignPlanTest(unittest.TestCase):
    def test_development_has_exact_frozen_population(self):
        tasks=list(campaign.development())
        self.assertEqual(len(tasks),120)
        self.assertEqual(len({campaign.identity(t) for t in tasks}),120)
        self.assertEqual(sum(t['arm']=='reply' for t in tasks),12)
        self.assertEqual({t['replicate'] for t in tasks},{2,3})
        self.assertEqual({t['nodes'] for t in tasks},{32})

    def test_holdout_count_and_comparator_collapse(self):
        strata=[dict(loss=loss,exchange=mode,status='bracketed',L='r4-g3',H='r4-g4') for loss in [0,10,30] for mode in ['on','off']]
        self.assertEqual(len(list(campaign.holdout(dict(strata=strata)))),234)
        for s in strata:
            s['H']=s['L']
        self.assertEqual(len(list(campaign.holdout(dict(strata=strata)))),156)

    def test_stopped_campaign_cannot_automatically_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            out=Path(temporary)
            path=out/'progress.json'
            original='{"stop":"screen-rejection"}'
            path.write_text(original)
            with self.assertRaisesRegex(RuntimeError,'already stopped'):
                campaign.run(out,resume=True,maximum=1)
            self.assertEqual(path.read_text(),original)

    def test_inflight_work_requires_reconciliation(self):
        with tempfile.TemporaryDirectory() as temporary:
            out=Path(temporary)
            (out/'progress.json').write_text('{"stop":null}')
            (out/'active/original').mkdir(parents=True)
            with self.assertRaisesRegex(RuntimeError,'unaccounted in-flight'):
                campaign.run(out,resume=True,maximum=1)
            self.assertTrue((out/'active/original').is_dir())


if __name__=='__main__':
    unittest.main()
