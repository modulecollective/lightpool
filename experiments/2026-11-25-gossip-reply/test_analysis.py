#!/usr/bin/env python3
"""Selection regression tests use only invented outcomes, never campaign data."""
from fractions import Fraction
import json
from pathlib import Path
import tempfile
import unittest

import analysis


class SelectionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.declaration = json.loads((Path(__file__).parent / 'declaration.json').read_text())
        for loss in [0, 10, 30]:
            for mode in ['on', 'off']:
                for arm in self.declaration['arms']:
                    for replicate in [2, 3]:
                        value = self.summary(150, 250, 6, 100)
                        if arm['id'] == 'r4-g3':
                            value = self.summary(90, 190, 12, 120)
                        elif arm['id'] == 'r4-g4':
                            value = self.summary(110, 210, 6, 100)
                        elif arm['id'] == 'reply':
                            value = self.summary(100, 200, 6, 100)
                        directory = self.directory(loss, mode, replicate, arm['id'])
                        directory.mkdir(parents=True)
                        (directory / 'exit-code').write_text('0\n')
                        (directory / 'summary.json').write_text(json.dumps(value))

    def directory(self, loss, mode, replicate, arm):
        return self.root / 'evidence' / f'dev-32-loss-{loss}-exchange-{mode}-r{replicate}-{arm}'

    @staticmethod
    def summary(framed, udp, missed, tail):
        return dict(status='complete', updateDelivery=dict(status='complete', generated='20', failed='0',
                    incompletePairs='0', generationFailedPairs='0', eligiblePairs='620', missed=str(missed),
                    pairLatencies=dict(p95NS=str(tail) if tail is not None else None)),
                    trafficCost=dict(measured=dict(attempted=dict(datagramBytes=str(framed), streamBytes='0', ipv4UDPBytes=str(udp)))))

    def result(self):
        return analysis.select(self.root, self.declaration)

    def replace(self, value, arm='r4-g3', replicate=2):
        (self.directory(30, 'off', replicate, arm) / 'summary.json').write_text(json.dumps(value))

    def test_exact_means_and_frozen_selection(self):
        result = self.result()
        self.assertTrue(result['advance'])
        for stratum in result['strata']:
            self.assertEqual((stratum['L'], stratum['H']), ('r4-g3', 'r4-g4'))
            self.assertEqual(stratum['candidate']['M'], Fraction(6, 620))

    def test_invalid_replicate_never_averages_survivor(self):
        value = self.summary(90, 190, 12, 120)
        value['status'] = 'invalid-warmup'
        self.replace(value)
        result = self.result()
        self.assertFalse(result['advance'])
        self.assertEqual(result['disposition'], 'inconclusive')
        self.assertIsNone(analysis.means(self.root, 30, 'off', 'r4-g3'))

    def test_null_tail_cannot_win(self):
        self.replace(self.summary(90, 190, 12, None))
        self.assertFalse(self.result()['advance'])

    def test_both_costs_must_bracket(self):
        for replicate in [2, 3]:
            self.replace(self.summary(90, 201, 12, 120), replicate=replicate)
        result = self.result()
        self.assertFalse(result['advance'])
        self.assertEqual(result['strata'][-1]['status'], 'unbracketed')

    def test_zero_misses_cannot_establish_benefit(self):
        for replicate in [2, 3]:
            self.replace(self.summary(90, 190, 0, 120), replicate=replicate)
        result = self.result()
        self.assertFalse(result['advance'])
        self.assertFalse(result['strata'][-1]['threshold1'])

    def test_reject_practical_threshold_without_claiming_equivalence(self):
        for replicate in [2, 3]:
            self.replace(self.summary(100, 200, 11, 115), 'reply', replicate)
        self.assertFalse(self.result()['advance'])
        self.assertEqual(self.result()['disposition'], 'screen-rejection')

    def test_equal_cost_point_qualifies_on_both_sides(self):
        for replicate in [2, 3]:
            self.replace(self.summary(100, 200, 6, 100), 'r4-g4', replicate)
        result = self.result()
        self.assertFalse(result['advance'])
        self.assertEqual((result['strata'][-1]['L'], result['strata'][-1]['H']), ('r4-g4', 'r4-g4'))


if __name__ == '__main__':
    unittest.main()
