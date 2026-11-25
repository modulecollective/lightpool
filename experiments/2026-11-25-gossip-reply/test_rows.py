import json
from pathlib import Path
import tempfile
import unittest

import rows


class IncompleteOriginalTest(unittest.TestCase):
    def test_absent_producer_finalization_stays_unavailable(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)/'interrupted-original'
            directory.mkdir()
            (directory/'spec.json').write_text(json.dumps(dict(case='delivery-32-loss-0-exchange-off',replicate='2',code='pinned-source')))
            values=dict(zip(rows.fields,rows.row(directory).split('\t')))
            self.assertEqual(values['status'],'missing-producer-status')
            self.assertEqual(values['exitCode'],'unobserved')
            self.assertEqual(values['summarySHA256'],'-')
            self.assertEqual(values['generated'],'-')
            self.assertFalse((directory/'status.json').exists())
            self.assertFalse((directory/'exit-code').exists())


if __name__=='__main__':
    unittest.main()
