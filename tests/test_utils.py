from .base import BaseTestCase

from elasticmagic.util import merge_params
from elasticmagic.expression import Params


class UtilsTest(BaseTestCase):

    def test_merge_params(self):
        original = Params()
        self.assertEqual(tuple(original.items()), ())

        p = merge_params(original, (), {})
        self.assertEqual(original, p)
        self.assertIsNot(original, p)

        p = merge_params(original, ({'key': 'value'},), {})
        self.assertEqual(tuple(p.items()), (('key', 'value'),))
        self.assertNotEqual(original, p)
        self.assertIsNot(original, p)

        p = merge_params(original, (), dict(key='value'))
        self.assertEqual(tuple(p.items()), (('key', 'value'),))
        self.assertNotEqual(original, p)
        self.assertIsNot(original, p)

        p = merge_params(p, ({'key': 'new value'}, ), {'foo': 'bar'})
        self.assertEqual(sorted(p.items()),
                         [('foo', 'bar'), ('key', 'new value')])

        self.assertRaises(AssertionError,
                          lambda: merge_params(original, None, {}))
        self.assertRaises(AssertionError,
                          lambda: merge_params(original, object(), {}))
        self.assertRaises(AssertionError,
                          lambda: merge_params(original, (), None))
        self.assertRaises(AssertionError,
                          lambda: merge_params(original, (), []))
