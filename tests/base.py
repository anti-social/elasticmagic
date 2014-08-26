import unittest

from elasticmagic.expression import Compiled


class BaseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.maxDiff = None

    def assert_expression(self, expr, params):
        c = Compiled(expr)
        self.assertEqual(params, c.params)

