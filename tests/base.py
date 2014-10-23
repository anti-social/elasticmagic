import unittest

from mock import MagicMock

from elasticmagic import Index
from elasticmagic.expression import Compiled


class BaseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.maxDiff = None

    def setUp(self):
        self.client = MagicMock()
        self.index = Index(self.client, 'test')

    def assert_expression(self, expr, params):
        c = Compiled(expr)
        self.assertEqual(c.params, params)

