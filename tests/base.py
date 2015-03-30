import unittest

from mock import MagicMock

from elasticmagic import Cluster, Index
from elasticmagic.expression import QueryCompiled


class BaseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.maxDiff = None

    def setUp(self):
        self.client = MagicMock()
        self.cluster = Cluster(self.client)
        self.index = Index(self.cluster, 'test')

    def assert_expression(self, expr, params):
        c = QueryCompiled(expr)
        self.assertEqual(c.params, params)

