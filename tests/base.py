import unittest

from mock import MagicMock

from elasticmagic import Cluster, Index


class BaseTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.client = MagicMock()
        self.cluster = Cluster(self.client)
        self.index = Index(self.cluster, 'test')

    def assert_expression(self, expr, params):
        self.assertEqual(expr.to_dict(), params)
