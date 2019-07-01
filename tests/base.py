import unittest

from mock import MagicMock, Mock

from elasticmagic import Cluster, Index


class BaseTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.client = MagicMock(
            info=Mock(return_value={'version': {'number': '5.6.2'}})
        )
        self.cluster = Cluster(self.client)
        self.index = Index(self.cluster, 'test')

    def assert_expression(self, expr, params):
        self.assertEqual(expr.to_dict(), params)


class OrderTolerantString(object):

    def __init__(self, line, sep):
        self.line = line
        self.sep = sep

    def __eq__(self, other):
        return set(self.line.split(self.sep)) == set(other.split(self.sep))
