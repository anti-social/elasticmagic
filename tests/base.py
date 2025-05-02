import unittest
from unittest.mock import MagicMock

from elasticmagic import Cluster, Index
from elasticmagic.compiler import Compiler_6_0


class BaseTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.client = MagicMock()
        self.cluster = Cluster(self.client, compiler=Compiler_6_0)
        self.index = Index(self.cluster, 'test')

    def assert_expression(self, expr, expected, compiler=None):
        compiler = compiler or Compiler_6_0
        self.assertEqual(expr.to_dict(compiler=compiler), expected)


class OrderTolerantString(object):

    def __init__(self, line, sep):
        self.line = line
        self.sep = sep

    def __eq__(self, other):
        return set(self.line.split(self.sep)) == set(other.split(self.sep))
