from mock import MagicMock

import pytest

from elasticmagic import Cluster, Index
from elasticmagic.compiler import all_compilers


@pytest.fixture(params=all_compilers)
def compiler(request):
    return request.param


@pytest.fixture
def client():
    yield MagicMock()


@pytest.fixture
def cluster(client, compiler):
    yield Cluster(client, compiler=compiler)


@pytest.fixture
def index(cluster):
    yield Index(cluster, 'test')


def assert_expression(expr, expected, compiler):  # noqa: F811
    assert expr.to_elastic(compiler=compiler) == expected


def assert_same_elements(seq1, seq2):
    it1 = iter(seq1)
    it2 = iter(seq2)
    i = 0
    while True:
        try:
            e1 = next(it1)
        except StopIteration:
            try:
                e2 = next(it2)
            except StopIteration:
                break
            else:
                raise AssertionError(
                    'seq2 has more elements than seq1: {!r}'.format(e2)
                )
        try:
            e2 = next(it2)
        except StopIteration:
            raise AssertionError(
                'seq1 has more elements than seq2: {!r}'.format(e1)
            )
        assert e1 is e2, '{} element is not the same'.format(i)
        i += 1
