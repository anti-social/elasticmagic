from elasticmagic import agg, types
from elasticmagic.result import SearchResult


def test_search_result_with_error_and_aggregations():
    raw_result = {'error': True}
    res = SearchResult(
        raw_result,
        aggregations={'types': agg.Terms(field='type', type=types.Integer)}
    )
    assert res.aggregations['types'].buckets == []
