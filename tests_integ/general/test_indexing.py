from elasticmagic import SearchQuery

from ..conftest import Car


def test_adding_documents(es_index):
    es_index.add(
        [
            Car(_id=1, name='Lightning McQueen'),
            Car(_id=2, name='Sally Carerra'),
        ]
    )

    doc = es_index.get(1, doc_cls=Car)
    assert doc.name == 'Lightning McQueen'
    assert doc._id == '1'
    assert doc._index == es_index.get_name()
    assert doc._score is None

    doc = es_index.get(2, doc_cls=Car)
    assert doc.name == 'Sally Carerra'
    assert doc._id == '2'
    assert doc._index == es_index.get_name()
    assert doc._score is None


def test_multi_search_with_stats(es_index, es_client, cars):
    # in msearch stats can only be passed inside a sub-request body:
    # Elasticsearch 7+ rejects it in the metadata line
    results = es_index.multi_search([
        SearchQuery().with_stats('cars:list'),
        SearchQuery(),
    ])

    assert results[0].total == 2
    assert results[1].total == 2

    stats = es_client.indices.stats(
        index=es_index.get_name(), groups='cars:list'
    )
    group_stats = stats['_all']['total']['search']['groups']['cars:list']
    assert group_stats['query_total'] >= 1


def test_multi_search_with_search_params(es_cluster, es_client, index_name):
    # msearch accepts stats, terminate_after and timeout only inside
    # a sub-request body; a single shard makes terminate_after counting
    # deterministic on any Elasticsearch version
    es_client.indices.create(
        index=index_name,
        body={'settings': {'index': {'number_of_shards': 1}}},
    )
    try:
        es_index = es_cluster[index_name]
        es_index.put_mapping(Car)
        es_index.add(
            [
                Car(_id=car_id, name='Car {}'.format(car_id))
                for car_id in range(1, 6)
            ],
            refresh=True,
        )

        results = es_index.multi_search([
            SearchQuery().with_stats('cars:list'),
            SearchQuery().with_search_params(terminate_after=1),
            SearchQuery().with_timeout('10s'),
            SearchQuery(),
        ])

        assert results[0].total == 5
        assert len(results[1].hits) == 1
        assert results[2].total == 5
        assert results[2].timed_out is False
        assert results[3].total == 5

        stats = es_client.indices.stats(index=index_name, groups='cars:list')
        group_stats = stats['_all']['total']['search']['groups']['cars:list']
        assert group_stats['query_total'] >= 1
    finally:
        es_client.indices.delete(index=index_name)


def test_scroll(es_index, cars):
    search_res = es_index.search(
        SearchQuery(), scroll='1m',
    )

    assert search_res.total == 2
    assert len(search_res.hits) == 2
    assert search_res.scroll_id is not None

    scroll_res = es_index.scroll(search_res.scroll_id, scroll='1m')

    assert scroll_res.total == 2
    assert len(scroll_res.hits) == 0

    clear_scroll_res = es_index.clear_scroll(scroll_res.scroll_id)

    assert clear_scroll_res.succeeded is True
