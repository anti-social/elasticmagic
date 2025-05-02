import warnings
from unittest.mock import Mock

from elasticmagic import (
    actions, agg, Cluster, DynamicDocument, Index, SearchQuery
)
from elasticmagic import MultiSearchError

from .base import BaseTestCase


class ClusterTest(BaseTestCase):
    def test_get_client(self):
        self.assertIs(self.cluster.get_client(), self.client)

    def test_search_query(self):
        self.client.search = Mock(
            return_value={
                'hits': {
                    'hits': [
                        {
                            '_id': '381',
                            '_type': 'product',
                            '_index': 'test1',
                            '_score': 4.675524,
                            '_source': {
                                'name': 'LG',
                            },
                        },
                        {
                            '_id': '921',
                            '_type': 'opinion',
                            '_index': 'test2',
                            '_score': 3.654321,
                            '_source': {
                                'rank': 1.2,
                            },
                        }
                    ],
                    'max_score': 4.675524,
                    'total': 6234
                },
                'timed_out': False,
                'took': 57
            }
        )
        sq = self.cluster.search_query()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            result = sq.get_result()
        self.client.search.assert_called_with(body={})

        self.assertEqual(len(result.hits), 2)
        self.assertEqual(result.hits[0]._id, '381')
        self.assertEqual(result.hits[0]._type, 'product')
        self.assertEqual(result.hits[0]._index, 'test1')
        self.assertAlmostEqual(result.hits[0]._score, 4.675524)
        self.assertEqual(result.hits[0].name, 'LG')
        self.assertEqual(result.hits[1]._id, '921')
        self.assertEqual(result.hits[1]._type, 'opinion')
        self.assertEqual(result.hits[1]._index, 'test2')
        self.assertAlmostEqual(result.hits[1]._score, 3.654321)
        self.assertAlmostEqual(result.hits[1].rank, 1.2)

    def test_multi_index_search(self):
        es_log_index = self.cluster[
            ('log_2014-11-19', 'log_2014-11-20', 'log_2014-11-20')
        ]
        self.assertIs(
            es_log_index,
            self.cluster[
                ('log_2014-11-19', 'log_2014-11-20', 'log_2014-11-20')
            ]
        )

        sq = (
            es_log_index.query(doc_cls=es_log_index['log'])
            .aggregations(
                percentiles=agg.Percentiles(es_log_index['log'].querytime, percents=[50, 95])
            )
        )
        sq.get_result()
        self.client.search.assert_called_with(
            index='log_2014-11-19,log_2014-11-20,log_2014-11-20',
            body={
                'aggregations': {
                    'percentiles': {
                        'percentiles': {
                            'field': 'querytime',
                            'percents': [50, 95]
                        }
                    }
                }
            }
        )

    def test_multi_search(self):
        self.client.msearch = Mock(
            return_value={
                u'responses': [
                    {
                        u'_shards': {
                            u'failed': 0,
                            u'successful': 64,
                            u'total': 64
                        },
                        u'hits': {
                            u'hits': [],
                            u'max_score': 0.0,
                            u'total': 27802974
                        },
                        u'timed_out': False,
                        u'took': 59
                    },
                    {
                        u'_shards': {
                            u'failed': 0,
                            u'successful': 16,
                            u'total': 16
                        },
                        u'hits': {
                            u'hits': [
                                {
                                    u'_id': u'56565215',
                                    u'_index': u'ca',
                                    u'_type': u'product',
                                    u'_score': 1.0,
                                    u'_source': {
                                        u'name': u'Gold usb cable',
                                        u'price': {u'local': 92.421, u'unit': 5.67},
                                        u'status': 0
                                    },
                                }
                            ],
                            u'max_score': 1.0,
                            u'total': 272
                        },
                        u'timed_out': False,
                        u'took': 53
                    }
                ]
            }
        )
        ProductDoc = self.index['product']
        sq1 = SearchQuery(doc_cls=ProductDoc, search_type='count', routing=123)
        sq2 = (
            SearchQuery(index=self.cluster['us'], doc_cls=ProductDoc)
            .filter(ProductDoc.status == 0)
            .limit(1)
        )
        results = self.cluster.multi_search([sq1, sq2])
        self.client.msearch.assert_called_with(
            body=[
                {'search_type': 'count', 'routing': 123},
                {},
                {'index': 'us'},
                {'query': {'bool': {'filter': {'term': {'status': 0}}}}, 'size': 1}
            ]
        )

        self.assertIs(results[0], sq1.get_result())
        self.assertIs(results[1], sq2.get_result())
        self.assertEqual(results[0].total, 27802974)
        self.assertEqual(len(results[0].hits), 0)
        self.assertEqual(results[1].total, 272)
        self.assertEqual(len(results[1].hits), 1)
        doc = results[1].hits[0]
        self.assertEqual(doc._id, '56565215')
        self.assertEqual(doc._index, 'ca')
        self.assertEqual(doc._type, 'product')
        self.assertAlmostEqual(doc._score, 1.0)
        self.assertEqual(doc.name, 'Gold usb cable')
        self.assertAlmostEqual(doc.price.local, 92.421)
        self.assertAlmostEqual(doc.price.unit, 5.67)
        self.assertEqual(doc.status, 0)

    def test_multi_search_with_error(self):
        self.client.msearch = Mock(
            return_value={
                u'responses': [
                    {
                        u'_shards': {
                            u'failed': 0,
                            u'successful': 64,
                            u'total': 64
                        },
                        u'hits': {
                            u'hits': [],
                            u'max_score': 0.0,
                            u'total': 27802974
                        },
                        u'timed_out': False,
                        u'took': 59
                    },
                    {
                        u'error': u'SearchPhaseExecutionException[Failed to execute phase [query], all shards failed;'
                    }
                ]
            }
        )
        ProductDoc = self.index['product']
        sq1 = SearchQuery(doc_cls=ProductDoc, search_type='count', routing=123)
        sq2 = (
            SearchQuery(index=self.cluster['us'], doc_cls=ProductDoc)
            .filter(ProductDoc.status == 0)
            .limit(1)
        )

        self.assertRaises(MultiSearchError, lambda: self.cluster.multi_search([sq1, sq2]))

        results = self.cluster.multi_search([sq1, sq2], raise_on_error=False)

        self.assertIs(results[0], sq1.get_result())
        self.assertIs(results[1], sq2.get_result())
        self.assertEqual(results[0].total, 27802974)
        self.assertEqual(results[0].took, 59)
        self.assertEqual(results[0].timed_out, False)
        self.assertEqual(results[0].max_score, 0.0)
        self.assertEqual(len(results[0].hits), 0)
        self.assertTrue(
            results[1].error.startswith('SearchPhaseExecutionException')
        )
        self.assertIsNone(results[1].total)
        self.assertEqual(results[1].hits, [])
        self.assertRaises(AttributeError, lambda: results[1].unknown_attr)

    def test_scroll(self):
        self.client.scroll = Mock(
            return_value={
                "_scroll_id": "c2NhbjsxNjsxNTM4NDo1ajYydHRRZVNDeXBrS2RNODVYUkt",
                "took": 570,
                "timed_out": False,
                "hits": {
                    "total": 93570,
                    "max_score": 0,
                    "hits": [
                        {
                            "_index": "test",
                            "_type": "product",
                            "_id": "55377178",
                            "_score": 0,
                            "_source": {
                                "name": "Super iPhone case"
                            }
                        },
                        {
                            "_index": "test",
                            "_type": "product",
                            "_id": "55377196",
                            "_score": 0,
                            "_source": {
                                "name": "Endorphone case for iPhone 5/5s Battlefield"
                            }
                        }
                    ]
                }
            }
        )
        result = self.cluster.scroll(
            scroll_id='c2NhbjsxNjsxNDk2MTg6TndpSEZscTBSUnlVc2I4NkcwNUQwUTsx',
            scroll='30m'
        )
        self.client.scroll.assert_called_with(
            scroll_id='c2NhbjsxNjsxNDk2MTg6TndpSEZscTBSUnlVc2I4NkcwNUQwUTsx',
            scroll='30m'
        )
        self.assertEqual(len(result.hits), 2)
        self.assertEqual(result.scroll_id, 'c2NhbjsxNjsxNTM4NDo1ajYydHRRZVNDeXBrS2RNODVYUkt')

    def test_get(self):
        self.client.get = Mock(
            return_value={
                "_index": "twitter",
                "_type": "tweet",
                "_id": "111",
                "_version": 1,
                "found": True,
                "_source": {
                    "user": "kimchy",
                    "post_date": "2009-11-15T14:12:12",
                    "message": "trying out Elasticsearch"
                }
            }
        )
        doc = self.cluster.get(111, index='twitter', doc_type='tweet')
        self.client.get.assert_called_with(
            index='twitter',
            doc_type='tweet',
            id=111
        )
        self.assertIsInstance(doc, DynamicDocument)
        self.assertEqual(doc._id, '111')
        self.assertEqual(doc._index, 'twitter')
        self.assertEqual(doc._type, 'tweet')
        self.assertEqual(doc._version, 1)
        self.assertEqual(doc.user, 'kimchy')
        self.assertEqual(doc.post_date, '2009-11-15T14:12:12')
        self.assertEqual(doc.message, 'trying out Elasticsearch')

        doc = self.cluster.get(
            111, index='twitter', doc_cls=self.cluster['twitter']['tweet'],
            routing=111
        )
        self.client.get.assert_called_with(
            index='twitter',
            doc_type='tweet',
            id=111,
            routing=111,
        )
        self.assertIsInstance(doc, self.cluster['twitter']['tweet'])
        self.assertEqual(doc._id, '111')
        self.assertEqual(doc._index, 'twitter')
        self.assertEqual(doc._type, 'tweet')
        self.assertEqual(doc._version, 1)
        self.assertEqual(doc.user, 'kimchy')
        self.assertEqual(doc.post_date, '2009-11-15T14:12:12')
        self.assertEqual(doc.message, 'trying out Elasticsearch')
        
    def test_multi_get(self):
        self.client.mget = Mock(
            return_value={
                "docs": [
                    {
                        "_index": "twitter",
                        "_type": "tweet",
                        "_id": "1",
                        "_version": 1,
                        "found": True,
                        "_source": {
                            "user": "kimchy",
                            "post_date": "2009-11-15T14:12:12",
                            "message": "trying out Elasticsearch"
                        }
                    },
                    {
                        "_index": "test",
                        "_type": "tweet",
                        "_id": "2",
                        "_version": 1,
                        "found": True,
                        "_source": {
                            "user": "kimchy",
                            "post_date": "2014-12-29T16:45:58",
                            "message": "Elasticsearch the best"
                        }
                    }
                ]
            }
        )
        # TODO: index aware document class
        docs = self.cluster.multi_get(
            [self.index['tweet'](_id=1, _index='twitter'),
             self.index['tweet'](_id=2, _index='test', _version=1)],
            realtime=False
        )
        self.client.mget.assert_called_with(
            body={
                "docs": [
                    {
                        "_index": "twitter",
                        "_type": "tweet",
                        "_id": 1
                    },
                    {
                        "_index": "test",
                        "_type": "tweet",
                        "_id": 2,
                        "_version": 1
                    }
                ]
            },
            realtime=False
        )
        self.assertEqual(len(docs), 2)
        self.assertIsInstance(docs[0], self.index['tweet'])
        self.assertEqual(docs[0]._id, '1')
        self.assertEqual(docs[0]._type, 'tweet')
        self.assertEqual(docs[0]._index, 'twitter')
        self.assertEqual(docs[0]._version, 1)
        self.assertEqual(docs[0].user, 'kimchy')
        self.assertEqual(docs[0].post_date, '2009-11-15T14:12:12')
        self.assertEqual(docs[0].message, 'trying out Elasticsearch')
        self.assertIsInstance(docs[1], self.index['tweet'])
        self.assertEqual(docs[1]._id, '2')
        self.assertEqual(docs[1]._type, 'tweet')
        self.assertEqual(docs[1]._index, 'test')
        self.assertEqual(docs[1]._version, 1)
        self.assertEqual(docs[1].user, 'kimchy')
        self.assertEqual(docs[1].post_date, '2014-12-29T16:45:58')
        self.assertEqual(docs[1].message, 'Elasticsearch the best')

    def test_bulk(self):
        self.client.bulk = Mock(
            return_value={
                "took": 2,
                "errors": True,
                "items": [
                    {
                        "index": {
                            "_index": "test",
                            "_type": "car",
                            "_id": "1",
                            "_version": 1,
                            "status": 200
                        }
                    },
                    {
                        "delete": {
                            "_index": "test",
                            "_type": "car",
                            "_id": "2",
                            "_version": 1,
                            "found": True,
                            "status": 200
                        }
                    },
                    {
                        "create": {
                            "_index": "test",
                            "_type": "car",
                            "_id": "3",
                            "_version": 1,
                            "status": 201
                        }
                    },
                    {
                        "update": {
                            "_index": "test",
                            "_type": "car",
                            "_id": "4",
                            "status": 404,
                            "error": "DocumentMissingException[[uaprom_cabinet][-1] [product][732635]: document missing]"
                        }
                    },
                    {
                        "update": {
                            "_index": "test",
                            "_type": "car",
                            "_id": "5",
                            "_version": 2,
                            "status": 200
                        }
                    }
                ]
            }
        )
        doc1 = self.index['car'](_id='1', _ttl='1d', field1='value1')
        doc2 = self.index['car'](_id='2')
        doc3 = self.index['car'](_id='3', field3='value3')
        doc4 = self.index['car'](_id='4', field4='value4')
        result = self.cluster.bulk(
            [
                actions.Index(doc1, index=self.index, consistency='one'),
                actions.Delete(doc2, index=self.index, refresh=True),
                actions.Create(doc3, index=self.index),
                actions.Update(doc4, index=self.index, retry_on_conflict=3),
                actions.Update(
                    {
                        '_id': '5',
                        '_type': 'car',
                        'status': 1
                    },
                    index=self.index,
                    doc_as_upsert=True,
                ),
                actions.Update(
                    _id='5',
                    doc_type='car',
                    index=self.index,
                    script="ctx._source.status += value",
                    params={'value': 1},
                ),
                actions.Update(
                    {
                        '_id': '5',
                        '_type': 'car',
                    },
                    index=self.index,
                    script="ctx._source.status += value",
                    params={'value': 1},
                ),
            ],
            refresh=True,
        )
        self.client.bulk.assert_called_with(
            body=[
                {'index': {'_index': 'test', '_type': 'car', '_id': '1', 'ttl': '1d', 'consistency': 'one'}},
                {'field1': 'value1'},
                {'delete': {'_index': 'test', '_type': 'car', '_id': '2', 'refresh': True}},
                {'create': {'_index': 'test', '_type': 'car', '_id': '3'}},
                {'field3': 'value3'},
                {'update': {'_index': 'test', '_type': 'car', '_id': '4', '_retry_on_conflict': 3}},
                {'doc': {'field4': 'value4'}},
                {'update': {'_index': 'test', '_type': 'car', '_id': '5'}},
                {'doc': {'status': 1}, 'doc_as_upsert': True},
                {'update': {'_index': 'test', '_type': 'car', '_id': '5'}},
                {'script': 'ctx._source.status += value', 'params': {'value': 1}},
                {'update': {'_index': 'test', '_type': 'car', '_id': '5'}},
                {'script': 'ctx._source.status += value', 'params': {'value': 1}},
            ],
            refresh=True,
        )

        self.assertEqual(result.took, 2)
        self.assertEqual(result.errors, True)
        self.assertEqual(len(result.items), 5)
        self.assertIs(next(iter(result)), result.items[0])
        self.assertEqual(result.items[0].name, 'index')
        self.assertEqual(result.items[0]._id, '1')
        self.assertEqual(result.items[0]._type, 'car')
        self.assertEqual(result.items[0]._index, 'test')
        self.assertEqual(result.items[0]._version, 1)
        self.assertEqual(result.items[0].status, 200)
        self.assertEqual(bool(result.items[0].error), False)
        self.assertEqual(result.items[1].name, 'delete')
        self.assertEqual(result.items[1]._id, '2')
        self.assertEqual(result.items[1]._type, 'car')
        self.assertEqual(result.items[1]._index, 'test')
        self.assertEqual(result.items[1]._version, 1)
        self.assertEqual(result.items[1].status, 200)
        self.assertEqual(result.items[1].found, True)
        self.assertEqual(bool(result.items[1].error), False)
        self.assertEqual(result.items[2].name, 'create')
        self.assertEqual(result.items[2]._id, '3')
        self.assertEqual(result.items[2]._type, 'car')
        self.assertEqual(result.items[2]._index, 'test')
        self.assertEqual(result.items[2]._version, 1)
        self.assertEqual(result.items[2].status, 201)
        self.assertEqual(bool(result.items[2].error), False)
        self.assertEqual(result.items[3].name, 'update')
        self.assertEqual(result.items[3]._id, '4')
        self.assertEqual(result.items[3]._type, 'car')
        self.assertEqual(result.items[3]._index, 'test')
        self.assertIs(result.items[3]._version, None)
        self.assertEqual(result.items[3].status, 404)
        self.assertEqual(bool(result.items[3].error), True)
        self.assertEqual(result.items[4].name, 'update')
        self.assertEqual(result.items[4]._id, '5')
        self.assertEqual(result.items[4]._type, 'car')
        self.assertEqual(result.items[4]._index, 'test')
        self.assertEqual(result.items[4]._version, 2)
        self.assertEqual(result.items[4].status, 200)
        self.assertEqual(bool(result.items[4].error), False)

    def test_custom_index_class(self):
        class NoSourceIndex(Index):
            def search_query(self, *args, **kwargs):
                return (
                    super(NoSourceIndex, self).search_query(*args, **kwargs)
                    .source(False)
                )

        cluster = Cluster(self.client, index_cls=NoSourceIndex)
        self.assert_expression(
            cluster['test'].search_query(),
            {
                "_source": False
            }
        )
        self.assert_expression(
            cluster['test'].search_query().source(None),
            {}
        )
