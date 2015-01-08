from mock import MagicMock

from elasticmagic import agg, actions, Cluster, SearchQuery, DynamicDocument

from .base import BaseTestCase


class ClusterTest(BaseTestCase):
    def test_multi_index_search(self):
        es_log_index = self.cluster[('log_2014-11-19', 'log_2014-11-20', 'log_2014-11-20')]
        self.assertIs(
            es_log_index,
            self.cluster[('log_2014-11-19', 'log_2014-11-20', 'log_2014-11-20')]
        )

        sq = (
            es_log_index.query(doc_cls=es_log_index.log)
            .aggregations(
                percentiles=agg.Percentiles(es_log_index.log.querytime, percents=[50, 95])
            )
        )
        sq.result
        self.client.search.assert_called_with(
            index='log_2014-11-19,log_2014-11-20,log_2014-11-20',
            doc_type='log',
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
        self.client.msearch = MagicMock(
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
        ProductDoc = self.index.product
        sq1 = SearchQuery(doc_cls=ProductDoc, search_type='count')
        sq2 = (
            SearchQuery(index=self.cluster['us'], doc_cls=ProductDoc)
            .filter(ProductDoc.status == 0)
            .limit(1)
        )
        results = self.cluster.multi_search([sq1, sq2])
        self.client.msearch.assert_called_with(
            body=[
                {'doc_type': 'product', 'search_type': 'count'},
                {},
                {'index': 'us', 'doc_type': 'product'},
                {'query': {'filtered': {'filter': {'term': {'status': 0}}}}, 'size': 1}
            ]
        )

        self.assertIs(results[0], sq1.result)
        self.assertIs(results[1], sq2.result)
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

    def test_get(self):
        self.client.get = MagicMock(
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
        doc = self.cluster.get('twitter', 111, doc_type='tweet')
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

        doc = self.cluster.get('twitter', 111, doc_cls=self.cluster['twitter'].tweet, routing=111)
        self.client.get.assert_called_with(
            index='twitter',
            doc_type='tweet',
            id=111,
            routing=111,
        )
        self.assertIsInstance(doc, self.cluster['twitter'].tweet)
        self.assertEqual(doc._id, '111')
        self.assertEqual(doc._index, 'twitter')
        self.assertEqual(doc._type, 'tweet')
        self.assertEqual(doc._version, 1)
        self.assertEqual(doc.user, 'kimchy')
        self.assertEqual(doc.post_date, '2009-11-15T14:12:12')
        self.assertEqual(doc.message, 'trying out Elasticsearch')
        
    def test_multi_get(self):
        self.client.mget = MagicMock(
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
            [self.index.tweet(_id=1, _index='twitter'),
             self.index.tweet(_id=2, _index='test', _version=1)],
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
        self.assertIsInstance(docs[0], self.index.tweet)
        self.assertEqual(docs[0]._id, '1')
        self.assertEqual(docs[0]._type, 'tweet')
        self.assertEqual(docs[0]._index, 'twitter')
        self.assertEqual(docs[0]._version, 1)
        self.assertEqual(docs[0].user, 'kimchy')
        self.assertEqual(docs[0].post_date, '2009-11-15T14:12:12')
        self.assertEqual(docs[0].message, 'trying out Elasticsearch')
        self.assertIsInstance(docs[1], self.index.tweet)
        self.assertEqual(docs[1]._id, '2')
        self.assertEqual(docs[1]._type, 'tweet')
        self.assertEqual(docs[1]._index, 'test')
        self.assertEqual(docs[1]._version, 1)
        self.assertEqual(docs[1].user, 'kimchy')
        self.assertEqual(docs[1].post_date, '2014-12-29T16:45:58')
        self.assertEqual(docs[1].message, 'Elasticsearch the best')

    def test_bulk(self):
        self.client.bulk = MagicMock(
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
        doc1 = self.index.car(_id='1', _ttl='1d', field1='value1')
        doc2 = self.index.car(_id='2')
        doc3 = self.index.car(_id='3', field3='value3')
        doc4 = self.index.car(_id='4', field4='value4')
        result = self.cluster.bulk(
            [
                actions.Index(doc1, index=self.index),
                actions.Delete(doc2, index=self.index),
                actions.Create(doc3, index=self.index),
                actions.Update(doc4, index=self.index, retry_on_conflict=3),
                actions.Update(
                    {
                        '_id': '5',
                        '_type': 'car',
                        'status': 1
                    },
                    index=self.index
                ),
            ],
            refresh=True,
        )
        self.client.bulk.assert_called_with(
            body=[
                {'index': {'_index': 'test', '_type': 'car', '_id': '1', '_ttl': '1d'}},
                {'field1': 'value1'},
                {'delete': {'_index': 'test', '_type': 'car', '_id': '2'}},
                {'create': {'_index': 'test', '_type': 'car', '_id': '3'}},
                {'field3': 'value3'},
                {'update': {'_index': 'test', '_type': 'car', '_id': '4', '_retry_on_conflict': 3}},
                {'doc': {'field4': 'value4'}},
                {'update': {'_index': 'test', '_type': 'car', '_id': '5'}},
                {'doc': {'status': 1}},
            ],
            refresh=True,
        )

        self.assertEqual(result.took, 2)
        self.assertEqual(result.errors, True)
        self.assertEqual(len(result.items), 5)
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
