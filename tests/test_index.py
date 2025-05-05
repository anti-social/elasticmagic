from unittest.mock import Mock

from elasticmagic import Cluster, Index, Document, DynamicDocument, MatchAll, Field
from elasticmagic import actions
from elasticmagic.compiler import Compiler_6_0
from elasticmagic.types import String, Date

from .base import BaseTestCase


class IndexTest(BaseTestCase):
    def test_auto_doc_cls(self):
        doc_cls = self.index['product']
        self.assertEqual(doc_cls.__name__, 'ProductDocument')
        self.assertEqual(doc_cls.__bases__, (DynamicDocument,))

        self.assertIs(doc_cls, self.index['product'])

    def test_index_compiler(self):
        cluster = Cluster(self.client, compiler=Compiler_6_0())
        index = Index(cluster, 'test')

        self.assert_expression(
            index.search_query(index['user'].name == 'kimchy')
            .filter(index['user'].status == 0),
            {
                "query": {
                    "bool": {
                        "must": {
                            "term": {"name": "kimchy"}
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            }
        )

    def test_get_name(self):
        self.assertEqual(self.index.get_name(), 'test')

    def test_get_cluster(self):
        self.assertIs(self.index.get_cluster(), self.cluster)

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
        doc = self.index.get(111, doc_type='tweet', ignore=[404])
        self.client.get.assert_called_with(
            index='test',
            doc_type='tweet',
            id=111,
            ignore=[404],
        )
        self.assertIsInstance(doc, DynamicDocument)
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
                        "_id": "111",
                        "_version": 1,
                        "found": True,
                        "_source": {
                            "user": "kimchy",
                            "post_date": "2009-11-15T14:12:12",
                            "message": "trying out Elasticsearch"
                        }
                    },
                    {
                        "_index": "twitter",
                        "_type": "user",
                        "_id": "222",
                        "found": False
                    }
                ]
            }
        )
        twitter_index = self.cluster['twitter']
        docs = twitter_index.multi_get(
            [
                twitter_index['tweet'](_id=111),
                twitter_index['user'](_id=222, _type='user')
            ],
            refresh=True
        )
        self.client.mget.assert_called_with(
            body={
                "docs": [
                    {
                        "_type": "tweet",
                        "_id": 111
                    },
                    {
                        "_type": "user",
                        "_id": 222
                    }
                ]
            },
            index='twitter',
            refresh=True
        )
        self.assertEqual(len(docs), 2)
        self.assertIsInstance(docs[0], DynamicDocument)
        self.assertEqual(docs[0]._id, '111')
        self.assertEqual(docs[0]._index, 'twitter')
        self.assertEqual(docs[0]._type, 'tweet')
        self.assertEqual(docs[0]._version, 1)
        self.assertEqual(docs[0].user, 'kimchy')
        self.assertEqual(docs[0].post_date, '2009-11-15T14:12:12')
        self.assertEqual(docs[0].message, 'trying out Elasticsearch')
        self.assertIs(docs[1], None)

    def test_count(self):
        self.index.count(MatchAll(), 'car', routing=123)
        self.client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match_all': {}
                }
            },
            routing=123
        )

        result = self.index.count(self.index['car'].name.match('Subaru'), 'car')
        self.client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match': {
                        'name': 'Subaru'
                    }
                }
            }
        )

    def test_exists(self):
        self.client.search = Mock(return_value={
            'hits': {
                'total': 1
            }
        })
        self.assertEqual(
            self.index.exists(MatchAll(), 'car', refresh=True).exists,
            True
        )
        self.client.search.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match_all': {}
                },
                'size': 0,
                'terminate_after': 1,
            },
            refresh=True
        )

        self.client.search = Mock(return_value={
            'hits': {
                'total': 0
            }
        })
        self.assertEqual(
            self.index.exists(
                self.index['car'].name.match('Subaru'), 'car'
            ).exists,
            False
        )
        self.client.search.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match': {
                        'name': 'Subaru'
                    }
                },
                'size': 0,
                'terminate_after': 1,
            }
        )

    def test_add(self):
        class CarDocument(Document):
            __doc_type__ = 'car'

            vendor = Field(String)
            model = Field(String)

        doc = CarDocument(_id='test_id', vendor='Subaru', model='VRX')
        doc._routing = 'Subaru'
        self.index.add([doc])
        self.client.bulk.assert_called_with(
            index='test',
            body=[
                {'index': {'_type': 'car', '_id': 'test_id', 'routing': 'Subaru'}},
                {'vendor': 'Subaru', 'model': 'VRX'}
            ],
        )

    def test_add_raw_docs(self):
        docs = [
            {
                '_id': '1',
                '_routing': 'Subaru',
                '_type': 'car',
                'vendor': 'Subaru',
                'model': 'VRX'
            },
            {
                '_id': '2',
                '_routing': 'Nissan',
                '_type': 'car',
                'vendor': 'Nissan',
                'model': 'X-Trail'
            },
        ]
        self.index.add(docs)
        self.client.bulk.assert_called_with(
            index='test',
            body=[
                {'index': {'_type': 'car', '_id': '1', 'routing': 'Subaru'}},
                {'vendor': 'Subaru', 'model': 'VRX'},
                {'index': {'_type': 'car', '_id': '2', 'routing': 'Nissan'}},
                {'vendor': 'Nissan', 'model': 'X-Trail'}
            ],
        )

    def test_bulk_errors(self):
        self.client.bulk = Mock(
            return_value={
                "took": 85,
                "errors": True,
                "items": [
                    {
                        "delete": {
                            "_index": "test",
                            "_type": "car",
                            "_id": "1",
                            "status": 429,
                            "error": {
                                "type": "es_rejected_execution_exception",
                                "reason": (
                                    "rejected execution of "
                                    "org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryPhase$1@3757e902"
                                    "on EsThreadPoolExecutor["
                                    "bulk, queue capacity = 50, "
                                    "org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@2a3fa7e2["
                                    "Running, pool size = 4, active threads = 4, queued tasks = 50, completed tasks = 225]]"
                                )
                            }
                        }
                    }
                ]
            }
        )
        res = self.index.bulk([actions.Delete(self.index['car'](_id=1))])
        self.client.bulk.assert_called_with(
            body=[
                {'delete': {'_type': 'car', '_id': 1}},
            ],
            index='test',
        )
        self.assertEqual(res.errors, True)
        self.assertEqual(res.took, 85)
        item = res.items[0]
        self.assertEqual(item._index, 'test')
        self.assertEqual(item._type, 'car')
        self.assertEqual(item._id, '1')
        self.assertEqual(item.status, 429)
        self.assertEqual(item.found, None)
        self.assertEqual(item.error.type, 'es_rejected_execution_exception')
        self.assertTrue('TransportReplicationAction' in item.error.reason)

    def test_delete(self):
        self.index.delete(self.index['car'](_id='test_id', _type='car'), refresh=True)
        self.client.delete.assert_called_with(
            index='test',
            doc_type='car',
            id='test_id',
            refresh=True
        )

        # delete also accept document id
        res = self.index.delete('test_id', doc_cls=self.index['car'])
        self.client.delete.assert_called_with(
            index='test',
            doc_type='car',
            id='test_id',
        )

        self.index.delete('test_id')
        self.client.delete.assert_called_with(
            id='test_id',
            index='test',
            doc_type=None,
        )

    def test_delete_by_query(self):
        self.index.delete_by_query(
            self.index['car'].vendor == 'Ford', doc_type='car', routing='Ford'
        )
        self.client.delete_by_query.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'term': {'vendor': 'Ford'}
                }
            },
            routing='Ford'
        )
        
    def test_refresh(self):
        self.index.refresh()
        self.client.indices.refresh.assert_called_with(index='test')

    def test_flush(self):
        self.index.flush()
        self.client.indices.flush.assert_called_with(index='test')

    def test_mapping(self):
        class CarDocument(Document):
            __doc_type__ = 'car'

            vendor = Field(String)
            model = Field(String)
            date_manufactured = Field(Date)

            __mapping_options__ = {
                'date_detection': False,
            }

        self.index.put_mapping(CarDocument)
        self.client.indices.put_mapping.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'date_detection': False,
                'properties': {
                    'vendor': {'type': 'string'},
                    'model': {'type': 'string'},
                    'date_manufactured': {'type': 'date'},
                }
            }
        )

    def test_settings(self):
        self.client.indices.get_settings.return_value = {
            "version": "latest"
        }
        settings = self.index.get_settings()
        self.assertDictEqual(settings, {
            "version": "latest"
        })

    def test_explain_without_doc_cls(self):
        self.client.explain.return_value = {
            "_index": "test",
            "_type": "tweet",
            "_id": "0",
            "matched": True,
            "explanation": {
                "value": 1.55077,
                "description": "score(doc=0,freq=1.0 = termFreq=1.0)"
            }
        }
        result = self.index.explain(MatchAll(), 0, doc_type='tweet')
        self.client.explain.assert_called_with(
            id=0,
            doc_type='tweet',
            index='test',
            body={
                'query': {
                    'match_all': {}
                }
            }
        )
        self.assertEqual(result._id, '0')
        self.assertEqual(result._type, 'tweet')
        self.assertEqual(result._index, 'test')
        self.assertTrue(result.matched)
        self.assertDictEqual(
            result.explanation,
            {
                "value": 1.55077,
                "description": "score(doc=0,freq=1.0 = termFreq=1.0)"
            }
        )
        self.assertIsNone(result.hit)
