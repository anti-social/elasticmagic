from mock import MagicMock

from elasticmagic import Index, Document, DynamicDocument, MatchAll, Field
from elasticmagic.types import String

from .base import BaseTestCase


class IndexTest(BaseTestCase):
    def test_auto_doc_cls(self):
        doc_cls = self.index.product
        self.assertEqual(doc_cls.__name__, 'ProductDocument')
        self.assertEqual(doc_cls.__bases__, (DynamicDocument,))

        self.assertIs(doc_cls, self.index.product)

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
        doc = self.index.get(111, doc_type='tweet')
        self.client.get.assert_called_with(
            index='test',
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

    def test_multi_get(self):
        self.client.mget = MagicMock(
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
                twitter_index.tweet(_id=111), 
                twitter_index.user(_id=222)
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

        self.index.count(self.index.car.name.match('Subaru'), 'car')
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
        self.index.exists(MatchAll(), 'car', refresh=True)
        self.client.exists.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'match_all': {}
                }
            },
            refresh=True
        )

        self.index.exists(self.index.car.name.match('Subaru'), 'car')
        self.client.exists.assert_called_with(
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
                {'index': {'_type': 'car', '_id': 'test_id', '_routing': 'Subaru'}},
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
                {'index': {'_type': 'car', '_id': '1', '_routing': 'Subaru'}},
                {'vendor': 'Subaru', 'model': 'VRX'},
                {'index': {'_type': 'car', '_id': '2', '_routing': 'Nissan'}},
                {'vendor': 'Nissan', 'model': 'X-Trail'}
            ],
        )

    def test_delete(self):
        self.index.delete(self.index.car(_id='test_id'), doc_type='car', refresh=True)
        self.client.delete.assert_called_with(
            index='test',
            doc_type='car',
            id='test_id',
            refresh=True
        )

    def test_delete_by_query(self):
        self.index.delete_by_query(self.index.car.vendor == 'Ford', doc_type='car', routing='Ford')
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
