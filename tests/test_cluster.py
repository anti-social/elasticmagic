from mock import MagicMock

from elasticmagic import agg, Cluster, SearchQuery

from .base import BaseTestCase


class ClusterTest(BaseTestCase):
    def setUp(self):
        super(ClusterTest, self).setUp()
        self.cluster = Cluster(self.client)

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
        sq.results
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
        results = self.cluster.multi_search(
            SearchQuery(
                index=self.cluster['us'], doc_cls=ProductDoc, search_type='count'
            ),
            SearchQuery(index=self.cluster['ca'], doc_cls=ProductDoc)
            .filter(ProductDoc.status == 0)
            .limit(1)
        )
        self.client.msearch.assert_called_with(
            body=[
                {'index': 'us', 'doc_type': 'product', 'search_type': 'count'},
                {},
                {'index': 'ca', 'doc_type': 'product'},
                {'query': {'filtered': {'filter': {'term': {'status': 0}}}}, 'size': 1}
            ]
        )

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
