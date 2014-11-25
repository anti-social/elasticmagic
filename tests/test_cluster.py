from elasticmagic import agg, Cluster

from .base import BaseTestCase


class ClusterTest(BaseTestCase):
    def test_multi_index_search(self):
        self.cluster = Cluster(self.client)
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
