from mock import MagicMock

from elasticmagic import SearchQuery, Term, Index, types
from elasticmagic.expression import Fields
from elasticmagic.queryfilter import QueryFilter, Facet

from .base import BaseTestCase


class QueryFilterTest(BaseTestCase):
    def test_facet(self):
        class CarType(object):
            def __init__(self, id, title):
                self.id = id
                self.title = title

        TYPES = {
            t.id: t
            for t in [
                    CarType(0, 'Sedan'),
                    CarType(1, 'Station Wagon'),
                    CarType(2, 'Hatchback'),
            ]
        }

        def type_mapper(values):
            return TYPES

        f = Fields()

        qf = QueryFilter()
        qf.add_filter(Facet('type', f.type, instance_mapper=type_mapper, type=types.Integer))
        qf.add_filter(Facet('vendor', f.vendor))
        qf.add_filter(Facet('model', f.model))

        es_client = MagicMock()
        es_client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.829381,
                    "total": 893
                },
                "aggregations": {
                    "qf": {
                        "doc_count": 7254,
                        "qf": {
                            "doc_count": 2180,
                            "type": {
                                "doc_count": 1298,
                                "type": {
                                    "buckets": [
                                        {
                                            "key": 0,
                                            "doc_count": 744
                                        },
                                        {
                                            "key": 2,
                                            "doc_count": 392
                                        },
                                        {
                                            "key": 1,
                                            "doc_count": 162
                                        }
                                    ]
                                }
                            },
                            "vendor": {
                                "doc_count": 2153,
                                "vendor": {
                                    "buckets": [
                                        {
                                            "key": "Subaru",
                                            "doc_count": 2153
                                        },
                                    ]
                                }
                            },
                            "model": {
                                "doc_count": 2153,
                                "model": {
                                    "buckets": [
                                        {
                                            "key": "Imprezza",
                                            "doc_count": 1586
                                        },
                                        {
                                            "key": "Forester",
                                            "doc_count": 456
                                        },
                                    ]
                                }
                            }
                        }
                    }
                },
                "timed_out": False,
                "took": 154
            }
        )
        es_index = Index(es_client, 'ads')
        sq = es_index.search(Term(es_index.car.name, 'test'))
        sq = qf.apply(sq, {'type': ['0', '1'], 'vendor': ['Subaru']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "term": {"name": "test"}
                        },
                        "filter": {
                            "and": [
                                {"terms": {"type": [0, 1]}},
                                {"term": {"vendor": "Subaru"}}
                            ]
                        }
                    }
                },
                "aggregations": {
                    "qf": {
                        "global": {},
                        "aggregations": {
                            "qf": {
                                "filter": {
                                    "filter": {
                                        "query": {
                                            "term": {"name": "test"}
                                        }
                                    }
                                },
                                "aggregations": {
                                    "type": {
                                        "filter": {
                                            "filter": {
                                                "term": {"vendor": "Subaru"}
                                            }
                                        },
                                        "aggregations": {
                                            "type": {
                                                "terms": {"field": "type"}
                                            }
                                        }
                                    },
                                    "vendor": {
                                        "filter": {
                                            "filter": {
                                                "terms": {"type": [0, 1]}
                                            }
                                        },
                                        "aggregations": {
                                            "vendor": {
                                                "terms": {"field": "vendor"}
                                            }
                                        }
                                    },
                                    "model": {
                                        "filter": {
                                            "filter": {
                                                "and": [
                                                    {"terms": {"type": [0, 1]}},
                                                    {"term": {"vendor": "Subaru"}}
                                                ]
                                            }
                                        },
                                        "aggregations": {
                                            "model": {
                                                "terms": {"field": "model"}
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            }
        )

        qf.process_results(sq.results)

        type_filter = qf.get_filter('type')
        self.assertEqual(len(type_filter.values), 3)
        self.assertEqual(type_filter.values[0].selected, True)
        self.assertEqual(type_filter.values[1].selected, False)
        self.assertEqual(type_filter.values[2].selected, True)
        vendor_filter = qf.get_filter('vendor')
        self.assertEqual(len(vendor_filter.values), 1)
        self.assertEqual(vendor_filter.values[0].selected, True)
        model_filter = qf.get_filter('model')
        self.assertEqual(len(model_filter.values), 2)
        self.assertEqual(model_filter.values[0].selected, False)
        self.assertEqual(model_filter.values[1].selected, False)
