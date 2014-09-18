from mock import MagicMock

from elasticmagic import SearchQuery, Term, Index
from elasticmagic.types import Integer, Float
from elasticmagic.expression import Fields
from elasticmagic.ext.queryfilter import QueryFilter, FacetFilter, RangeFilter

from .base import BaseTestCase


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


class QueryFilterTest(BaseTestCase):
    def test_facet_filter(self):
        f = Fields()

        qf = QueryFilter()
        qf.add_filter(FacetFilter('type', f.type, instance_mapper=type_mapper, type=Integer))
        qf.add_filter(FacetFilter('vendor', f.vendor))
        qf.add_filter(FacetFilter('model', f.model))

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
                }
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
                                    "query": {
                                        "term": {"name": "test"}
                                    }
                                },
                                "aggregations": {
                                    "type": {
                                        "filter": {
                                            "term": {"vendor": "Subaru"}
                                        },
                                        "aggregations": {
                                            "type": {
                                                "terms": {"field": "type"}
                                            }
                                        }
                                    },
                                    "vendor": {
                                        "filter": {
                                            "terms": {"type": [0, 1]}
                                        },
                                        "aggregations": {
                                            "vendor": {
                                                "terms": {"field": "vendor"}
                                            }
                                        }
                                    },
                                    "model": {
                                        "filter": {
                                            "and": [
                                                {"terms": {"type": [0, 1]}},
                                                {"term": {"vendor": "Subaru"}}
                                            ]
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
        self.assertEqual(type_filter.values[0].value, 0)
        self.assertEqual(type_filter.values[0].count, 744)
        self.assertEqual(type_filter.values[0].selected, True)
        self.assertEqual(type_filter.values[0].instance.title, 'Sedan')
        self.assertEqual(type_filter.values[1].value, 2)
        self.assertEqual(type_filter.values[1].count, 392)
        self.assertEqual(type_filter.values[1].selected, False)
        self.assertEqual(type_filter.values[1].instance.title, 'Hatchback')
        self.assertEqual(type_filter.values[2].value, 1)
        self.assertEqual(type_filter.values[2].count, 162)
        self.assertEqual(type_filter.values[2].selected, True)
        self.assertEqual(type_filter.values[2].instance.title, 'Station Wagon')
        vendor_filter = qf.get_filter('vendor')
        self.assertEqual(len(vendor_filter.values), 1)
        self.assertEqual(vendor_filter.values[0].value, 'Subaru')
        self.assertEqual(vendor_filter.values[0].count, 2153)
        self.assertEqual(vendor_filter.values[0].selected, True)
        model_filter = qf.get_filter('model')
        self.assertEqual(len(model_filter.values), 2)
        self.assertEqual(model_filter.values[0].value, 'Imprezza')
        self.assertEqual(model_filter.values[0].count, 1586)
        self.assertEqual(model_filter.values[0].selected, False)
        self.assertEqual(model_filter.values[1].value, 'Forester')
        self.assertEqual(model_filter.values[1].count, 456)
        self.assertEqual(model_filter.values[1].selected, False)

    def test_range_filter(self):
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
                        "doc_count": 128,
                        "price_min": {"value": 7500},
                        "price_max": {"value": 25800},
                        "disp": {
                            "doc_count": 237,
                            "disp_min": {"value": 1.6},
                            "disp_max": {"value": 3.0}
                        }
                    }
                }
            }
        )
        es_index = Index(es_client, 'ads')

        qf = QueryFilter()
        qf.add_filter(RangeFilter('price', es_index.car.price, type=Integer))
        qf.add_filter(RangeFilter('disp', es_index.car.engine_displacement, type=Float))

        sq = es_index.search()
        sq = qf.apply(sq, {'price': [':10000']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "range": {"price": {"lte": 10000}}
                        }
                    }
                },
                "aggregations": {
                    "qf": {
                        "global": {},
                        "aggregations": {
                            "price_min": {"min": {"field": "price"}},
                            "price_max": {"max": {"field": "price"}},
                            "disp": {
                                "filter": {
                                    "range": {"price": {"lte": 10000}}
                                },
                                "aggregations": {
                                    "disp_min": {"min": {"field": "engine_displacement"}},
                                    "disp_max": {"max": {"field": "engine_displacement"}}
                                }
                            }
                        }
                    }
                }
            }
        )

        qf.process_results(sq.results)
        price_filter = qf.get_filter('price')
        self.assertEqual(price_filter.min, 7500)
        self.assertEqual(price_filter.max, 25800)
        disp_filter = qf.get_filter('disp')
        self.assertAlmostEqual(disp_filter.min, 1.6)
        self.assertAlmostEqual(disp_filter.max, 3.0)
        
    # def test_nested(self):
    #     f = Fields()

    #     qf = QueryFilter()
    #     qf.add_filter(
    #         FacetFilter('cat', f.category, type=Integer,
    #                     filters=[FacetFilter('manu', f.manufacturer),
    #                              FacetFilter('manu_country', f.manufacturer_country)])
    #     )

    #     sq = SearchQuery()
    #     sq = qf.apply(sq, {'cat__manu': ['1:thl', '2:china', '3']})
    #     self.assert_expression(
    #         sq,
    #         {
    #             "query": {
    #                 "filtered": {
    #                     "filter": {
    #                         "or": [
    #                             {
    #                                 "and": [
    #                                     {
    #                                         "term": {"category": 1},
    #                                         "term": {"manufacturer": "thl"}
    #                                     }
    #                                 ]
    #                             },
    #                             {
    #                                 "and": [
    #                                     {
    #                                         "term": {"category": 2},
    #                                         "term": {"manufacturer_country": "china"},
    #                                     }
    #                                 ]
    #                             },
    #                             {
    #                                 "term": {"category": 3}
    #                             }
    #                         ]
    #                     }
    #                 }
    #             }
    #         }
    #     )
