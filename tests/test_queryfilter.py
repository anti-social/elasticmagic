from mock import MagicMock

from elasticmagic import agg, SearchQuery, Term, Index
from elasticmagic.types import Integer, Float
from elasticmagic.expression import Fields
from elasticmagic.ext.queryfilter import QueryFilter, FacetFilter, RangeFilter
from elasticmagic.ext.queryfilter import FacetQueryFilter, FacetQueryValue
from elasticmagic.ext.queryfilter import OrderingFilter, OrderingValue

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

        class CarQueryFilter(QueryFilter):
            type = FacetFilter(f.type, instance_mapper=type_mapper, type=Integer)
            vendor = FacetFilter(f.vendor, aggs={'min_price': agg.Min(f.price)})
            model = FacetFilter(f.model)

        qf = CarQueryFilter()

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
                            "type.filter": {
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
                            "vendor.filter": {
                                "doc_count": 2153,
                                "vendor": {
                                    "buckets": [
                                        {
                                            "key": "Subaru",
                                            "doc_count": 2153,
                                            "min_price": {"value": 4000}
                                                ,
                                        },
                                    ]
                                }
                            },
                            "model.filter": {
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
        sq = es_index.query(Term(es_index.car.name, 'test'))
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
                            "bool": {
                                "must": [
                                    {"terms": {"type": [0, 1]}},
                                    {"term": {"vendor": "Subaru"}}
                                ]
                            }
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
                                    "type.filter": {
                                        "filter": {
                                            "term": {"vendor": "Subaru"}
                                        },
                                        "aggregations": {
                                            "type": {
                                                "terms": {"field": "type"}
                                            }
                                        }
                                    },
                                    "vendor.filter": {
                                        "filter": {
                                            "terms": {"type": [0, 1]}
                                        },
                                        "aggregations": {
                                            "vendor": {
                                                "terms": {"field": "vendor"},
                                                "aggregations": {
                                                    "min_price": {
                                                        "min": {"field": "price"}
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "model.filter": {
                                        "filter": {
                                            "bool": {
                                                "must": [
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

        type_filter = qf.type
        self.assertEqual(len(type_filter.selected_values), 2)
        self.assertEqual(len(type_filter.values), 1)
        self.assertEqual(len(type_filter.all_values), 3)
        self.assertEqual(type_filter.all_values[0].value, 0)
        self.assertEqual(type_filter.all_values[0].count, 744)
        self.assertEqual(type_filter.all_values[0].count_text, '744')
        self.assertEqual(type_filter.all_values[0].selected, True)
        self.assertEqual(type_filter.all_values[0].instance.title, 'Sedan')
        self.assertIs(type_filter.all_values[0], type_filter.get_value(0))
        self.assertIs(type_filter.all_values[0], type_filter.selected_values[0])
        self.assertEqual(type_filter.all_values[1].value, 2)
        self.assertEqual(type_filter.all_values[1].count, 392)
        self.assertEqual(type_filter.all_values[1].count_text, '+392')
        self.assertEqual(type_filter.all_values[1].selected, False)
        self.assertEqual(type_filter.all_values[1].instance.title, 'Hatchback')
        self.assertIs(type_filter.all_values[1], type_filter.get_value(2))
        self.assertIs(type_filter.all_values[1], type_filter.values[0])
        self.assertEqual(type_filter.all_values[2].value, 1)
        self.assertEqual(type_filter.all_values[2].count, 162)
        self.assertEqual(type_filter.all_values[2].count_text, '162')
        self.assertEqual(type_filter.all_values[2].selected, True)
        self.assertEqual(type_filter.all_values[2].instance.title, 'Station Wagon')
        self.assertIs(type_filter.all_values[2], type_filter.get_value(1))
        self.assertIs(type_filter.all_values[2], type_filter.selected_values[1])
        vendor_filter = qf.vendor
        self.assertEqual(len(vendor_filter.selected_values), 1)
        self.assertEqual(len(vendor_filter.values), 0)
        self.assertEqual(len(vendor_filter.all_values), 1)
        self.assertEqual(vendor_filter.all_values[0].value, 'Subaru')
        self.assertEqual(vendor_filter.all_values[0].count, 2153)
        self.assertEqual(vendor_filter.all_values[0].count_text, '2153')
        self.assertEqual(vendor_filter.all_values[0].selected, True)
        self.assertEqual(vendor_filter.all_values[0].bucket.get_aggregation('min_price').value, 4000)
        self.assertIs(vendor_filter.all_values[0], vendor_filter.selected_values[0])
        self.assertIs(vendor_filter.all_values[0], vendor_filter.get_value('Subaru'))
        model_filter = qf.model
        self.assertEqual(len(model_filter.selected_values), 0)
        self.assertEqual(len(model_filter.values), 2)
        self.assertEqual(len(model_filter.all_values), 2)
        self.assertEqual(model_filter.all_values[0].value, 'Imprezza')
        self.assertEqual(model_filter.all_values[0].count, 1586)
        self.assertEqual(model_filter.all_values[0].count_text, '1586')
        self.assertEqual(model_filter.all_values[0].selected, False)
        self.assertIs(model_filter.all_values[0], model_filter.values[0])
        self.assertIs(model_filter.all_values[0], model_filter.get_value('Imprezza'))
        self.assertEqual(model_filter.all_values[1].value, 'Forester')
        self.assertEqual(model_filter.all_values[1].count, 456)
        self.assertEqual(model_filter.all_values[1].count_text, '456')
        self.assertEqual(model_filter.all_values[1].selected, False)
        self.assertIs(model_filter.all_values[1], model_filter.values[1])
        self.assertIs(model_filter.all_values[1], model_filter.get_value('Forester'))

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
                        "price.min": {"value": 7500},
                        "price.max": {"value": 25800},
                        "disp.filter": {
                            "doc_count": 237,
                            "disp.min": {"value": 1.6},
                            "disp.max": {"value": 3.0}
                        }
                    }
                }
            }
        )
        es_index = Index(es_client, 'ads')

        class CarQueryFilter(QueryFilter):
            price = RangeFilter(es_index.car.price, type=Integer)
            disp = RangeFilter(es_index.car.engine_displacement, type=Float)

        qf = CarQueryFilter()

        sq = es_index.query()
        sq = qf.apply(sq, {'price__lte': ['10000']})
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
                            "price.min": {"min": {"field": "price"}},
                            "price.max": {"max": {"field": "price"}},
                            "disp.filter": {
                                "filter": {
                                    "range": {"price": {"lte": 10000}}
                                },
                                "aggregations": {
                                    "disp.min": {"min": {"field": "engine_displacement"}},
                                    "disp.max": {"max": {"field": "engine_displacement"}}
                                }
                            }
                        }
                    }
                }
            }
        )

        qf.process_results(sq.results)
        price_filter = qf.price
        self.assertEqual(price_filter.min, 7500)
        self.assertEqual(price_filter.max, 25800)
        self.assertIs(price_filter.from_value, None)
        self.assertEqual(price_filter.to_value, 10000)
        disp_filter = qf.disp
        self.assertAlmostEqual(disp_filter.min, 1.6)
        self.assertAlmostEqual(disp_filter.max, 3.0)
        self.assertIs(disp_filter.from_value, None)
        self.assertIs(disp_filter.to_value, None)
        
    def test_facet_query_filter(self):
        es_client = MagicMock()
        es_index = Index(es_client, 'ads')

        class CarQueryFilter(QueryFilter):
            is_new = FacetQueryFilter(
                FacetQueryValue('true', es_index.car.state == 'new')
            )
            price = FacetQueryFilter(
                FacetQueryValue('*-10000', es_index.car.price <= 10000),
                FacetQueryValue('10000-20000', es_index.car.price.range(gt=10000, lte=20000)),
                FacetQueryValue('20000-30000', es_index.car.price.range(gt=20000, lte=30000)),
                FacetQueryValue('30000-*', es_index.car.price.range(gt=30000)),
                aggs={'disp_avg': agg.Avg(es_index.car.engine_displacement)}
            )

        qf = CarQueryFilter()

        sq = es_index.query()
        sq = qf.apply(sq, {'is_new': ['true', 'false']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {"state": "new"}
                        }
                    }
                },
                "aggregations": {
                    "qf": {
                        "global": {},
                        "aggregations": {
                            "is_new:true": {
                                "filter": {
                                    "term": {"state": "new"}
                                }
                            },
                            "price.filter": {
                                "filter": {
                                    "term": {"state": "new"}
                                },
                                "aggregations": {
                                    "price:*-10000": {
                                        "filter": {
                                            "range": {"price": {"lte": 10000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    },
                                    "price:10000-20000": {
                                        "filter": {
                                            "range": {"price": {"gt": 10000, "lte": 20000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    },
                                    "price:20000-30000": {
                                        "filter": {
                                            "range": {"price": {"gt": 20000, "lte": 30000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    },
                                    "price:30000-*": {
                                        "filter": {
                                            "range": {"price": {"gt": 30000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )

        es_client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.829381,
                    "total": 893
                },
                "aggregations": {
                    "qf": {
                        "doc_count": 931,
                        "is_new:true": {
                            "doc_count": 82
                        },
                        "price.filter": {
                            "doc_count": 82,
                            "price:*-10000": {
                                "doc_count": 11,
                                "disp_avg": {"value": 1.56}
                            },
                            "price:10000-20000": {
                                "doc_count": 16,
                                "disp_avg": {"value": 2.4}
                            },
                            "price:20000-30000": {
                                "doc_count": 23,
                                "disp_avg": {"value": 2.85}
                            },
                            "price:30000-*": {
                                "doc_count": 32,
                                "disp_avg": {"value": 2.92}
                            }
                        }
                    }
                }
            }
        )
        qf.process_results(sq.results)
        self.assertEqual(len(qf.is_new.all_values), 1)
        self.assertEqual(len(qf.is_new.selected_values), 1)
        self.assertEqual(len(qf.is_new.values), 0)
        self.assertEqual(qf.is_new.get_value('true').value, 'true')
        self.assertEqual(qf.is_new.get_value('true').count, 82)
        self.assertEqual(qf.is_new.get_value('true').count_text, '82')
        self.assertEqual(qf.is_new.get_value('true').selected, True)
        self.assertEqual(len(qf.price.all_values), 4)
        self.assertEqual(len(qf.price.selected_values), 0)
        self.assertEqual(len(qf.price.values), 4)
        self.assertEqual(qf.price.get_value('*-10000').value, '*-10000')
        self.assertEqual(qf.price.get_value('*-10000').count, 11)
        self.assertEqual(qf.price.get_value('*-10000').count_text, '11')
        self.assertEqual(qf.price.get_value('*-10000').selected, False)
        self.assertEqual(qf.price.get_value('*-10000').agg.get_aggregation('disp_avg').value, 1.56)
        self.assertEqual(qf.price.get_value('10000-20000').value, '10000-20000')
        self.assertEqual(qf.price.get_value('10000-20000').count, 16)
        self.assertEqual(qf.price.get_value('10000-20000').count_text, '16')
        self.assertEqual(qf.price.get_value('10000-20000').selected, False)
        self.assertEqual(qf.price.get_value('10000-20000').agg.get_aggregation('disp_avg').value, 2.4)
        self.assertEqual(qf.price.get_value('20000-30000').value, '20000-30000')
        self.assertEqual(qf.price.get_value('20000-30000').count, 23)
        self.assertEqual(qf.price.get_value('20000-30000').count_text, '23')
        self.assertEqual(qf.price.get_value('20000-30000').selected, False)
        self.assertEqual(qf.price.get_value('20000-30000').agg.get_aggregation('disp_avg').value, 2.85)
        self.assertEqual(qf.price.get_value('30000-*').value, '30000-*')
        self.assertEqual(qf.price.get_value('30000-*').count, 32)
        self.assertEqual(qf.price.get_value('30000-*').count_text, '32')
        self.assertEqual(qf.price.get_value('30000-*').selected, False)
        self.assertEqual(qf.price.get_value('30000-*').agg.get_aggregation('disp_avg').value, 2.92)

        qf = CarQueryFilter()
        sq = es_index.query(es_index.car.year == 2014)
        sq = qf.apply(sq, {'price': ['*-10000', '10000-20000', 'null']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "term": {"year": 2014}
                        },
                        "filter": {
                            "or": [
                                {
                                    "range": {
                                        "price": {"lte": 10000}
                                    }
                                },
                                {
                                    "range": {
                                        "price": {"gt": 10000, "lte": 20000}
                                    }
                                }
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
                                        "term": {"year": 2014}
                                    }
                                },
                                "aggregations": {
                                    "is_new.filter": {
                                        "filter": {
                                            "or": [
                                                {
                                                    "range": {
                                                        "price": {"lte": 10000}
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "price": {"gt": 10000, "lte": 20000}
                                                    }
                                                }
                                            ]
                                        },
                                        "aggregations": {
                                            "is_new:true": {
                                                "filter": {
                                                    "term": {"state": "new"}
                                                }
                                            }
                                        }
                                    },
                                    "price:*-10000": {
                                        "filter": {
                                            "range": {"price": {"lte": 10000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    },
                                    "price:10000-20000": {
                                        "filter": {
                                            "range": {"price": {"gt": 10000, "lte": 20000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    },
                                    "price:20000-30000": {
                                        "filter": {
                                            "range": {"price": {"gt": 20000, "lte": 30000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    },
                                    "price:30000-*": {
                                        "filter": {
                                            "range": {"price": {"gt": 30000}}
                                        },
                                        "aggregations": {
                                            "disp_avg": {
                                                "avg": {"field": "engine_displacement"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )

        es_client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.0,
                    "total": 514
                },
                "aggregations": {
                    "qf": {
                        "doc_count": 931,
                        "qf": {
                            "doc_count": 112,
                            "is_new.filter": {
                                "doc_count": 34,
                                "is_new:true": {
                                    "doc_count": 32
                                }
                            },
                            "price:*-10000": {
                                "doc_count": 7,
                                "disp_avg": {"value": 1.43}
                            },
                            "price:10000-20000": {
                                "doc_count": 11,
                                "disp_avg": {"value": 1.98}
                            },
                            "price:20000-30000": {
                                "doc_count": 6,
                                "disp_avg": {"value": 2.14}
                            },
                            "price:30000-*": {
                                "doc_count": 10,
                                "disp_avg": {"value": 2.67}
                            }
                        }
                    }
                }
            }
        )
        qf.process_results(sq.results)
        self.assertEqual(len(qf.is_new.all_values), 1)
        self.assertEqual(len(qf.is_new.selected_values), 0)
        self.assertEqual(len(qf.is_new.values), 1)
        self.assertEqual(qf.is_new.get_value('true').value, 'true')
        self.assertEqual(qf.is_new.get_value('true').count, 32)
        self.assertEqual(qf.is_new.get_value('true').count_text, '32')
        self.assertEqual(qf.is_new.get_value('true').selected, False)
        self.assertEqual(len(qf.price.all_values), 4)
        self.assertEqual(len(qf.price.selected_values), 2)
        self.assertEqual(len(qf.price.values), 2)
        self.assertEqual(qf.price.get_value('*-10000').value, '*-10000')
        self.assertEqual(qf.price.get_value('*-10000').count, 7)
        self.assertEqual(qf.price.get_value('*-10000').count_text, '7')
        self.assertEqual(qf.price.get_value('*-10000').selected, True)
        self.assertEqual(qf.price.get_value('*-10000').agg.get_aggregation('disp_avg').value, 1.43)
        self.assertEqual(qf.price.get_value('10000-20000').value, '10000-20000')
        self.assertEqual(qf.price.get_value('10000-20000').count, 11)
        self.assertEqual(qf.price.get_value('10000-20000').count_text, '11')
        self.assertEqual(qf.price.get_value('10000-20000').selected, True)
        self.assertEqual(qf.price.get_value('10000-20000').agg.get_aggregation('disp_avg').value, 1.98)
        self.assertEqual(qf.price.get_value('20000-30000').value, '20000-30000')
        self.assertEqual(qf.price.get_value('20000-30000').count, 6)
        self.assertEqual(qf.price.get_value('20000-30000').count_text, '+6')
        self.assertEqual(qf.price.get_value('20000-30000').selected, False)
        self.assertEqual(qf.price.get_value('20000-30000').agg.get_aggregation('disp_avg').value, 2.14)
        self.assertEqual(qf.price.get_value('30000-*').value, '30000-*')
        self.assertEqual(qf.price.get_value('30000-*').count, 10)
        self.assertEqual(qf.price.get_value('30000-*').count_text, '+10')
        self.assertEqual(qf.price.get_value('30000-*').selected, False)
        self.assertEqual(qf.price.get_value('30000-*').agg.get_aggregation('disp_avg').value, 2.67)

    def test_ordering(self):
        es_client = MagicMock()
        es_index = Index(es_client, 'ads')

        class CarQueryFilter(QueryFilter):
            sort = OrderingFilter(
                OrderingValue(
                    'popularity',
                    [es_index.car.popularity.desc(),
                     es_index.car.opinion_count.desc(missing='_last')],
                ),
                OrderingValue('price', [es_index.car.price]),
                OrderingValue('-price', [es_index.car.price.desc()]),
                default='popularity',
            )

        sq = es_index.query()

        qf = CarQueryFilter()
        self.assert_expression(
            qf.apply(sq, {}),
            {
                "aggregations": {
                    "qf": {
                        "global": {}
                    }
                },
                "sort": [
                    {
                        "popularity": "desc"
                    },
                    {
                        "opinion_count": {"order": "desc", "missing": "_last"}
                    }
                ]
            }
        )

        self.assertEqual(qf.sort.selected_value.value, 'popularity')
        self.assertEqual(qf.sort.selected_value.selected, True)
        self.assertEqual(qf.sort.get_value('price').selected, False)
        self.assertEqual(qf.sort.get_value('-price').selected, False)

        qf = CarQueryFilter()
        self.assert_expression(
            qf.apply(sq, {'sort': ['price']}),
            {
                "aggregations": {
                    "qf": {
                        "global": {}
                    }
                },
                "sort": [
                    "price"
                ]
            }
        )
        self.assertEqual(qf.sort.selected_value.value, 'price')
        self.assertEqual(qf.sort.selected_value.selected, True)
        self.assertEqual(qf.sort.get_value('popularity').selected, False)
        self.assertEqual(qf.sort.get_value('-price').selected, False)

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
