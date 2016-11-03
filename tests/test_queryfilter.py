from mock import MagicMock

from elasticmagic import agg, Document, DynamicDocument, Field, SearchQuery, Term, Match, Index
from elasticmagic.types import Integer, Float
from elasticmagic.ext.queryfilter import QueryFilter, FacetFilter, RangeFilter, SimpleFilter
from elasticmagic.ext.queryfilter import FacetQueryFilter, FacetQueryValue
from elasticmagic.ext.queryfilter import SimpleQueryFilter, SimpleQueryValue
from elasticmagic.ext.queryfilter import OrderingFilter, OrderingValue
from elasticmagic.ext.queryfilter import GroupedPageFilter, PageFilter

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
            CarType(3, 'Coupe'),
    ]
}

def type_mapper(values):
    return TYPES


class QueryFilterTest(BaseTestCase):
    def test_simple_filter(self):
        class CarQueryFilter(QueryFilter):
            type = SimpleFilter(self.index.car.type, type=Integer)
            vendor = SimpleFilter(self.index.car.vendor)
            model = SimpleFilter(self.index.car.model, alias='m')

        qf = CarQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(sq, {})

        sq = self.index.query()
        sq = qf.apply(sq, {'m': ['vrx']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {
                                "model": "vrx"
                            }
                        }
                    }
                }
            }
        )

        sq = (
            self.index.query(Match(self.index.car.name, 'test'))
            .filter(self.index.car.status == 0)
        )
        sq = qf.apply(sq, {'type': ['0', '1:break', '3', 'null'], 'vendor': ['Subaru']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "match": {"name": "test"}
                        },
                        "filter": {
                            "bool": {
                                "must": [
                                    {"term": {"status": 0}},
                                    {"terms": {"type": [0, 1, 3]}},
                                    {"term": {"vendor": "Subaru"}}
                                ]
                            }
                        }
                    }
                }
            }
        )

    def test_simple_filter_with_and_conjunction(self):
        class ClientQueryFilter(QueryFilter):
            label = SimpleFilter(self.index.client.label, conj_operator=QueryFilter.CONJ_AND)

        qf = ClientQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(sq, {})

        sq = self.index.query()
        sq = qf.apply(sq, {'label': ['greedy']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {
                                "label": "greedy"
                            }
                        }
                    }
                }
            }
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'label': ['greedy', 'young', 'nasty']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {"label": "greedy"}
                                    },
                                    {
                                        "term": {"label": "young"}
                                    },
                                    {
                                        "term": {"label": "nasty"}
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        )

    def test_facet_filter(self):
        class CarQueryFilter(QueryFilter):
            type = FacetFilter(
                self.index.car.type,
                instance_mapper=type_mapper,
                get_title=lambda v: v.instance.title if v.instance else unicode(v.value),
                type=Integer,
            )
            vendor = FacetFilter(self.index.car.vendor, aggs={'min_price': agg.Min(self.index.car.price)})
            model = FacetFilter(self.index.car.model, alias='m')

        qf = CarQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.type": {
                        "terms": {"field": "type"}
                    },
                    "qf.vendor": {
                        "terms": {"field": "vendor"},
                        "aggregations": {
                            "min_price": {
                                "min": {"field": "price"}
                            }
                        }
                    },
                    "qf.model": {
                        "terms": {"field": "model"}
                    }
                }
            }
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'m': ['vrx']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.type.filter": {
                        "filter": {
                            "term": {"model": "vrx"}
                        },
                        "aggregations": {
                            "qf.type": {
                                "terms": {"field": "type"}
                            }
                        }
                    },
                    "qf.vendor.filter": {
                        "filter": {
                            "term": {"model": "vrx"}
                        },
                        "aggregations": {
                            "qf.vendor": {
                                "terms": {"field": "vendor"},
                                "aggregations": {
                                    "min_price": {
                                        "min": {"field": "price"}
                                    }
                                }
                            }
                        }
                    },
                    "qf.model": {
                        "terms": {"field": "model"}
                    }
                },
                "post_filter": {
                    "term": {
                        "model": "vrx"
                    }
                }
            }
        )
        
        sq = (
            self.index.query(Match(self.index.car.name, 'test'))
            .filter(self.index.car.status == 0)
            .post_filter(self.index.car.date_created > 'now-1y',
                         meta={'tags': {qf.get_name()}})
        )
        sq = qf.apply(sq, {'type': ['0', '1:break', '3', 'null'], 'vendor': ['Subaru']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "match": {"name": "test"}
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                },
                "aggregations": {
                    "qf.type.filter": {
                        "filter": {
                            "term": {"vendor": "Subaru"}
                        },
                        "aggregations": {
                            "qf.type": {
                                "terms": {"field": "type"}
                            }
                        }
                    },
                    "qf.vendor.filter": {
                        "filter": {
                            "terms": {"type": [0, 1, 3]}
                        },
                        "aggregations": {
                            "qf.vendor": {
                                "terms": {"field": "vendor"},
                                "aggregations": {
                                    "min_price": {
                                        "min": {"field": "price"}
                                    }
                                }
                            }
                        }
                    },
                    "qf.model.filter": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"terms": {"type": [0, 1, 3]}},
                                    {"term": {"vendor": "Subaru"}}
                                ]
                            }
                        },
                        "aggregations": {
                            "qf.model": {
                                "terms": {"field": "model"}
                            }
                        }
                    }
                },
                "post_filter": {
                    "bool": {
                        "must": [
                            {"range": {"date_created": {"gt": "now-1y"}}},
                            {"terms": {"type": [0, 1, 3]}},
                            {"term": {"vendor": "Subaru"}}
                        ]
                    }
                }
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.829381,
                    "total": 893
                },
                "aggregations": {
                    "qf.type.filter": {
                        "doc_count": 1298,
                        "qf.type": {
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
                    "qf.vendor.filter": {
                        "doc_count": 2153,
                        "qf.vendor": {
                            "buckets": [
                                {
                                    "key": "Subaru",
                                    "doc_count": 2153,
                                    "min_price": {"value": 4000},
                                },
                            ]
                        }
                    },
                    "qf.model.filter": {
                        "doc_count": 2153,
                        "qf.model": {
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
        )

        qf.process_results(sq.get_result())

        type_filter = qf.type
        self.assertEqual(len(type_filter.selected_values), 3)
        self.assertEqual(len(type_filter.values), 1)
        self.assertEqual(len(type_filter.all_values), 4)
        self.assertEqual(type_filter.all_values[0].value, 0)
        self.assertEqual(type_filter.all_values[0].count, 744)
        self.assertEqual(type_filter.all_values[0].count_text, '744')
        self.assertEqual(type_filter.all_values[0].selected, True)
        self.assertEqual(type_filter.all_values[0].title, 'Sedan')
        self.assertEqual(type_filter.all_values[0].instance.title, 'Sedan')
        self.assertIs(type_filter.all_values[0], type_filter.get_value(0))
        self.assertIs(type_filter.all_values[0], type_filter.selected_values[0])
        self.assertEqual(type_filter.all_values[1].value, 2)
        self.assertEqual(type_filter.all_values[1].count, 392)
        self.assertEqual(type_filter.all_values[1].count_text, '+392')
        self.assertEqual(type_filter.all_values[1].selected, False)
        self.assertEqual(type_filter.all_values[1].title, 'Hatchback')
        self.assertEqual(type_filter.all_values[1].instance.title, 'Hatchback')
        self.assertIs(type_filter.all_values[1], type_filter.get_value(2))
        self.assertIs(type_filter.all_values[1], type_filter.values[0])
        self.assertEqual(type_filter.all_values[2].value, 1)
        self.assertEqual(type_filter.all_values[2].count, 162)
        self.assertEqual(type_filter.all_values[2].count_text, '162')
        self.assertEqual(type_filter.all_values[2].selected, True)
        self.assertEqual(type_filter.all_values[2].title, 'Station Wagon')
        self.assertEqual(type_filter.all_values[2].instance.title, 'Station Wagon')
        self.assertIs(type_filter.all_values[2], type_filter.get_value(1))
        self.assertIs(type_filter.all_values[2], type_filter.selected_values[1])
        self.assertEqual(type_filter.all_values[3].value, 3)
        self.assertIs(type_filter.all_values[3].count, None)
        self.assertEqual(type_filter.all_values[3].count_text, '')
        self.assertEqual(type_filter.all_values[3].selected, True)
        self.assertEqual(type_filter.all_values[3].title, 'Coupe')
        self.assertEqual(type_filter.all_values[3].instance.title, 'Coupe')
        self.assertIs(type_filter.all_values[3], type_filter.get_value(3))
        self.assertIs(type_filter.all_values[3], type_filter.selected_values[2])
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

    def test_facet_filter_with_and_conjunction(self):
        class ClientQueryFilter(QueryFilter):
            region = FacetFilter(self.index.client.region_id, type=Integer)
            label = FacetFilter(self.index.client.label, conj_operator=QueryFilter.CONJ_AND)

        qf = ClientQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.region": {
                        "terms": {
                            "field": "region_id"
                        }
                    },
                    "qf.label": {
                        "terms": {
                            "field": "label"
                        }
                    }
                }
            }
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'label': ['greedy']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.region.filter": {
                        "filter": {
                            "term": {
                                "label": "greedy"
                            }
                        },
                        "aggregations": {
                            "qf.region": {
                                "terms": {
                                    "field": "region_id"
                                }
                            }
                        }
                    },
                    "qf.label.filter": {
                        "filter": {
                            "term": {
                                "label": "greedy"
                            }
                        },
                        "aggregations": {
                            "qf.label": {
                                "terms": {
                                    "field": "label"
                                }
                            }
                        }
                    }
                },
                "post_filter": {
                    "term": {
                        "label": "greedy"
                    }
                }
            }
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'region': [123, 456], 'label': ['greedy', 'young', 'nasty']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.region.filter": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {"label": "greedy"}
                                    },
                                    {
                                        "term": {"label": "young"}
                                    },
                                    {
                                        "term": {"label": "nasty"}
                                    }
                                ]
                            }
                        },
                        "aggregations": {
                            "qf.region": {
                                "terms": {
                                    "field": "region_id"
                                }
                            }
                        }
                    },
                    "qf.label.filter": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "terms": {"region_id": [123, 456]}
                                    },
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "term": {"label": "greedy"}
                                                },
                                                {
                                                    "term": {"label": "young"}
                                                },
                                                {
                                                    "term": {"label": "nasty"}
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        "aggregations": {
                            "qf.label": {
                                "terms": {
                                    "field": "label"
                                }
                            }
                        }
                    }
                },
                "post_filter": {
                    "bool": {
                        "must": [
                            {
                                "terms": {"region_id": [123, 456]}
                            },
                            {
                                "bool": {
                                    "must": [
                                        {
                                            "term": {"label": "greedy"}
                                        },
                                        {
                                            "term": {"label": "young"}
                                        },
                                        {
                                            "term": {"label": "nasty"}
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        )

    def test_range_filter(self):
        class CarDocument(Document):
            __doc_type__ = 'car'

            price = Field(Integer)
            engine_displacement = Field(Float)

        class CarQueryFilter(QueryFilter):
            price = RangeFilter(CarDocument.price, compute_min_max=False)
            disp = RangeFilter(CarDocument.engine_displacement, alias='ed', compute_enabled=False)

        qf = CarQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {'ed__gte': ['1.9']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.price.enabled": {"filter": {"exists": {"field": "price"}}},
                    "qf.disp.min": {"min": {"field": "engine_displacement"}},
                    "qf.disp.max": {"max": {"field": "engine_displacement"}}
                },
                "post_filter": {
                    "range": {"engine_displacement": {"gte": 1.9}}
                }
            }
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'price__lte': ['10000']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.price.enabled": {"filter": {"exists": {"field": "price"}}},
                    "qf.disp.filter": {
                        "filter": {
                            "range": {"price": {"lte": 10000}}
                        },
                        "aggregations": {
                            "qf.disp.min": {"min": {"field": "engine_displacement"}},
                            "qf.disp.max": {"max": {"field": "engine_displacement"}}
                        }
                    }
                },
                "post_filter": {
                    "range": {"price": {"lte": 10000}}
                }
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.829381,
                    "total": 893
                },
                "aggregations": {
                    "qf.price.enabled": {"doc_count": 890},
                    "qf.disp.filter": {
                        "doc_count": 237,
                        "qf.disp.min": {"value": 1.6},
                        "qf.disp.max": {"value": 3.0}
                    }
                }
            }
        )
        qf.process_results(sq.get_result())

        price_filter = qf.price
        self.assertEqual(price_filter.enabled, True)
        self.assertIs(price_filter.min, None)
        self.assertIs(price_filter.max, None)
        self.assertIs(price_filter.from_value, None)
        self.assertEqual(price_filter.to_value, 10000)
        disp_filter = qf.disp
        self.assertIs(disp_filter.enabled, None)
        self.assertAlmostEqual(disp_filter.min, 1.6)
        self.assertAlmostEqual(disp_filter.max, 3.0)
        self.assertIs(disp_filter.from_value, None)
        self.assertIs(disp_filter.to_value, None)

    def test_range_filter_dynamic_document(self):
        class CarQueryFilter(QueryFilter):
            price = RangeFilter(self.index.car.price, type=Integer)
            disp = RangeFilter(self.index.car.engine_displacement, type=Float)

        qf = CarQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {'price__lte': ['10000']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.price.enabled": {"filter": {"exists": {"field": "price"}}},
                    "qf.price.min": {"min": {"field": "price"}},
                    "qf.price.max": {"max": {"field": "price"}},
                    "qf.disp.enabled": {"filter": {"exists": {"field": "engine_displacement"}}},
                    "qf.disp.filter": {
                        "filter": {
                            "range": {"price": {"lte": 10000}}
                        },
                        "aggregations": {
                            "qf.disp.min": {"min": {"field": "engine_displacement"}},
                            "qf.disp.max": {"max": {"field": "engine_displacement"}}
                        }
                    }
                },
                "post_filter": {
                    "range": {"price": {"lte": 10000}}
                }
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.829381,
                    "total": 893
                },
                "aggregations": {
                    "qf.price.enabled": {"doc_count": 890},
                    "qf.price.min": {"value": 7500},
                    "qf.price.max": {"value": 25800},
                    "qf.disp.enabled": {"doc_count": 888},
                    "qf.disp.filter": {
                        "doc_count": 237,
                        "qf.disp.min": {"value": 1.6},
                        "qf.disp.max": {"value": 3.0}
                    }
                }
            }
        )
        qf.process_results(sq.get_result())

        price_filter = qf.price
        self.assertEqual(price_filter.enabled, True)
        self.assertEqual(price_filter.min, 7500)
        self.assertEqual(price_filter.max, 25800)
        self.assertIs(price_filter.from_value, None)
        self.assertEqual(price_filter.to_value, 10000)
        disp_filter = qf.disp
        self.assertAlmostEqual(disp_filter.enabled, True)
        self.assertAlmostEqual(disp_filter.min, 1.6)
        self.assertAlmostEqual(disp_filter.max, 3.0)
        self.assertIs(disp_filter.from_value, None)
        self.assertIs(disp_filter.to_value, None)

    def test_simple_query_filter(self):
        class CarQueryFilter(QueryFilter):
            is_new = SimpleQueryFilter(
                SimpleQueryValue('true', self.index.car.state == 'new'),
                alias='new'
            )
            price = SimpleQueryFilter(
                SimpleQueryValue('*-10000', self.index.car.price <= 10000),
                SimpleQueryValue('10000-20000', self.index.car.price.range(gt=10000, lte=20000)),
                SimpleQueryValue('20000-30000', self.index.car.price.range(gt=20000, lte=30000)),
                SimpleQueryValue('30000-*', self.index.car.price.range(gt=30000)),
                aggs={'disp_avg': agg.Avg(self.index.car.engine_displacement)}
            )

        qf = CarQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(sq, {})

        sq = self.index.query()
        sq = qf.apply(sq, {'price': [None]})
        self.assert_expression(sq, {})

        sq = self.index.query()
        sq = qf.apply(sq, {'new': ['true', 'false']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {"state": "new"}
                        }
                    }
                }
            }
        )

        qf = CarQueryFilter()
        sq = (
            self.index.query()
            .filter(self.index.car.year == 2014)
        )
        sq = qf.apply(sq, {'price': ['*-10000', '10000-20000', 'null']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {"year": 2014}
                                    },
                                    {
                                        "bool": {
                                            "should": [
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
                                ]
                            }
                        }
                    }
                }
            }
        )

    def test_simple_query_filter_with_and_conjunction(self):
        class ItemQueryFilter(QueryFilter):
            selling_type = SimpleQueryFilter(
                SimpleQueryValue('retail', self.index.item.selling_type.in_([1, 2, 3])),
                SimpleQueryValue('wholesale', self.index.item.selling_type.in_([3, 4, 5])),
                conj_operator=QueryFilter.CONJ_AND
            )

        qf = ItemQueryFilter()

        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(sq, {})

        sq = self.index.query()
        sq = qf.apply(sq, {'selling_type': ['retail', 'wholesale']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "terms": {"selling_type": [1, 2, 3]}
                                    },
                                    {
                                        "terms": {"selling_type": [3, 4, 5]}
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        )

    def test_facet_query_filter(self):
        class CarQueryFilter(QueryFilter):
            is_new = FacetQueryFilter(
                FacetQueryValue('true', self.index.car.state == 'new'),
                alias='new'
            )
            price = FacetQueryFilter(
                FacetQueryValue('*-10000', self.index.car.price <= 10000),
                FacetQueryValue('10000-20000', self.index.car.price.range(gt=10000, lte=20000)),
                FacetQueryValue('20000-30000', self.index.car.price.range(gt=20000, lte=30000)),
                FacetQueryValue('30000-*', self.index.car.price.range(gt=30000)),
                aggs={'disp_avg': agg.Avg(self.index.car.engine_displacement)}
            )

        qf = CarQueryFilter()

        self.assertIsNot(
            CarQueryFilter().get_filter('price').get_value('*-10000'),
            qf.get_filter('price').get_value('*-10000')
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'new': ['true', 'false']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.is_new:true": {
                        "filter": {
                            "term": {"state": "new"}
                        }
                    },
                    "qf.price.filter": {
                        "filter": {
                            "term": {"state": "new"}
                        },
                        "aggregations": {
                            "qf.price:*-10000": {
                                "filter": {
                                    "range": {"price": {"lte": 10000}}
                                },
                                "aggregations": {
                                    "disp_avg": {
                                        "avg": {"field": "engine_displacement"}
                                    }
                                }
                            },
                            "qf.price:10000-20000": {
                                "filter": {
                                    "range": {"price": {"gt": 10000, "lte": 20000}}
                                },
                                "aggregations": {
                                    "disp_avg": {
                                        "avg": {"field": "engine_displacement"}
                                    }
                                }
                            },
                            "qf.price:20000-30000": {
                                "filter": {
                                    "range": {"price": {"gt": 20000, "lte": 30000}}
                                },
                                "aggregations": {
                                    "disp_avg": {
                                        "avg": {"field": "engine_displacement"}
                                    }
                                }
                            },
                            "qf.price:30000-*": {
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
                },
                "post_filter": {
                    "term": {"state": "new"}
                }
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.829381,
                    "total": 893
                },
                "aggregations": {
                    "qf.is_new:true": {
                        "doc_count": 82
                    },
                    "qf.price.filter": {
                        "doc_count": 82,
                        "qf.price:*-10000": {
                            "doc_count": 11,
                            "disp_avg": {"value": 1.56}
                        },
                        "qf.price:10000-20000": {
                            "doc_count": 16,
                            "disp_avg": {"value": 2.4}
                        },
                        "qf.price:20000-30000": {
                            "doc_count": 23,
                            "disp_avg": {"value": 2.85}
                        },
                        "qf.price:30000-*": {
                            "doc_count": 32,
                            "disp_avg": {"value": 2.92}
                        }
                    }
                }
            }
        )
        qf.process_results(sq.get_result())
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
        sq = self.index.query(self.index.car.year == 2014)
        sq = qf.apply(sq, {'price': ['*-10000', '10000-20000', 'null']})
        self.assert_expression(
            sq,
            {
                "query": {
                    "term": {"year": 2014}
                },
                "aggregations": {
                    "qf.is_new.filter": {
                        "filter": {
                            "bool": {
                                "should": [
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
                        },
                        "aggregations": {
                            "qf.is_new:true": {
                                "filter": {
                                    "term": {"state": "new"}
                                }
                            }
                        }
                    },
                    "qf.price:*-10000": {
                        "filter": {
                            "range": {"price": {"lte": 10000}}
                        },
                        "aggregations": {
                            "disp_avg": {
                                "avg": {"field": "engine_displacement"}
                            }
                        }
                    },
                    "qf.price:10000-20000": {
                        "filter": {
                            "range": {"price": {"gt": 10000, "lte": 20000}}
                        },
                        "aggregations": {
                            "disp_avg": {
                                "avg": {"field": "engine_displacement"}
                            }
                        }
                    },
                    "qf.price:20000-30000": {
                        "filter": {
                            "range": {"price": {"gt": 20000, "lte": 30000}}
                        },
                        "aggregations": {
                            "disp_avg": {
                                "avg": {"field": "engine_displacement"}
                            }
                        }
                    },
                    "qf.price:30000-*": {
                        "filter": {
                            "range": {"price": {"gt": 30000}}
                        },
                        "aggregations": {
                            "disp_avg": {
                                "avg": {"field": "engine_displacement"}
                            }
                        }
                    }
                },
                "post_filter": {
                    "bool": {
                        "should": [
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
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.0,
                    "total": 514
                },
                "aggregations": {
                    "qf.is_new.filter": {
                        "doc_count": 34,
                        "qf.is_new:true": {
                            "doc_count": 32
                        }
                    },
                    "qf.price:*-10000": {
                        "doc_count": 7,
                        "disp_avg": {"value": 1.43}
                    },
                    "qf.price:10000-20000": {
                        "doc_count": 11,
                        "disp_avg": {"value": 1.98}
                    },
                    "qf.price:20000-30000": {
                        "doc_count": 6,
                        "disp_avg": {"value": 2.14}
                    },
                    "qf.price:30000-*": {
                        "doc_count": 10,
                        "disp_avg": {"value": 2.67}
                    }
                }
            }
        )
        qf.process_results(sq.get_result())
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

    def test_facet_query_filter_with_and_conjunction(self):
        class ItemQueryFilter(QueryFilter):
            available = FacetQueryFilter(
                SimpleQueryValue('true', self.index.item.is_available == True),
            )
            selling_type = FacetQueryFilter(
                SimpleQueryValue('retail', self.index.item.selling_type.in_([1, 2, 3])),
                SimpleQueryValue('wholesale', self.index.item.selling_type.in_([3, 4, 5])),
                conj_operator=QueryFilter.CONJ_AND
            )

        qf = ItemQueryFilter()
        
        sq = self.index.query()
        sq = qf.apply(sq, {})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.available:true": {
                        "filter": {
                            "term": {"is_available": True}
                        }
                    },
                    "qf.selling_type:retail": {
                        "filter": {
                            "terms": {"selling_type": [1, 2, 3]}
                        }
                    },
                    "qf.selling_type:wholesale": {
                        "filter": {
                            "terms": {"selling_type": [3, 4, 5]}
                        }
                    }
                }
            }
        )

        sq = self.index.query()
        sq = qf.apply(sq, {'selling_type': ['retail']})
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.available.filter": {
                        "filter": {
                            "terms": {"selling_type": [1, 2, 3]},
                        },
                        "aggregations": {
                            "qf.available:true": {
                                "filter": {
                                    "term": {"is_available": True}
                                }
                            }
                        }
                    },
                    "qf.selling_type.filter": {
                        "filter": {
                            "terms": {"selling_type": [1, 2, 3]},
                        },
                        "aggregations": {
                            "qf.selling_type:retail": {
                                "filter": {
                                    "terms": {"selling_type": [1, 2, 3]}
                                }
                            },
                            "qf.selling_type:wholesale": {
                                "filter": {
                                    "terms": {"selling_type": [3, 4, 5]}
                                }
                            }
                        }
                    }
                },
                "post_filter": {
                    "terms": {"selling_type": [1, 2, 3]}
                }
            }
        )

    def test_ordering(self):
        class CarQueryFilter(QueryFilter):
            sort = OrderingFilter(
                OrderingValue(
                    'popularity',
                    [self.index.car.popularity.desc(),
                     self.index.car.opinion_count.desc(missing='_last')],
                ),
                OrderingValue('price', [self.index.car.price]),
                OrderingValue('-price', [self.index.car.price.desc()]),
                alias='o',
                default='popularity',
            )

        sq = self.index.query()

        qf = CarQueryFilter()

        self.assertIsNot(
            CarQueryFilter().get_filter('sort').get_value('popularity'), 
            qf.get_filter('sort').get_value('popularity')
        )
        
        self.assert_expression(
            qf.apply(sq, {}),
            {
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
            qf.apply(sq, {'o': ['price']}),
            {
                "sort": [
                    "price"
                ]
            }
        )
        self.assertEqual(qf.sort.selected_value.value, 'price')
        self.assertEqual(qf.sort.selected_value.selected, True)
        self.assertEqual(qf.sort.get_value('popularity').selected, False)
        self.assertEqual(qf.sort.get_value('-price').selected, False)

    def test_page(self):
        class CarQueryFilter(QueryFilter):
            page = PageFilter(alias='p', per_page_values=[10, 25, 50])

        sq = self.index.search_query()

        qf = CarQueryFilter()
        self.assert_expression(
            qf.apply(sq, {}),
            {
                "size": 10
            }
        )

        self.assert_expression(
            qf.apply(sq, {'p': 3}),
            {
                "size": 10,
                "from": 20
            }
        )

        self.assert_expression(
            qf.apply(sq, {'per_page': 25}),
            {
                "size": 25
            }
        )

        self.assert_expression(
            qf.apply(sq, {'p': 201, 'per_page': 50}),
            {
                "size": 0
            }
        )

        self.assert_expression(
            qf.apply(sq, {'p': 3, 'per_page': 100}),
            {
                "size": 10,
                "from": 20
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [
                        {"_id": "21", "_type": "car"},
                        {"_id": "22", "_type": "car"},
                        {"_id": "23", "_type": "car"},
                        {"_id": "24", "_type": "car"},
                        {"_id": "25", "_type": "car"},
                        {"_id": "26", "_type": "car"},
                        {"_id": "27", "_type": "car"},
                        {"_id": "28", "_type": "car"},
                        {"_id": "29", "_type": "car"},
                        {"_id": "30", "_type": "car"},
                    ],
                    "max_score": 5.487631,
                    "total": 105
                }
            }
        )
        qf.process_results(sq.get_result())
        self.assertEqual(qf.page.offset, 20)
        self.assertEqual(qf.page.limit, 10)
        self.assertEqual(qf.page.total, 105)
        self.assertEqual(qf.page.pages, 11)
        self.assertEqual(qf.page.has_next, True)
        self.assertEqual(qf.page.has_prev, True)
        self.assertEqual(len(qf.page.items), 10)

    def test_page_with_max_items(self):
        class CarQueryFilter(QueryFilter):
            page = PageFilter(alias='p', per_page_values=[24, 48, 96], max_items=1000)

        sq = self.index.search_query()
        qf = CarQueryFilter()

        self.assert_expression(
            qf.apply(sq, {'p': 11, 'per_page': 96}),
            {
                "size": 40,
                "from": 960
            }
        )

        self.assert_expression(
            qf.apply(sq, {'p': 500}),
            {
                "size": 0
            }
        )

    def test_grouped_page_filter(self):
        class CarQueryFilter(QueryFilter):
            page = GroupedPageFilter(
                self.index.car.vendor, 
                group_kwargs={'size': 2}, per_page_values=[4]
            )

        sq = self.index.search_query()

        qf = CarQueryFilter()
        self.assert_expression(
            qf.apply(sq, {}),
            {
                "aggregations": {
                    "qf.page.pagination": {
                        "terms": {
                            "field": "vendor",
                            "size": 1000,
                            "order": [
                                {
                                    "order_0": "desc"
                                }
                            ]
                        },
                        "aggregations": {
                            "order_0": {
                                "max": {
                                    "script": "_score"
                                }
                            }
                        }
                        
                    },
                    "qf.page": {
                        "terms": {
                            "field": "vendor",
                            "size": 4,
                            "order": [
                                {
                                    "order_0": "desc"
                                }
                            ]
                        },
                        "aggregations": {
                            "top_items": {
                                "top_hits": {
                                    "size": 2,
                                }
                            },
                            "order_0": {
                                "max": {
                                    "script": "_score"
                                }
                            }
                        }
                    }
                }
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.804551,
                    "total": 10378992
                },
                "aggregations": {
                    "qf.page.pagination": {
                        "buckets": [
                            {
                                "key": "toyota",
                                "doc_count": 1158096,
                                "order_0": {
                                    "value": 1.804551
                                }
                            },
                            {
                                "key": "ford",
                                "doc_count": 1354892,
                                "order_0": {
                                    "value": 1.689384
                                }
                            },
                            {
                                "key": "subaru",
                                "doc_count": 934756,
                                "order_0": {
                                    "value": 1.540802
                                }
                            },
                            {
                                "key": "bmw",
                                "doc_count": 125871,
                                "order_0": {
                                    "value": 1.540802
                                }
                            },
                            {
                                "key": "volkswagen",
                                "doc_count": 2573903,
                                "order_0": {
                                    "value": 1.351459
                                }
                            },
                            {
                                "key": "jeep",
                                "doc_count": 10327,
                                "order_0": {
                                    "value": 1.045751
                                }
                            }
                        ]
                    }
                }
            }
        )
        qf = CarQueryFilter()
        sq = qf.apply(sq, {'page': 2, 'per_page': 3})
        self.client.search.assert_called_with(
            body={
                "aggregations": {
                    "qf.page.pagination": {
                        "terms": {
                            "field": "vendor",
                            "size": 1000,
                            "order": [
                                {
                                    "order_0": "desc"
                                }
                            ]
                        },
                        "aggregations": {
                            "order_0": {
                                "max": {
                                    "script": "_score"
                                }
                            }
                        }
                    }
                }
            },
            doc_type='car',
            index='test',
            search_type='count',
        )
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.page.filter": {
                        "filter": {
                            "terms": {
                                "vendor": ["volkswagen", "jeep"]
                            }
                        },
                        "aggregations": {
                            "qf.page": {
                                "terms": {
                                    "field": "vendor",
                                    "size": 4,
                                    "order": [
                                        {
                                            "order_0": "desc"
                                        }
                                    ]
                                },
                                "aggregations": {
                                    "top_items": {
                                        "top_hits": {
                                            "size": 2,
                                        }
                                    },
                                    "order_0": {
                                        "max": {
                                            "script": "_score"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )
        self.assertEqual(qf.page.total, 6)
        self.assertEqual(qf.page.page, 2)
        self.assertEqual(qf.page.pages, 2)
        self.assertEqual(qf.page.has_next, False)
        self.assertEqual(qf.page.has_prev, True)
        self.assertIs(qf.page.items, None)

    def test_grouped_page_filter_with_post_filters(self):
        class CarQueryFilter(QueryFilter):
            page = GroupedPageFilter(
                self.index.car.vendor, 
                group_kwargs={'size': 2}, per_page_values=[2]
            )

        sq = (
            self.index.search_query()
            .post_filter(self.index.car.engine_displacement >= 2)
            .order_by(self.index.car._score, self.index.car.rank)
        )

        qf = CarQueryFilter()

        
        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.804551,
                    "total": 10378992
                },
                "aggregations": {
                    "qf.page.filter": {
                        "doc_count": 21987654,
                        "qf.page.pagination": {
                            "buckets": [
                                {
                                    "key": "toyota",
                                    "doc_count": 1158096,
                                    "order_0": {
                                        "value": 1.804551
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                },
                                {
                                    "key": "ford",
                                    "doc_count": 1354892,
                                    "order_0": {
                                        "value": 1.689384
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                },
                                {
                                    "key": "subaru",
                                    "doc_count": 934756,
                                    "order_0": {
                                        "value": 1.540802
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                },
                                {
                                    "key": "bmw",
                                    "doc_count": 125871,
                                    "order_0": {
                                        "value": 1.540802
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                },
                                {
                                    "key": "volkswagen",
                                    "doc_count": 2573903,
                                    "order_0": {
                                        "value": 1.351459
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                },
                                {
                                    "key": "jeep",
                                    "doc_count": 10327,
                                    "order_0": {
                                        "value": 1.045751
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        )

        sq = qf.apply(sq, {'page': 2})

        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "qf.page.filter": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "engine_displacement": {"gte": 2}
                                        }
                                    },
                                    {
                                        "terms": {
                                            "vendor": ["subaru", "bmw"]
                                        }
                                    }
                                ]
                            }
                        },
                        "aggregations": {
                            "qf.page": {
                                "terms": {
                                    "field": "vendor",
                                    "size": 2,
                                    "order": [
                                        {
                                            "order_0": "desc"
                                        },
                                        {
                                            "order_1": "asc"
                                        }
                                    ]
                                },
                                "aggregations": {
                                    "top_items": {
                                        "top_hits": {
                                            "size": 2,
                                        }
                                    },
                                    "order_0": {
                                        "max": {
                                            "script": "_score"
                                        }
                                    },
                                    "order_1": {
                                        "min": {
                                            "field": "rank"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "post_filter": {
                    "range": {
                        "engine_displacement": {"gte": 2}
                    }
                },
                "sort": [
                    "_score",
                    "rank"
                ]
            }
        )

        self.client.search = MagicMock(
            return_value={
                "hits": {
                    "hits": [],
                    "max_score": 1.804551,
                    "total": 8409177
                },
                "aggregations": {
                    "qf.page.filter": {
                        "doc_count": 1354892,
                        "qf.page": {
                            "buckets": [
                                {
                                    "key": "subaru",
                                    "doc_count": 196874,
                                    "top_items": {
                                        "hits": {
                                            "total": 196874,
                                            "max_score": 1,
                                            "hits": [
                                                {"_id": "21", "_type": "car"},
                                                {"_id": "22", "_type": "car"},
                                            ]
                                        }
                                    },
                                    "order_0": {
                                        "value": 1.804551
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                },
                                {
                                    "key": "bmw",
                                    "doc_count": 98351,
                                    "top_items": {
                                        "hits": {
                                            "total": 98351,
                                            "max_score": 1,
                                            "hits": [
                                                {"_id": "31", "_type": "car"},
                                                {"_id": "32", "_type": "car"},
                                            ]
                                        }
                                    },
                                    "order_0": {
                                        "value": 1.804551
                                    },
                                    "order_1": {
                                        "value": 1.804551
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        )
        qf.process_results(sq.get_result())
        self.assertEqual(qf.page.total, 6)
        self.assertEqual(qf.page.page, 2)
        self.assertEqual(qf.page.pages, 3)
        self.assertEqual(qf.page.has_next, True)
        self.assertEqual(qf.page.has_prev, True)
        self.assertEqual(len(qf.page.items), 2)
        self.assertEqual(qf.page.items[0].key, 'subaru')
        self.assertEqual(qf.page.items[0].get_aggregation('top_items').hits[0]._id, '21')
        self.assertEqual(qf.page.items[0].get_aggregation('top_items').hits[1]._id, '22')
        self.assertEqual(qf.page.items[1].key, 'bmw')
        self.assertEqual(qf.page.items[1].get_aggregation('top_items').hits[0]._id, '31')
        self.assertEqual(qf.page.items[1].get_aggregation('top_items').hits[1]._id, '32')

    def test_query_filter_inheritance(self):
        class SuperBaseItemQueryFilter(QueryFilter):
            price = RangeFilter(self.index.item.price)

        class BaseItemQueryFilter(SuperBaseItemQueryFilter):
            sort = OrderingFilter(
                OrderingValue('-score', [self.index.item._score])
            )

        class ItemQueryFilter(BaseItemQueryFilter):
            selling_type = FacetQueryFilter(
                SimpleQueryValue('retail', self.index.item.selling_type.in_([1, 2, 3])),
                SimpleQueryValue('wholesale', self.index.item.selling_type.in_([3, 4, 5])),
                conj_operator=QueryFilter.CONJ_AND
            )
            page = PageFilter()
            sort = OrderingFilter(
                OrderingValue('-score', [self.index.item._score]),
                OrderingValue('price', [self.index.item.price]),
                OrderingValue('-price', [self.index.item.price.desc()]),
            )

        qf = ItemQueryFilter()
        self.assertEqual(
            [f.name for f in qf.filters],
            ['price', 'selling_type', 'page', 'sort']
        )
        self.assertEqual(
            [v.value for v in qf.sort.values],
            ['-score', 'price', '-price']
        )
            
        BaseItemQueryFilter.presence = FacetFilter(self.index.item.presence)
        ItemQueryFilter.status = FacetFilter(self.index.item.status)

        qf = ItemQueryFilter()
        self.assertEqual(
            [f.name for f in qf.filters],
            ['price', 'presence', 'selling_type', 'page', 'sort', 'status']
        )
        self.assertEqual(
            [v.value for v in qf.sort.values],
            ['-score', 'price', '-price']
        )
        
    def test_dynamic_filters(self):
        class ItemQueryFilter(QueryFilter):
            price = RangeFilter(self.index.item.price)
            selling_type = FacetQueryFilter(
                SimpleQueryValue('retail', self.index.item.selling_type.in_([1, 2, 3])),
                SimpleQueryValue('wholesale', self.index.item.selling_type.in_([3, 4, 5])),
                conj_operator=QueryFilter.CONJ_AND
            )
            page = PageFilter()

        qf = ItemQueryFilter()
        self.assertEqual(len(qf.filters), 3)
        self.assertEqual(qf.price.name, 'price')
        self.assertEqual(qf.selling_type.name, 'selling_type')
        self.assertEqual(qf.page.name, 'page')
        self.assertEqual(qf.page.per_page_values, [10])

        qf.remove_filter('selling_type')
        self.assertEqual(len(qf.filters), 2)
        self.assertEqual(qf.price.name, 'price')
        self.assertIs(qf.get_filter('selling_type'), None)
        self.assertRaises(AttributeError, lambda: qf.selling_type)
        self.assertEqual(qf.page.name, 'page')

        qf.add_filter(PageFilter('page', per_page_values=[10, 20]))
        self.assertEqual(len(qf.filters), 2)
        self.assertEqual(qf.page.per_page_values, [10, 20])

    # def test_nested(self):
    #     f = DynamicDocument.fields

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
