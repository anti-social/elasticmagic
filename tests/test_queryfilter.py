from mock import Mock, MagicMock

from elasticmagic import agg, Document, Field, Match
from elasticmagic.types import Integer, Float, List, Nested, String
from elasticmagic.ext.queryfilter import QueryFilter, FacetFilter, RangeFilter, SimpleFilter
from elasticmagic.ext.queryfilter import FacetQueryFilter, FacetQueryValue
from elasticmagic.ext.queryfilter import SimpleQueryFilter, SimpleQueryValue
from elasticmagic.ext.queryfilter import OrderingFilter, OrderingValue
from elasticmagic.ext.queryfilter import NestedFacetFilter, NestedRangeFilter
from elasticmagic.ext.queryfilter import PageFilter

from .fixtures import client, index
from .util import assert_expr


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


def test_simple_filter(index):
    class CarQueryFilter(QueryFilter):
        type = SimpleFilter(index.car.type, type=Integer)
        vendor = SimpleFilter(index.car.vendor)
        model = SimpleFilter(index.car.model, alias='m')

    qf = CarQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(sq, {})

    sq = index.search_query()
    sq = qf.apply(sq, {'m': ['vrx']})
    assert_expr(
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
        index.search_query(Match(index.car.name, 'test'))
        .filter(index.car.status == 0)
    )
    sq = qf.apply(sq, {'type': ['0', '1:break', '3', 'null'], 'vendor': ['Subaru']})
    assert_expr(
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


def test_simple_filter_with_and_conjunction(index):
    class ClientQueryFilter(QueryFilter):
        label = SimpleFilter(index.client.label, conj_operator=QueryFilter.CONJ_AND)

    qf = ClientQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(sq, {})

    sq = index.search_query()
    sq = qf.apply(sq, {'label': ['greedy']})
    assert_expr(
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

    sq = index.search_query()
    sq = qf.apply(sq, {'label': ['greedy', 'young', 'nasty']})
    assert_expr(
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


def test_facet_filter(index, client):
    class CarQueryFilter(QueryFilter):
        type = FacetFilter(
            index.car.type,
            instance_mapper=type_mapper,
            get_title=lambda v: v.instance.title if v.instance else unicode(v.value),
            type=Integer,
        )
        vendor = FacetFilter(index.car.vendor, aggs={'min_price': agg.Min(index.car.price)})
        model = FacetFilter(index.car.model, alias='m')

    qf = CarQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(
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

    sq = index.search_query()
    sq = qf.apply(sq, {'m': ['vrx']})
    assert_expr(
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
        index.search_query(Match(index.car.name, 'test'))
        .filter(index.car.status == 0)
        .post_filter(index.car.date_created > 'now-1y',
                     meta={'tags': {qf.get_name()}})
    )
    sq = qf.apply(sq, {'type': ['0', '1:break', '3', 'null'], 'vendor': ['Subaru']})
    assert_expr(
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

    client.search = MagicMock(
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

    qf_result = qf.process_results(sq.get_result())

    type_filter = qf_result.type
    assert len(type_filter.selected_values) == 3
    assert len(type_filter.values) == 1
    assert len(type_filter.all_values) == 4
    assert type_filter.all_values[0].value == 0
    assert type_filter.all_values[0].count == 744
    assert type_filter.all_values[0].count_text == '744'
    assert type_filter.all_values[0].selected == True
    assert type_filter.all_values[0].title == 'Sedan'
    assert type_filter.all_values[0].instance.title == 'Sedan'
    assert type_filter.all_values[0] is type_filter.get_value(0)
    assert type_filter.all_values[0] is type_filter.selected_values[0]
    assert type_filter.all_values[1].value == 2
    assert type_filter.all_values[1].count == 392
    assert type_filter.all_values[1].count_text == '+392'
    assert type_filter.all_values[1].selected == False
    assert type_filter.all_values[1].title == 'Hatchback'
    assert type_filter.all_values[1].instance.title == 'Hatchback'
    assert type_filter.all_values[1] is type_filter.get_value(2)
    assert type_filter.all_values[1] is type_filter.values[0]
    assert type_filter.all_values[2].value == 1
    assert type_filter.all_values[2].count == 162
    assert type_filter.all_values[2].count_text == '162'
    assert type_filter.all_values[2].selected == True
    assert type_filter.all_values[2].title == 'Station Wagon'
    assert type_filter.all_values[2].instance.title == 'Station Wagon'
    assert type_filter.all_values[2] is type_filter.get_value(1)
    assert type_filter.all_values[2] is type_filter.selected_values[1]
    assert type_filter.all_values[3].value == 3
    assert type_filter.all_values[3].count is None
    assert type_filter.all_values[3].count_text == ''
    assert type_filter.all_values[3].selected == True
    assert type_filter.all_values[3].title == 'Coupe'
    assert type_filter.all_values[3].instance.title == 'Coupe'
    assert type_filter.all_values[3] is type_filter.get_value(3)
    assert type_filter.all_values[3] is type_filter.selected_values[2]
    vendor_filter = qf_result.vendor
    assert len(vendor_filter.selected_values) == 1
    assert len(vendor_filter.values) == 0
    assert len(vendor_filter.all_values) == 1
    assert vendor_filter.all_values[0].value == 'Subaru'
    assert vendor_filter.all_values[0].count == 2153
    assert vendor_filter.all_values[0].count_text == '2153'
    assert vendor_filter.all_values[0].selected == True
    assert vendor_filter.all_values[0].bucket.get_aggregation('min_price').value == 4000
    assert vendor_filter.all_values[0] is vendor_filter.selected_values[0]
    assert vendor_filter.all_values[0] is vendor_filter.get_value('Subaru')
    model_filter = qf_result.model
    assert len(model_filter.selected_values) == 0
    assert len(model_filter.values) == 2
    assert len(model_filter.all_values) == 2
    assert model_filter.all_values[0].value == 'Imprezza'
    assert model_filter.all_values[0].count == 1586
    assert model_filter.all_values[0].count_text == '1586'
    assert model_filter.all_values[0].selected == False
    assert model_filter.all_values[0] is model_filter.values[0]
    assert model_filter.all_values[0] is model_filter.get_value('Imprezza')
    assert model_filter.all_values[1].value == 'Forester'
    assert model_filter.all_values[1].count == 456
    assert model_filter.all_values[1].count_text == '456'
    assert model_filter.all_values[1].selected == False
    assert model_filter.all_values[1] is model_filter.values[1]
    assert model_filter.all_values[1] is model_filter.get_value('Forester')


def test_facet_filter_with_and_conjunction(index):
    class ClientQueryFilter(QueryFilter):
        region = FacetFilter(index.client.region_id, type=Integer)
        label = FacetFilter(index.client.label, conj_operator=QueryFilter.CONJ_AND)

    qf = ClientQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(
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

    sq = index.search_query()
    sq = qf.apply(sq, {'label': ['greedy']})
    assert_expr(
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

    sq = index.search_query()
    sq = qf.apply(sq, {'region': [123, 456], 'label': ['greedy', 'young', 'nasty']})
    assert_expr(
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


def test_range_filter(index, client):
    class CarDocument(Document):
        __doc_type__ = 'car'

        price = Field(Integer)
        engine_displacement = Field(Float)

    class CarQueryFilter(QueryFilter):
        price = RangeFilter(CarDocument.price, compute_min_max=False)
        disp = RangeFilter(CarDocument.engine_displacement, alias='ed', compute_enabled=False)

    qf = CarQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {'ed__gte': ['1.9']})
    assert_expr(
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

    sq = index.search_query()
    sq = qf.apply(sq, {'price__lte': ['10000']})
    assert_expr(
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

    client.search = MagicMock(
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
    qf_result = qf.process_result(sq.get_result())

    price_filter = qf_result.price
    assert price_filter.enabled is True
    assert price_filter.min_value is None
    assert price_filter.max_value is None
    assert price_filter.from_value is None
    assert price_filter.to_value == 10000
    disp_filter = qf_result.disp
    assert disp_filter.enabled is None
    assert disp_filter.min_value == 1.6
    assert disp_filter.max_value == 3.0
    assert disp_filter.from_value is None
    assert disp_filter.to_value is None


def test_range_filter_dynamic_document(index, client):
    class CarQueryFilter(QueryFilter):
        price = RangeFilter(index.car.price, type=Integer)
        disp = RangeFilter(index.car.engine_displacement, type=Float)

    qf = CarQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {'price__lte': ['10000']})
    assert_expr(
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

    client.search = MagicMock(
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
    qf_result = qf.process_results(sq.get_result())

    price_filter = qf_result.price
    assert price_filter.enabled == True
    assert price_filter.min_value == 7500
    assert price_filter.max_value == 25800
    assert price_filter.from_value is None
    assert price_filter.to_value == 10000
    disp_filter = qf_result.disp
    assert disp_filter.enabled == True
    assert disp_filter.min_value == 1.6
    assert disp_filter.max_value == 3.0
    assert disp_filter.from_value is None
    assert disp_filter.to_value is None


def test_simple_query_filter(index):
    class CarQueryFilter(QueryFilter):
        is_new = SimpleQueryFilter(
            SimpleQueryValue('true', index.car.state == 'new'),
            alias='new'
        )
        price = SimpleQueryFilter(
            SimpleQueryValue('*-10000', index.car.price <= 10000),
            SimpleQueryValue('10000-20000', index.car.price.range(gt=10000, lte=20000)),
            SimpleQueryValue('20000-30000', index.car.price.range(gt=20000, lte=30000)),
            SimpleQueryValue('30000-*', index.car.price.range(gt=30000)),
            aggs={'disp_avg': agg.Avg(index.car.engine_displacement)}
        )

    qf = CarQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(sq, {})

    sq = index.search_query()
    sq = qf.apply(sq, {'price': [None]})
    assert_expr(sq, {})

    sq = index.search_query()
    sq = qf.apply(sq, {'new': ['true', 'false']})
    assert_expr(
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
        index.search_query()
        .filter(index.car.year == 2014)
    )
    sq = qf.apply(sq, {'price': ['*-10000', '10000-20000', 'null']})
    assert_expr(
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


def test_simple_query_filter_with_and_conjunction(index):
    class ItemQueryFilter(QueryFilter):
        selling_type = SimpleQueryFilter(
            SimpleQueryValue('retail', index.item.selling_type.in_([1, 2, 3])),
            SimpleQueryValue('wholesale', index.item.selling_type.in_([3, 4, 5])),
            conj_operator=QueryFilter.CONJ_AND
        )

    qf = ItemQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(sq, {})

    sq = index.search_query()
    sq = qf.apply(sq, {'selling_type': ['retail', 'wholesale']})
    assert_expr(
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


def test_facet_query_filter(index, client):
    class CarQueryFilter(QueryFilter):
        is_new = FacetQueryFilter(
            FacetQueryValue('true', index.car.state == 'new'),
            alias='new'
        )
        price = FacetQueryFilter(
            FacetQueryValue('*-10000', index.car.price <= 10000),
            FacetQueryValue('10000-20000', index.car.price.range(gt=10000, lte=20000)),
            FacetQueryValue('20000-30000', index.car.price.range(gt=20000, lte=30000)),
            FacetQueryValue('30000-*', index.car.price.range(gt=30000)),
            aggs={'disp_avg': agg.Avg(index.car.engine_displacement)}
        )

    qf = CarQueryFilter()

    # assert (
    #     CarQueryFilter().get_filter('price').get_value('*-10000')
    #     is not qf.get_filter('price').get_value('*-10000')
    # )

    sq = index.search_query()
    sq = qf.apply(sq, {'new': ['true', 'false']})
    assert_expr(
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

    client.search = MagicMock(
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
    qf_res = qf.process_results(sq.get_result())
    assert len(qf_res.is_new.all_values) == 1
    assert len(qf_res.is_new.selected_values) == 1
    assert len(qf_res.is_new.values) == 0
    assert qf_res.is_new.get_value('true').value == 'true'
    assert qf_res.is_new.get_value('true').count == 82
    assert qf_res.is_new.get_value('true').count_text == '82'
    assert qf_res.is_new.get_value('true').selected == True
    assert len(qf_res.price.all_values) == 4
    assert len(qf_res.price.selected_values) == 0
    assert len(qf_res.price.values) == 4
    assert qf_res.price.get_value('*-10000').value == '*-10000'
    assert qf_res.price.get_value('*-10000').count == 11
    assert qf_res.price.get_value('*-10000').count_text == '11'
    assert qf_res.price.get_value('*-10000').selected == False
    assert qf_res.price.get_value('*-10000').agg.get_aggregation('disp_avg').value == 1.56
    assert qf_res.price.get_value('10000-20000').value == '10000-20000'
    assert qf_res.price.get_value('10000-20000').count == 16
    assert qf_res.price.get_value('10000-20000').count_text == '16'
    assert qf_res.price.get_value('10000-20000').selected == False
    assert qf_res.price.get_value('10000-20000').agg.get_aggregation('disp_avg').value == 2.4
    assert qf_res.price.get_value('20000-30000').value == '20000-30000'
    assert qf_res.price.get_value('20000-30000').count == 23
    assert qf_res.price.get_value('20000-30000').count_text == '23'
    assert qf_res.price.get_value('20000-30000').selected == False
    assert qf_res.price.get_value('20000-30000').agg.get_aggregation('disp_avg').value == 2.85
    assert qf_res.price.get_value('30000-*').value == '30000-*'
    assert qf_res.price.get_value('30000-*').count == 32
    assert qf_res.price.get_value('30000-*').count_text == '32'
    assert qf_res.price.get_value('30000-*').selected == False
    assert qf_res.price.get_value('30000-*').agg.get_aggregation('disp_avg').value == 2.92

    qf = CarQueryFilter()
    sq = index.search_query(index.car.year == 2014)
    sq = qf.apply(sq, {'price': ['*-10000', '10000-20000', 'null']})
    assert_expr(
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

    client.search = MagicMock(
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
    qf_res = qf.process_results(sq.get_result())
    assert len(qf_res.is_new.all_values) == 1
    assert len(qf_res.is_new.selected_values) == 0
    assert len(qf_res.is_new.values) == 1
    assert qf_res.is_new.get_value('true').value == 'true'
    assert qf_res.is_new.get_value('true').count == 32
    assert qf_res.is_new.get_value('true').count_text == '32'
    assert qf_res.is_new.get_value('true').selected is False
    assert len(qf_res.price.all_values) == 4
    assert len(qf_res.price.selected_values) == 2
    assert len(qf_res.price.values) == 2
    assert qf_res.price.get_value('*-10000').value == '*-10000'
    assert qf_res.price.get_value('*-10000').count == 7
    assert qf_res.price.get_value('*-10000').count_text == '7'
    assert qf_res.price.get_value('*-10000').selected is True
    assert qf_res.price.get_value('*-10000').agg.get_aggregation('disp_avg').value == 1.43
    assert qf_res.price.get_value('10000-20000').value == '10000-20000'
    assert qf_res.price.get_value('10000-20000').count == 11
    assert qf_res.price.get_value('10000-20000').count_text == '11'
    assert qf_res.price.get_value('10000-20000').selected is True
    assert qf_res.price.get_value('10000-20000').agg.get_aggregation('disp_avg').value == 1.98
    assert qf_res.price.get_value('20000-30000').value == '20000-30000'
    assert qf_res.price.get_value('20000-30000').count == 6
    assert qf_res.price.get_value('20000-30000').count_text == '+6'
    assert qf_res.price.get_value('20000-30000').selected is False
    assert qf_res.price.get_value('20000-30000').agg.get_aggregation('disp_avg').value == 2.14
    assert qf_res.price.get_value('30000-*').value == '30000-*'
    assert qf_res.price.get_value('30000-*').count == 10
    assert qf_res.price.get_value('30000-*').count_text == '+10'
    assert qf_res.price.get_value('30000-*').selected is False
    assert qf_res.price.get_value('30000-*').agg.get_aggregation('disp_avg').value == 2.67


def test_facet_query_filter_with_and_conjunction(index):
    class ItemQueryFilter(QueryFilter):
        available = FacetQueryFilter(
            SimpleQueryValue('true', index.item.is_available == True),
        )
        selling_type = FacetQueryFilter(
            SimpleQueryValue('retail', index.item.selling_type.in_([1, 2, 3])),
            SimpleQueryValue('wholesale', index.item.selling_type.in_([3, 4, 5])),
            conj_operator=QueryFilter.CONJ_AND
        )

    qf = ItemQueryFilter()

    sq = index.search_query()
    sq = qf.apply(sq, {})
    assert_expr(
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

    sq = index.search_query()
    sq = qf.apply(sq, {'selling_type': ['retail']})
    assert_expr(
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


def test_ordering(index):
    class CarQueryFilter(QueryFilter):
        sort = OrderingFilter(
            OrderingValue(
                'popularity',
                [index.car.popularity.desc(),
                 index.car.opinion_count.desc(missing='_last')],
            ),
            OrderingValue('price', [index.car.price]),
            OrderingValue('-price', [index.car.price.desc()]),
            alias='o',
            default='popularity',
        )

    sq = index.search_query()

    qf = CarQueryFilter()

    assert_expr(
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

    qf_res = qf.process_result(MagicMock())
    assert qf_res.sort.default_value.value == 'popularity'
    assert qf_res.sort.default_value.selected is True
    assert qf_res.sort.selected_value.value == 'popularity'
    assert qf_res.sort.selected_value.selected is True
    assert qf_res.sort.get_value('price').selected is False
    assert qf_res.sort.get_value('-price').selected is False

    qf = CarQueryFilter()
    assert_expr(
        qf.apply(sq, {'o': ['price']}),
        {
            "sort": [
                "price"
            ]
        }
    )
    qf_res = qf.process_result(MagicMock())
    assert qf_res.sort.default_value.value == 'popularity'
    assert qf_res.sort.default_value.selected is False
    assert qf_res.sort.selected_value.value, 'price'
    assert qf_res.sort.selected_value.selected is True
    assert qf_res.sort.get_value('popularity').selected is False
    assert qf_res.sort.get_value('-price').selected is False


def test_page(index, client):
    class CarQueryFilter(QueryFilter):
        page = PageFilter(alias='p', per_page_values=[10, 25, 50])

    sq = index.search_query()

    qf = CarQueryFilter()
    assert_expr(
        qf.apply(sq, {}),
        {
            "size": 10
        }
    )

    assert_expr(
        qf.apply(sq, {'p': 3}),
        {
            "size": 10,
            "from": 20
        }
    )

    assert_expr(
        qf.apply(sq, {'per_page': 25}),
        {
            "size": 25
        }
    )

    assert_expr(
        qf.apply(sq, {'p': 201, 'per_page': 50}),
        {
            "size": 0
        }
    )

    assert_expr(
        qf.apply(sq, {'p': 3, 'per_page': 100}),
        {
            "size": 10,
            "from": 20
        }
    )

    client.search = MagicMock(
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
    qf_res = qf.process_results(sq.get_result())
    assert qf_res.page.offset == 20
    assert qf_res.page.limit == 10
    assert qf_res.page.total == 105
    assert qf_res.page.pages == 11
    assert qf_res.page.has_next == True
    assert qf_res.page.has_prev == True
    assert len(qf_res.page.items) == 10


def test_page_with_max_items(index):
    class CarQueryFilter(QueryFilter):
        page = PageFilter(alias='p', per_page_values=[24, 48, 96], max_items=1000)

    sq = index.search_query()
    qf = CarQueryFilter()

    assert_expr(
        qf.apply(sq, {'p': 11, 'per_page': 96}),
        {
            "size": 40,
            "from": 960
        }
    )

    assert_expr(
        qf.apply(sq, {'p': 500}),
        {
            "size": 0
        }
    )


def test_nested_facet_filter(index, client):
    class AttributeDoc(Document):
        name = Field(String)
        value = Field(Float)

    class ProductDoc(Document):
        __doc_type__ = 'product'

        attrs = Field(List(Nested(AttributeDoc)))

    f = NestedFacetFilter(
        'size', 'attrs', ProductDoc.attrs.name == 'size', ProductDoc.attrs.value,
    )
    f.qf = Mock(_name='qf')
    assert_expr(
        f._apply_filter(index.search_query(), {}),
        {}
    )
    assert_expr(
        f._apply_agg(index.search_query(), {}),
        {
            "aggregations": {
                "qf.size": {
                    "nested": {
                        "path": "attrs"
                    },
                    "aggregations": {
                        "qf.size.key": {
                            "filter": {
                                "term": {"attrs.name": "size"}
                            },
                            "aggregations": {
                                "qf.size.value": {
                                    "terms": {"field": "attrs.value"}
                                }
                            }
                        }
                    }
                }
            }
        }
    )
    assert_expr(
        f._apply_filter(index.search_query(), {'size': {'exact': [[1]]}}),
        {
            "post_filter": {
                "nested": {
                    "path": "attrs",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"attrs.name": "size"}},
                                {"term": {"attrs.value": 1}}
                            ]
                        }
                    }
                }
            }
        }
    )
    assert_expr(
        f._apply_filter(index.search_query(), {'size': {'exact': [[1], [2]]}}),
        {
            "post_filter": {
                "nested": {
                    "path": "attrs",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"attrs.name": "size"}},
                                {"terms": {"attrs.value": [1, 2]}}
                            ]
                        }
                    }
                }
            }
        }
    )

    f = NestedFacetFilter(
        'size', ProductDoc.attrs, ProductDoc.attrs.name == 'size', ProductDoc.attrs.value,
        conj_operator=QueryFilter.CONJ_AND,
    )
    assert \
        f._apply_filter(index.search_query(), {}).to_dict() == \
        {}
    assert \
        f._apply_filter(index.search_query(),
                        {'size': {'exact': [[1]]}}).to_dict() == \
        {
            "post_filter": {
                "nested": {
                    "path": "attrs",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"attrs.name": "size"}},
                                {"term": {"attrs.value": 1}}
                            ]
                        }
                    }
                }
            }
        }
    assert \
        f._apply_filter(index.search_query(),
                        {'size': {'exact': [[1], [2]]}}).to_dict() == \
        {
            "post_filter": {
                "nested": {
                    "path": "attrs",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"attrs.name": "size"}},
                                {"term": {"attrs.value": 1}},
                                {"term": {"attrs.value": 2}}
                            ]
                        }
                    }
                }
            }
        }

def test_nested_facet_filter_func(index, client):
    class AttributeDoc(Document):
        name = Field(String)
        value = Field(String)

    class ProductDoc(Document):
        __doc_type__ = 'product'

        attrs = Field(List(Nested(AttributeDoc)))

    class TestQueryFilter(QueryFilter):
        size = NestedFacetFilter(
            ProductDoc.attrs,
            ProductDoc.attrs.name == 'size', ProductDoc.attrs.value,
        )
        color = NestedFacetFilter(
            ProductDoc.attrs,
            ProductDoc.attrs.name == 'color', ProductDoc.attrs.value,
        )

    qf = TestQueryFilter()

    client.search = MagicMock(
        return_value={
            "hits": {
                "hits": [],
                "max_score": 1,
                "total": 1
            },
            "aggregations": {
                "qf.size": {
                    "doc_count": 1000,
                    "qf.size.key": {
                        "doc_count": 1000,
                        "qf.size.value": {
                            "buckets": [
                                {
                                    "key": "M",
                                    "doc_count": 284
                                },
                                {
                                    "key": "S",
                                    "doc_count": 172
                                },
                                {
                                    "key": "L",
                                    "doc_count": 93
                                },
                                {
                                    "key": "XL",
                                    "doc_count": 26
                                },
                            ]
                        }
                    }
                },
                "qf.color": {
                    "doc_count": 1000,
                    "qf.color.key": {
                        "doc_count": 1000,
                        "qf.color.value": {
                            "buckets": []
                        }
                    }
                }
            }
        }
    )
    sq = index.search_query()
    sq = qf.apply(sq, {})
    qf_res = qf.process_result(sq.get_result())
    size_res = qf_res.size
    assert len(size_res.values) == 4
    assert len(size_res.all_values) == 4
    assert len(size_res.selected_values) == 0
    values = iter(size_res.all_values)
    fv = next(values)
    assert fv.value == 'M'
    assert fv.count == 284
    assert fv.count_text == '284'
    assert not fv.selected
    fv = next(values)
    assert fv.value == 'S'
    assert fv.count == 172
    assert fv.count_text == '172'
    assert not fv.selected
    fv = next(values)
    assert fv.value == 'L'
    assert fv.count == 93
    assert fv.count_text == '93'
    assert not fv.selected
    fv = next(values)
    assert fv.value == 'XL'
    assert fv.count == 26
    assert fv.count_text == '26'
    assert not fv.selected
    color_res = qf_res.color
    assert len(color_res.values) == 0
    assert len(color_res.all_values) == 0
    assert len(color_res.selected_values) == 0

    client.search = MagicMock(
        return_value={
            "hits": {
                "hits": [],
                "max_score": 1,
                "total": 1
            },
            "aggregations": {
                "qf.size": {
                    "doc_count": 1000,
                    "qf.size.key": {
                        "doc_count": 1000,
                        "qf.size.value": {
                            "buckets": [
                                {
                                    "key": "M",
                                    "doc_count": 284
                                },
                                {
                                    "key": "S",
                                    "doc_count": 172
                                },
                                {
                                    "key": "L",
                                    "doc_count": 93
                                },
                                {
                                    "key": "XL",
                                    "doc_count": 26
                                },
                            ]
                        }
                    }
                },
                "qf.color.filter": {
                    "doc_count": 900,
                    "qf.color": {
                        "doc_count": 900,
                        "qf.color.key": {
                            "doc_count": 900,
                            "qf.color.value": {
                                "buckets": []
                            }
                        }
                    }
                }
            }
        }
    )
    sq = index.search_query()
    sq = qf.apply(sq, {"size": ['S', 'XS']})
    assert \
        sq.to_dict() == \
        {
            "aggregations": {
                "qf.color.filter": {
                    "filter": {
                        "nested": {
                            "path": "attrs",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"attrs.name": "size"}},
                                        {"terms": {"attrs.value": ["S", "XS"]}}
                                    ]
                                }
                            }
                        }
                    },
                    "aggregations": {
                        "qf.color": {
                            "nested": {
                                "path": "attrs"
                            },
                            "aggregations": {
                                "qf.color.key": {
                                    "filter": {
                                        "term": {"attrs.name": "color"}
                                    },
                                    "aggregations": {
                                        "qf.color.value": {
                                            "terms": {"field": "attrs.value"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "qf.size": {
                    "nested": {
                        "path": "attrs"
                    },
                    "aggregations": {
                        "qf.size.key": {
                            "filter": {
                                "term": {"attrs.name": "size"}
                            },
                            "aggregations": {
                                "qf.size.value": {
                                    "terms": {"field": "attrs.value"}
                                }
                            }
                        }
                    }
                }
            },
            "post_filter": {
                "nested": {
                    "path": "attrs",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"attrs.name": "size"}},
                                {"terms": {"attrs.value": ["S", "XS"]}}
                            ]

                        }
                    }
                }
            }
        }
    qf_res = qf.process_result(sq.get_result())
    size_res = qf_res.size
    assert len(size_res.values) == 3
    assert len(size_res.all_values) == 5
    assert len(size_res.selected_values) == 2
    values = iter(size_res.all_values)
    fv = next(values)
    assert fv.value == 'M'
    assert fv.count == 284
    assert fv.count_text == '+284'
    assert not fv.selected
    fv = next(values)
    assert fv.value == 'S'
    assert fv.count == 172
    assert fv.count_text == '172'
    assert fv.selected
    fv = next(values)
    assert fv.value == 'L'
    assert fv.count == 93
    assert fv.count_text == '+93'
    assert not fv.selected
    fv = next(values)
    assert fv.value == 'XL'
    assert fv.count == 26
    assert fv.count_text == '+26'
    assert not fv.selected
    fv = next(values)
    assert fv.value == 'XS'
    assert fv.count is None
    assert fv.count_text == ''
    assert fv.selected
    color_res = qf_res.color
    assert len(color_res.values) == 0
    assert len(color_res.all_values) == 0
    assert len(color_res.selected_values) == 0

def test_nested_range_filter(index, client):
    class AttributeDoc(Document):
        name = Field(String)
        value = Field(Float)

    class ProductDoc(Document):
        __doc_type__ = 'product'

        attrs = Field(List(Nested(AttributeDoc)))

    f = NestedRangeFilter(
        'test', 'attrs', ProductDoc.attrs.name == 'size', ProductDoc.attrs.value,
        compute_enabled=True, compute_min_max=True,
    )
    f.qf = Mock(_name='qf')
    assert \
        f._apply_filter(index.search_query(), {}).to_dict() == \
        {}
    assert \
        f._apply_agg(index.search_query(), {}).to_dict() == \
        {
            "aggregations": {
                "qf.test.enabled": {
                    "nested": {
                        "path": "attrs"
                    },
                    "aggregations": {
                        "qf.test.key": {
                            "filter": {
                                "term": {"attrs.name": "size"}
                            },
                            "aggregations": {
                                "qf.test.value": {
                                    "filter": {
                                        "exists": {"field": "attrs.value"}
                                    }
                                }
                            }
                        }
                    }
                },
                "qf.test.enabled.stat": {
                    "nested": {
                        "path": "attrs"
                    },
                    "aggregations": {
                        "qf.test.key": {
                            "filter": {
                                "term": {"attrs.name": "size"}
                            },
                            "aggregations": {
                                "qf.test.min": {
                                    "min": {"field": "attrs.value"}
                                },
                                "qf.test.max": {
                                    "max": {"field": "attrs.value"}
                                }
                            }
                        }
                    }
                }
            }
        }

    assert \
        f._apply_filter(index.search_query(),
                        {'test': {'gte': [[5.1]]}}).to_dict() == \
        {
            "post_filter": {
                "nested": {
                    "path": "attrs",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"attrs.name": "size"}},
                                {"range": {"attrs.value": {"gte": 5.1}}}
                            ]
                        }
                    }
                }
            }
        }

def test_nested_range_filter_func(index, client):
    class AttributeDoc(Document):
        name = Field(String)
        value = Field(Float)

    class ProductDoc(Document):
        __doc_type__ = 'product'

        attrs = Field(List(Nested(AttributeDoc)))

    class TestQueryFilter(QueryFilter):
        diagonal = NestedRangeFilter(
            ProductDoc.attrs, ProductDoc.attrs.name == 'diagonal', ProductDoc.attrs.value,
            compute_enabled=True, compute_min_max=True,
        )
        weight = NestedRangeFilter(
            ProductDoc.attrs, ProductDoc.attrs.name == 'weight', ProductDoc.attrs.value,
            compute_enabled=False, compute_min_max=True,
        )

    qf = TestQueryFilter()

    client.search = MagicMock(
        return_value={
            "hits": {
                "hits": [],
                "max_score": 1,
                "total": 1
            },
            "aggregations": {
                "qf.diagonal.enabled": {
                    "doc_count": 1000,
                    "qf.diagonal.key": {
                        "doc_count": 1000,
                        "qf.diagonal.value": {
                            "doc_count": 1000
                        }
                    }
                },
                "qf.diagonal.enabled.stat": {
                    "doc_count": 1000,
                    "qf.diagonal.key": {
                        "doc_count": 1000,
                        "qf.diagonal.min": {
                            "value": 17
                        },
                        "qf.diagonal.max": {
                            "value": 102
                        }
                    }
                },
                "qf.weight.enabled.stat": {
                    "doc_count": 1000,
                    "qf.weight.key": {
                        "doc_count": 1000,
                        "qf.weight.min": {
                            "value": 2.5
                        },
                        "qf.weight.max": {
                            "value": 38
                        }
                    }
                }
            }
        }
    )
    sq = index.search_query()
    sq = qf.apply(sq, {})
    qf_res = qf.process_result(sq.get_result())
    diag = qf_res.diagonal
    assert diag.enabled is True
    assert diag.min_value == 17
    assert diag.max_value == 102
    weight = qf_res.weight
    assert weight.enabled is None
    assert weight.min_value == 2.5
    assert weight.max_value == 38.0
