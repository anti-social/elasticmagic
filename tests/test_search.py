import datetime
import warnings
from mock import Mock

import pytest

from elasticmagic import (
    Document, DynamicDocument,
    SearchQuery, Params, Term, MultiMatch,
    FunctionScore, Sort, QueryRescorer, agg
)
from elasticmagic.search import FunctionScoreSettings
from elasticmagic.function import FieldValueFactor, Weight
from elasticmagic.util import collect_doc_classes
from elasticmagic.types import String, Integer, Float, Object
from elasticmagic.expression import Field, Script

from .base import OrderTolerantString
from .conftest import assert_expression


f = DynamicDocument.fields


def test_search_query__query(compiler):
    sq = SearchQuery()
    assert_expression(sq, {}, compiler)
    assert collect_doc_classes(sq) == set()

    sq = SearchQuery(Term(f.user, 'kimchy')).limit(10).offset(0)
    assert_expression(
        sq,
        {
            "from": 0,
            "size": 10,
            "query": {
                "term": {"user": "kimchy"}
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery(Term(f.user, 'kimchy'))
        .query(None)
    )
    assert_expression(sq, {}, compiler)
    assert collect_doc_classes(sq) == set()


def test_search_query__filter(compiler):
    sq = SearchQuery(Term(f.user, 'kimchy')).filter(f.age >= 16)
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": {
                            "range": {
                                "age": {"gte": 16}
                            }
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": {
                            "range": {
                                "age": {"gte": 16}
                            }
                        }
                    }
                }
            },
            compiler
        )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery(Term(f.user, 'kimchy'))
        .filter(f.age >= 16)
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": {
                            "range": {
                                "age": {"gte": 16}
                            }
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": {
                            "range": {
                                "age": {"gte": 16}
                            }
                        }
                    }
                }
            },
            compiler
        )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery(Term(f.user, 'kimchy'))
        .query(f.user != 'kimchy')
    )
    assert_expression(
        sq,
        {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "term": {"user": "kimchy"}
                        }
                    ]
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery(Term(f.user, 'kimchy'))
        .filter(f.age >= 16)
        .filter(f.lang == 'English')
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "age": {"gte": 16}
                                        }
                                    },
                                    {
                                        "term": {
                                            "lang": "English"
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
                            "term": {"user": "kimchy"}
                        },
                        "filter": [
                            {
                                "range": {
                                    "age": {"gte": 16}
                                }
                            },
                            {
                                "term": {
                                    "lang": "English"
                                }
                            }
                        ]
                    }
                }
            },
            compiler
        )
    assert collect_doc_classes(sq) == {DynamicDocument}


def test_search_query__order_by(compiler):
    sq = (
        SearchQuery()
        .order_by(
            f.opinion_rating.desc(missing='_last'),
            f.opinion_count.desc(),
            f.id
        )
    )
    assert_expression(
        sq,
        {
            "sort": [
                {
                    "opinion_rating": {
                        "order": "desc",
                        "missing": "_last"
                    }
                },
                {
                    "opinion_count": "desc"
                },
                "id"
            ]
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .order_by(
            f.opinion_rating.desc(missing='_last'),
            f.opinion_count.desc(),
            f.id
        )
        .order_by(None)
    )
    assert_expression(sq, {}, compiler)
    assert collect_doc_classes(sq) == set()


def test_search_query__source(compiler):
    sq = SearchQuery().source(f.name, f.company)
    assert_expression(
        sq,
        {
            "_source": ["name", "company"]
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = SearchQuery().source(exclude=[f.name, f.company])
    assert_expression(
        sq,
        {
            "_source": {
                "exclude": ["name", "company"]
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .source(
            include=[f.obj1.wildcard('*'), f.obj2.wildcard('*')],
            # FIXME: f.wildcard('*')
            exclude=DynamicDocument.wildcard('*').description
        )
    )
    assert_expression(
        sq,
        {
            "_source": {
                "include": ["obj1.*", "obj2.*"],
                "exclude": "*.description"
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .source(None)
        .source(f.name, f.company)
        .source(None)
    )
    assert_expression(sq, {}, compiler)
    assert collect_doc_classes(sq) == set()

    sq = (
        SearchQuery()
        .source(f.name, f.company)
        .source(False)
    )
    assert_expression(
        sq,
        {
            "_source": False
        },
        compiler
    )
    assert collect_doc_classes(sq) == set()

    sq = SearchQuery().source(True)
    assert_expression(
        sq,
        {
            "_source": True
        },
        compiler
    )
    assert collect_doc_classes(sq) == set()


def test_search_query__stored_fields(compiler):
    sq = SearchQuery().stored_fields(f.name, f.company)
    if compiler.min_es_version < (5,):
        assert_expression(sq, {"fields": ["name", "company"]}, compiler)
    else:
        assert_expression(sq, {"stored_fields": ["name", "company"]}, compiler)
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = SearchQuery().fields(f.name, f.company)
    if compiler.min_es_version < (5,):
        assert_expression(sq, {"fields": ["name", "company"]}, compiler)
    else:
        assert_expression(sq, {"stored_fields": ["name", "company"]}, compiler)
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = SearchQuery().stored_fields(True)
    if compiler.min_es_version < (5,):
        assert_expression(sq, {"fields": '*'}, compiler)
    else:
        assert_expression(sq, {"stored_fields": '*'}, compiler)
    assert collect_doc_classes(sq) == set()

    sq = (
        SearchQuery()
        .stored_fields(None)
        .stored_fields(f.name, f.company)
        .stored_fields(None)
    )
    assert_expression(sq, {}, compiler)
    assert collect_doc_classes(sq) == set()

    sq = (
        SearchQuery()
        .stored_fields(f.name, f.company)
        .stored_fields(False)
    )
    assert_expression(sq, {}, compiler)
    assert collect_doc_classes(sq) == set()


def test_search_query__script_fields(compiler):
    if compiler.min_es_version < (5, 6):
        script_inline_param = 'inline'
    else:
        script_inline_param = 'source'

    sq = SearchQuery().script_fields(
        rating=Script(
            inline='doc[params.brand].value +'
                   'doc[params.color].value',
            params=dict(
                brand='brand',
                color='color',
            )
        )
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "script_fields": {
                    "rating": {
                        "script": (
                            "doc[params.brand].value +"
                            "doc[params.color].value"
                        ),
                        "params": {
                            "brand": "brand",
                            "color": "color",
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "script_fields": {
                    "rating": {
                        "script": {
                            script_inline_param: (
                                "doc[params.brand].value +"
                                "doc[params.color].value"
                            ),
                            "params": {
                                "brand": "brand",
                                "color": "color",
                            }
                        }
                    }
                }
            },
            compiler
        )

    sq = sq.script_fields(
        brand=Script(
            inline='doc[params.brand].value', params={'brand': 'brand'}
        )
    )
    if compiler.min_es_version < (2,):
        expected = {
            'script_fields': {
                'rating': {
                    'script': (
                        'doc[params.brand].value +'
                        'doc[params.color].value'
                    ),
                    'params': {
                        'brand': 'brand',
                        'color': 'color',
                    }
                },
                'brand': {
                    'script': 'doc[params.brand].value',
                    'params': {'brand': 'brand'}
                },
            }
        }
    else:
        expected = {
            'script_fields': {
                'rating': {
                    'script': {
                        script_inline_param: (
                            'doc[params.brand].value +'
                            'doc[params.color].value'
                        ),
                        'params': {
                            'brand': 'brand',
                            'color': 'color',
                        }
                    }
                },
                'brand': {
                    'script': {
                        script_inline_param: 'doc[params.brand].value',
                        'params': {'brand': 'brand'}
                    }
                }
            }
        }
    assert_expression(sq, expected, compiler)

    sq = sq.script_fields(True)
    assert_expression(sq, expected, compiler)

    sq = sq.script_fields(None)
    assert_expression(sq, {}, compiler)

    # passing dict as script field is not portable
    sq = SearchQuery().script_fields(
        rating={
            'inline': 'doc[params.rating_field].value',
            'params': {'rating_field': 'rank'}
        }
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                'script_fields': {
                    'rating': {
                        'inline': 'doc[params.rating_field].value',
                        'params': {
                            'rating_field': 'rank',
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                'script_fields': {
                    'rating': {
                        'script': {
                            'inline': 'doc[params.rating_field].value',
                            'params': {
                                'rating_field': 'rank',
                            }
                        }
                    }
                }
            },
            compiler
        )


def test_search_query__function_score(compiler):
    sq = (
        SearchQuery()
        .function_score({'random_score': {"seed": 1234}})
    )
    assert_expression(
        sq,
        {
            "query": {
                "function_score": {
                    "functions": [
                        {
                            "random_score": {"seed": 1234}
                        }
                    ],
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == set()

    sq = (
        SearchQuery()
        .function_score({'field_value_factor': {'field': f.popularity}})
    )
    assert_expression(
        sq,
        {
            "query": {
                "function_score": {
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "popularity"
                            }
                        }
                    ],
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery(MultiMatch('Iphone 6', fields=[f.name, f.description]))
        .filter(f.status == 0)
        .function_score(None)
        .function_score({'_score': {"seed": 1234}})
        .function_score(None)
        .function_score({'field_value_factor': {'field': f.popularity,
                                                'factor': 1.2,
                                                'modifier': 'sqrt'}},
                        boost_mode='sum')
        .function_score({'boost_factor': 3,
                         'filter': f.region == 12})
    )
    expected_function_score_query = {
        "function_score": {
            "query": {
                "multi_match": {
                    "query": "Iphone 6",
                    "fields": ["name", "description"]
                }
            },
            "functions": [
                {
                    "field_value_factor": {
                        "field": "popularity",
                        "factor": 1.2,
                        "modifier": "sqrt"
                    }
                },
                {
                    "filter": {
                        "term": {"region": 12}
                    },
                    "boost_factor": 3
                }
            ],
            "boost_mode": "sum"
        }
    }
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": expected_function_score_query,
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": expected_function_score_query,
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            },
            compiler
        )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .filter(f.status == 0)
        .boost_score(
            {'filter': f.discount_percent >= 10, 'weight': 1000},
            {'filter': f.discount_percent >= 50, 'weight': 2000},
            {'filter': f.presence == 'available', 'weight': 10000},
        )
    )
    expected_function_score_query = {
        "function_score": {
            "functions": [
                {
                    "filter": {
                        "range": {
                            "discount_percent": {"gte": 10}
                        }
                    },
                    "weight": 1000
                },
                {
                    "filter": {
                        "range": {
                            "discount_percent": {"gte": 50}
                        }
                    },
                    "weight": 2000
                },
                {
                    "filter": {
                        "term": {"presence": "available"}
                    },
                    "weight": 10000
                },
            ],
            "score_mode": "sum",
            "boost_mode": "sum"
        }
    }
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": expected_function_score_query,
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": expected_function_score_query,
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            },
            compiler
        )

    sq = (
        SearchQuery(f.name.match('test'))
        .filter(f.status == 0)
        .function_score(
            {'field_value_factor': {'field': f.popularity}},
        )
        .boost_score(
            {'filter': f.discount_percent >= 10, 'weight': 100},
        )
        .boost_score(None)
        .boost_score(
            {'filter': f.discount_percent >= 10, 'weight': 1000},
            {'filter': f.discount_percent >= 50, 'weight': 2000},
            score_mode='max',
        )
    )
    expected_function_score_query = {
        "query": {
            "function_score": {
                "query": {
                    "match": {
                        "name": "test"
                    }
                },
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "popularity"
                        }
                    }
                ]
            }
        },
        "functions": [
            {
                "filter": {
                    "range": {
                        "discount_percent": {"gte": 10}
                    }
                },
                "weight": 1000
            },
            {
                "filter": {
                    "range": {
                        "discount_percent": {"gte": 50}
                    }
                },
                "weight": 2000
            },
        ],
        "score_mode": "max",
        "boost_mode": "sum"
    }
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "query": {
                            "function_score": expected_function_score_query,
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
                            "function_score": expected_function_score_query,
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            },
            compiler
        )


def test_custom_function_scores(compiler):
    AD_ONLY_FUNCTION_SCORE = FunctionScoreSettings(
        'AD_ONLY', score_mode='min', boost_mode='replace', min_score=1e-6
    )
    sq = SearchQuery()
    sq = sq.function_score_settings(AD_ONLY_FUNCTION_SCORE)
    assert_expression(sq, {}, compiler)
    assert_expression(
        sq.function_score(
            AD_ONLY_FUNCTION_SCORE,
            FieldValueFactor(DynamicDocument.ad_price, missing=0.0)
        ),
        {
            "query": {
                "function_score": {
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "ad_price",
                                "missing": 0.0
                            }
                        }
                    ],
                    "score_mode": "min",
                    "boost_mode": "replace",
                    "min_score": 1e-6
                }
            }
        },
        compiler
    )
    assert_expression(
        (
            sq
            .function_score(
                AD_ONLY_FUNCTION_SCORE,
                FieldValueFactor(DynamicDocument.ad_price, missing=0.0)
            )
            .function_score(FieldValueFactor(DynamicDocument.rank))
        ),
        {
            "query": {
                "function_score": {
                    "query": {
                        "function_score": {
                            "functions": [
                                {
                                    "field_value_factor": {
                                        "field": "rank"
                                    }
                                }
                            ]
                        }
                    },
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "ad_price",
                                "missing": 0.0
                            }
                        }
                    ],
                    "score_mode": "min",
                    "boost_mode": "replace",
                    "min_score": 1e-6
                }
            }
        },
        compiler
    )
    assert_expression(
        (
            sq
            .function_score(
                AD_ONLY_FUNCTION_SCORE,
                FieldValueFactor(DynamicDocument.ad_price, missing=0.0)
            )
            .function_score(FieldValueFactor(DynamicDocument.rank))
            .boost_score(
                Weight(
                    filter=DynamicDocument.presence == 'available',
                    weight=1000
                )
            )
        ),
        {
            "query": {
                "function_score": {
                    "query": {
                        "function_score": {
                            "query": {
                                "function_score": {
                                    "functions": [
                                        {
                                            "field_value_factor": {
                                                "field": "rank"
                                            }
                                        }
                                    ]
                                }
                            },
                            "functions": [
                                {
                                    "filter": {
                                        "term": {"presence": "available"}
                                    },
                                    "weight": 1000
                                }
                            ],
                            "score_mode": "sum",
                            "boost_mode": "sum"
                        }
                    },
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "ad_price",
                                "missing": 0.0
                            }
                        }
                    ],
                    "score_mode": "min",
                    "boost_mode": "replace",
                    "min_score": 1e-6
                }
            }
        },
        compiler
    )
    with pytest.raises(ValueError):
        ANOTHER_FUNCTION_SCORE = FunctionScoreSettings('ANOTHER')
        sq.function_score(ANOTHER_FUNCTION_SCORE, Weight(1))


def test_search_query__rescore(index, compiler):
    sq = (
        SearchQuery()
        .rescore(
            QueryRescorer(
                index['type'].field1.match('the quick brown')
            )
        )
        .rescore(None)
        .rescore(
            QueryRescorer(
                index['type'].field1.match('the quick brown fox'),
                window_size=100,
                query_weight=0.7,
                rescore_query_weight=1.2
            ),
        )
        .rescore(
            QueryRescorer(
                FunctionScore(
                    script_score={
                        'script': "log10(doc['numeric'].value + 2)"
                    }
                ),
                window_size=10,
                score_mode='multiply'
            ),
        )
    )
    assert_expression(
        sq,
        {
            "rescore": [
                {
                    "query": {
                        "rescore_query": {
                            "match": {
                                "field1": "the quick brown fox",
                            }
                        },
                        "query_weight": 0.7,
                        "rescore_query_weight": 1.2,
                        "window_size": 100,
                    }
                },
                {
                    "query": {
                        "score_mode": "multiply",
                        "window_size": 10,
                        "rescore_query": {
                            "function_score": {
                                "script_score": {
                                    "script": "log10(doc['numeric'].value + 2)"
                                }
                            }
                        }
                    }
                }
            ]
        },
        compiler
    )
    assert collect_doc_classes(sq) == {index['type']}


def test_search_query__post_filter(index, compiler):
    sq = SearchQuery().post_filter(index['shirt'].color == 'red')
    assert_expression(
        sq,
        {
            "post_filter": {
                "term": {"color": "red"}
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {index['shirt']}

    sq = (
        SearchQuery()
        .filter(index['shirt'].brand == 'gucci')
        .post_filter(index['shirt'].color == 'red')
        .post_filter(index['shirt'].model == 't-shirt')
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            sq,
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {"brand": "gucci"}
                        }
                    }
                },
                "post_filter": {
                    "bool": {
                        "must": [
                            {"term": {"color": "red"}},
                            {"term": {"model": "t-shirt"}}
                        ]
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "filter": {
                            "term": {"brand": "gucci"}
                        }
                    }
                },
                "post_filter": {
                    "bool": {
                        "must": [
                            {"term": {"color": "red"}},
                            {"term": {"model": "t-shirt"}}
                        ]
                    }
                }
            },
            compiler
        )
    assert collect_doc_classes(sq) == {index['shirt']}


def test_aggregations(compiler):
    sq = SearchQuery().aggregations(min_price=agg.Min(f.price))
    assert_expression(
        sq,
        {
            "aggregations": {
                "min_price": {
                    "min": {"field": "price"}
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = SearchQuery().aggregations(genders=agg.Terms(f.gender))
    assert_expression(
        sq,
        {
            "aggregations": {
                "genders": {
                    "terms": {"field": "gender"}
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .aggregations(
            type=agg.Terms(f.type, aggs={'min_price': agg.Min(f.price)})
        )
    )
    assert_expression(
        sq,
        {
            "aggregations": {
                "type": {
                    "terms": {"field": "type"},
                    "aggregations": {
                        "min_price": {
                            "min": {"field": "price"}
                        }
                    }
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .aggregations(
            top_tags=(
                agg.Terms(
                    f.tags,
                    size=3,
                    aggs={
                        'top_tag_hits': agg.TopHits(
                            sort=f.last_activity_date.desc(),
                            size=1,
                            _source=Params(include=[f.title]))
                    }
                )
            )
        )
    )
    assert_expression(
        sq,
        {
            "aggregations": {
                "top_tags": {
                    "terms": {
                        "field": "tags",
                        "size": 3
                    },
                    "aggregations": {
                        "top_tag_hits": {
                            "top_hits": {
                                "sort": {
                                    "last_activity_date": "desc"
                                },
                                "_source": {
                                    "include": ["title"]
                                },
                                "size": 1
                            }
                        }
                    }
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}

    sq = (
        SearchQuery()
        .aggregations({
            'top_sites': agg.Terms(
                f.domain,
                order=Sort('top_hit', 'desc'),
                aggs={
                    'top_tags_hits': agg.TopHits(),
                    'top_hit': agg.Max(script='_doc.score'),
                }
            )
        })
    )
    assert_expression(
        sq,
        {
            "aggregations": {
                "top_sites": {
                    "terms": {
                        "field": "domain",
                        "order": {
                            "top_hit": "desc"
                        }
                    },
                    "aggregations": {
                        "top_tags_hits": {
                            "top_hits": {}
                        },
                        "top_hit": {
                            "max": {
                                "script": "_doc.score"
                            }
                        }
                    }
                }
            }
        },
        compiler
    )
    assert collect_doc_classes(sq) == {DynamicDocument}


def test_search_query__count(client, index, compiler):
    client.count.return_value = {
        "count": 1024,
        "_shards": {
            "total": 5,
            "successful": 5,
            "failed": 0
        }
    }
    assert (
        SearchQuery(index=index, doc_cls=index['car']).count()
    ) == 1024
    if compiler.min_es_version < (6,):
        client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={},
        )
    else:
        client.count.assert_called_with(
            index='test',
            body={},
        )

    client.count.return_value = {
        "count": 2,
        "_shards": {
            "total": 5,
            "successful": 5,
            "failed": 0
        }
    }
    assert (
        SearchQuery(index=index)
        .filter(index['car'].status == 1)
        .function_score({'boost_factor': 3})
        .count()
    ) == 2
    if compiler.min_es_version < (2,):
        client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={
                "query": {
                    "filtered": {
                        "query": {
                            "function_score": {
                                "functions": [
                                    {
                                        "boost_factor": 3
                                    }
                                ]
                            }
                        },
                        "filter": {
                            "term": {"status": 1}
                        }
                    }
                }
            }
        )
    else:
        body = {
            "query": {
                "bool": {
                    "must": {
                        "function_score": {
                            "functions": [
                                {
                                    "boost_factor": 3
                                }
                            ]
                        }
                    },
                    "filter": {
                        "term": {"status": 1}
                    }
                }
            }
        }
        if compiler.min_es_version < (6,):
            client.count.assert_called_with(
                index='test',
                doc_type='car',
                body=body
            )
        else:
            client.count.assert_called_with(
                index='test',
                body=body
            )


def test_search_query__exists(client, index, compiler):
    if compiler.min_es_version < (5,):
        client.exists.return_value = {'exists': True}
    else:
        client.search.return_value = {
            "hits": {"total": 1, "max_score": 1.0, "hits": []}
        }
    assert SearchQuery(index=index, doc_cls=index['car']).exists() is True
    if compiler.min_es_version < (5,):
        client.exists.assert_called_with(
            index='test',
            doc_type='car',
            body={},
        )
    elif compiler.min_es_version < (6,):
        client.search.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'size': 0,
                'terminate_after': 1,
            },
        )
    else:
        client.search.assert_called_with(
            index='test',
            body={
                'size': 0,
                'terminate_after': 1,
            },
        )

    if compiler.min_es_version < (5,):
        client.exists.return_value = {'exists': False}
    else:
        client.search.return_value = {
            "hits": {"total": 0, "max_score": 0.0, "hits": []}
        }
    assert (
        SearchQuery(index=index)
        .filter(index['car'].status == 1)
        .function_score({'boost_factor': 3})
        .exists()
    ) is False
    if compiler.min_es_version < (2,):
        compiled_query = {
            "filtered": {
                "filter": {
                    "term": {"status": 1}
                },
                "query": {
                    "function_score": {
                        "functions": [
                            {
                                "boost_factor": 3
                            }
                        ]
                    }
                }
            }
        }
    else:
        compiled_query = {
            "bool": {
                "filter": {
                    "term": {"status": 1}
                },
                "must": {
                    "function_score": {
                        "functions": [
                            {
                                "boost_factor": 3
                            }
                        ]
                    }
                }
            }
        }
    if compiler.min_es_version < (5,):
        client.exists.assert_called_with(
            index='test',
            doc_type='car',
            body={"query": compiled_query}
        )
    elif compiler.min_es_version < (6,):
        client.search.assert_called_with(
            index='test',
            doc_type='car',
            body={
                "size": 0,
                "terminate_after": 1,
                "query": compiled_query
            },
        )
    else:
        client.search.assert_called_with(
            index='test',
            body={
                "size": 0,
                "terminate_after": 1,
                "query": compiled_query
            },
        )


def test_search(client, index, compiler):
    class CarObject(object):
        def __init__(self, id):
            self.id = id
            self.name = '{0}:{0}'.format(id)

    def _obj_mapper(ids):
        return {id: CarObject(int(id)) for id in ids}
    obj_mapper = Mock(wraps=_obj_mapper)

    class NameDocument(Document):
        first = Field(String)
        last = Field(String)

    class CarSellerDocument(Document):
        name = Field(Object(NameDocument))
        rating = Field(Float)

    class CarDocument(Document):
        __doc_type__ = 'car'

        vendor = Field(String)
        model = Field(String)
        year = Field(Integer)
        seller = Field(Object(CarSellerDocument))

    client.search = Mock(
        return_value={
            'hits': {
                'hits': [
                    {
                        '_id': '31888815',
                        '_type': 'car',
                        '_index': 'ads',
                        '_score': 4.675524,
                        '_source': {
                            'vendor': 'Subaru',
                            'model': 'Imprezza',
                            'year': 2004,
                        },
                    },
                    {
                        '_id': '987321',
                        '_type': 'car',
                        '_index': 'ads',
                        '_score': 3.654321,
                        '_source': {
                            'vendor': 'Subaru',
                            'model': 'Forester',
                            'year': 2007,
                        },
                    }
                ],
                'max_score': 4.675524,
                'total': 6234
            },
            'timed_out': False,
            'took': 47
        }
    )
    sq = (
        index.query(
            CarDocument.seller.name.first.match('Alex'),
            search_type='dfs_query_then_fetch',
        )
        .filter(CarDocument.seller.rating > 4)
        .with_instance_mapper(obj_mapper)
    )
    assert collect_doc_classes(sq) == {CarDocument}
    get_result = sq.get_result()
    assert len(get_result) == 2
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        assert len(sq.results) == 2
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        assert len(sq.result) == 2
    assert sq.get_query_compiler() is compiler.compiled_query
    if compiler.min_es_version < (2,):
        client.search.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'filtered': {
                        'query': {
                            'match': {'seller.name.first': 'Alex'}
                        },
                        'filter': {
                            'range': {'seller.rating': {'gt': 4.0}}
                        }
                    }
                }
            },
            search_type='dfs_query_then_fetch',
        )
    else:
        body = {
            'query': {
                'bool': {
                    'must': {
                        'match': {'seller.name.first': 'Alex'}
                    },
                    'filter': {
                        'range': {'seller.rating': {'gt': 4.0}}
                    }
                }
            }
        }
        if compiler.min_es_version < (6,):
            client.search.assert_called_with(
                index='test',
                doc_type='car',
                body=body,
                search_type='dfs_query_then_fetch',
            )
        else:
            client.search.assert_called_with(
                index='test',
                body=body,
                search_type='dfs_query_then_fetch',
            )

    assert len(sq.get_result().hits) == 2
    doc = sq.get_result().hits[0]
    assert isinstance(doc, CarDocument)
    assert doc._id == '31888815'
    assert doc._type == 'car'
    assert doc._index == 'ads'
    assert doc._score == 4.675524
    assert doc.vendor == 'Subaru'
    assert doc.model == 'Imprezza'
    assert doc.year == 2004
    assert doc.instance.id == 31888815
    assert doc.instance.name == '31888815:31888815'
    doc = sq.get_result().hits[1]
    assert isinstance(doc, CarDocument)
    assert doc._id == '987321'
    assert doc._type == 'car'
    assert doc._index == 'ads'
    assert doc._score == 3.654321
    assert doc.vendor == 'Subaru'
    assert doc.model == 'Forester'
    assert doc.year == 2007
    assert doc.instance.id == 987321
    assert doc.instance.name == '987321:987321'
    assert obj_mapper.call_count == 1


def test_multi_type_search(client, index, compiler):
    def seller_mapper(ids):
        return {id: '{0}-{0}'.format(id) for id in ids}

    def customer_mapper(ids):
        return {id: '{0}:{0}'.format(id) for id in ids}

    sq = (
        index.query(
            index['seller'].name.first.match('Alex'),
            doc_cls=(index['seller'], index['customer'])
        )
        .with_instance_mapper({
            index['seller']: seller_mapper,
            index['customer']: customer_mapper
        })
        .filter(index['customer'].birthday >= datetime.date(1960, 1, 1))
        .limit(2)
    )
    assert collect_doc_classes(sq) == {index['seller'], index['customer']}

    client.search = Mock(
        return_value={
            'hits': {
                'hits': [
                    {
                        '_id': '3',
                        '_type': 'customer',
                        '_index': 'test',
                        '_score': 2.437682,
                        '_source': {
                            'name': {
                                'first': 'Alex',
                                'last': 'Exler'
                            },
                            'birthday': '1966-10-04'
                        },
                    },
                    {
                        '_id': '21',
                        '_type': 'seller',
                        '_index': 'test',
                        '_score': 2.290845,
                        '_source': {
                            'name': {
                                'first': 'Alexa',
                                'last': 'Chung'
                            },
                            'birthday': '1983-10-05',
                            'rating': 4.8
                        },
                    }
                ],
                'max_score': 2.437682,
                'total': 73
            },
            'timed_out': False,
            'took': 25
        }
    )
    results = sq.get_result()
    assert len(results) == 2

    if compiler.min_es_version < (2,):
        client.search.assert_called_with(
            index='test',
            doc_type=OrderTolerantString('customer,seller', ','),
            body={
                'query': {
                    'filtered': {
                        'query': {
                            'match': {'name.first': 'Alex'}
                        },
                        'filter': {
                            'range': {
                                'birthday': {'gte': datetime.date(1960, 1, 1)}
                            }
                        }
                    },
                },
                'size': 2
            }
        )
    elif compiler.min_es_version < (6,):
        client.search.assert_called_with(
            index='test',
            doc_type=OrderTolerantString('customer,seller', ','),
            body={
                'query': {
                    'bool': {
                        'must': {
                            'match': {'name.first': 'Alex'}
                        },
                        'filter': {
                            'range': {
                                'birthday': {'gte': datetime.date(1960, 1, 1)}
                            }
                        }
                    },
                },
                'size': 2
            }
        )
    else:
        # TODO: Should we use our own type field for Elasticsearch 6.x and more
        client.search.assert_called_with(
            index='test',
            body={
                'query': {
                    'bool': {
                        'must': {
                            'match': {'name.first': 'Alex'}
                        },
                        'filter': {
                            'range': {
                                'birthday': {'gte': datetime.date(1960, 1, 1)}
                            }
                        }
                    },
                },
                'size': 2
            }
        )

    assert len(sq.get_result().hits) == 2
    doc = sq.get_result().hits[0]
    assert isinstance(doc, index['customer'])
    assert doc._id == '3'
    assert doc._type == 'customer'
    assert doc._index == 'test'
    assert doc._score == 2.437682
    assert doc.name.first == 'Alex'
    assert doc.name.last == 'Exler'
    assert doc.birthday == '1966-10-04'
    assert doc.instance == '3:3'
    doc = sq.get_result().hits[1]
    assert isinstance(doc, index['seller'])
    assert doc._id == '21'
    assert doc._type == 'seller'
    assert doc._index == 'test'
    assert doc._score == 2.290845
    assert doc.name.first == 'Alexa'
    assert doc.name.last == 'Chung'
    assert doc.birthday == '1983-10-05'
    assert doc.rating == 4.8
    assert doc.instance == '21-21'


def test_search_query__scroll(client, index, compiler):
    client.search = Mock(
        return_value={
            '_scroll_id': 'c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1',
            'hits': {
                'total': 93570,
                'max_score': 0,
                'hits': []
            },
            'timed_out': False,
            'took': 90
        }
    )
    sq = (
        index.search_query(
            search_type='scan', scroll='1m', doc_cls=index['type']
        )
        .limit(1000)
    )
    result = sq.get_result()

    if compiler.min_es_version < (6,):
        client.search.assert_called_with(
            index='test',
            doc_type='type',
            body={
                'size': 1000
            },
            search_type='scan',
            scroll='1m',
        )
    else:
        client.search.assert_called_with(
            index='test',
            body={
                'size': 1000
            },
            search_type='scan',
            scroll='1m',
        )
    assert result.scroll_id == 'c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1'
    assert list(result) == []


def test_delete(client, index, compiler):
    index.query(index['car'].vendor == 'Focus').delete()
    body = {
        'query': {
            'term': {'vendor': 'Focus'}
        }
    }
    if compiler.min_es_version < (6,):
        client.delete_by_query.assert_called_with(
            index='test',
            doc_type='car',
            body=body,
        )
    else:
        client.delete_by_query.assert_called_with(
            index='test',
            body=body,
        )

    index.query(index['car'].vendor == 'Focus') \
        .filter(index['car'].status == 0) \
        .limit(20) \
        .delete(timeout='1m', replication='async')
    if compiler.min_es_version < (2,):
        body = {
            "query": {
                "filtered": {
                    "query": {
                        "term": {"vendor": "Focus"}
                    },
                    "filter": {
                        "term": {"status": 0}
                    }
                }
            }
        }
    else:
        body = {
            "query": {
                "bool": {
                    "must": {
                        "term": {"vendor": "Focus"}
                    },
                    "filter": {
                        "term": {"status": 0}
                    }
                }
            }
        }
    if compiler.min_es_version < (6,):
        client.delete_by_query.assert_called_with(
            index='test',
            doc_type='car',
            body=body,
            timeout='1m',
            replication='async',
        )
    else:
        client.delete_by_query.assert_called_with(
            index='test',
            body=body,
            timeout='1m',
            replication='async',
        )


def test_search_query__to_bool(index, client):
    assert bool(index.search_query())
    assert not client.count.called
    assert not client.search.called


def test_search_params():
    sq = SearchQuery()
    assert sq._search_params == {}

    sq = SearchQuery(search_type='count')
    assert sq._search_params == {'search_type': 'count'}
    sq = sq.with_search_type(None)
    assert sq._search_params == {}
    sq = sq.with_search_params(
        {'search_type': 'count', 'query_cache': True},
        unknown_param='none'
    )
    assert sq._search_params == {
        'search_type': 'count',
        'query_cache': True,
        'unknown_param': 'none',
    }
    sq = sq.with_routing(1234)
    assert sq._search_params == {
        'routing': 1234,
        'search_type': 'count',
        'query_cache': True,
        'unknown_param': 'none',
    }


def test_search_query___suggest(compiler):
    sq = SearchQuery()
    sq = sq.suggest(text="Complete",
                    in_title={'term': {'size': 3, 'field': 'title'}})
    assert_expression(
        sq,
        {
            'suggest': {
                'text': 'Complete',
                'in_title': {
                    'term': {
                        'size': 3,
                        'field': 'title',
                    }
                }
            }
        },
        compiler
    )

    sq = sq.suggest(in_body={'completion': {'field': 'body'}})
    assert_expression(
        sq,
        {
            'suggest': {
                'text': 'Complete',
                'in_title': {
                    'term': {
                        'size': 3,
                        'field': 'title',
                    }
                },
                'in_body': {
                    'completion': {
                        'field': 'body',
                    }
                },
            }
        },
        compiler
    )

    sq = sq.suggest(None)
    assert_expression(sq, {}, compiler)


def test_highlight(index, compiler):
    sq = SearchQuery()
    sq = sq.highlight(fields={'content': {}})
    assert collect_doc_classes(sq) == set()
    assert_expression(
        sq,
        {
            "highlight": {
                "fields": {
                    "content": {}
                }
            }
        },
        compiler
    )

    sq = SearchQuery()
    sq = sq.highlight(
        fields=[index['test'].content],
        pre_tags=['[em]'],
        post_tags=['[/em]']
    )
    assert collect_doc_classes(sq) == {index['test']}
    assert_expression(
        sq,
        {
            "highlight": {
                "fields": [
                    {
                        "content": {}
                    }
                ],
                "pre_tags": ["[em]"],
                "post_tags": ["[/em]"]
            }
        },
        compiler
    )

    sq = SearchQuery()
    sq = sq.highlight(
        fields=[
            index['test'].content.highlight(
                matched_fields=[
                    index['test'].content,
                    index['test'].content.plain,
                ],
                type='fvh',
            )
        ]
    )
    assert collect_doc_classes(sq) == {index['test']}
    assert_expression(
        sq,
        {
            "highlight": {
                "fields": [
                    {
                        "content": {
                            "matched_fields": ["content", "content.plain"],
                            "type": "fvh"
                        }
                    }
                ]
            }
        },
        compiler
    )
