import datetime
import warnings
from unittest.mock import Mock

from elasticmagic import (
    Document, DynamicDocument,
    SearchQuery, Params, Term, MultiMatch,
    FunctionScore, Sort, QueryRescorer, agg
)
from elasticmagic.compiler import Compiler_7_0
from elasticmagic.search import FunctionScoreSettings
from elasticmagic.function import FieldValueFactor, Weight
from elasticmagic.util import collect_doc_classes
from elasticmagic.types import String, Integer, Float, Object
from elasticmagic.expression import Field, Script, SortScript

from .base import BaseTestCase, OrderTolerantString


class SearchQueryTest(BaseTestCase):
    def test_search_query_compile(self):
        f = DynamicDocument.fields

        sq = SearchQuery()
        self.assert_expression(sq, {})
        self.assertEqual(collect_doc_classes(sq), set())

        sq = SearchQuery(Term(f.user, 'kimchy')).limit(10).offset(0)
        self.assert_expression(
            sq,
            {
                "from": 0,
                "size": 10,
                "query": {
                    "term": {"user": "kimchy"}
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = SearchQuery(Term(f.user, 'kimchy')).filter(f.age >= 16)
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery(Term(f.user, 'kimchy'))
            .filter(f.age >= 16)
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery(Term(f.user, 'kimchy'))
            .query(f.user != 'kimchy')
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery(Term(f.user, 'kimchy'))
            .query(None)
        )
        self.assert_expression(sq, {})
        self.assertEqual(collect_doc_classes(sq), set())

        sq = (
            SearchQuery(Term(f.user, 'kimchy'))
            .filter(f.age >= 16)
            .filter(f.lang == 'English')
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .order_by(
                f.opinion_rating.desc(missing='_last'),
                f.opinion_count.desc(),
                f.id
            )
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .order_by(
                f.opinion_rating.desc(missing='_last'),
                f.opinion_count.desc(),
                f.id
            )
            .order_by(None)
            .order_by(None)
        )
        self.assert_expression(sq, {})
        self.assertEqual(collect_doc_classes(sq), set())

        sq = SearchQuery().source(f.name, f.company)
        self.assert_expression(
            sq,
            {
                "_source": ["name", "company"]
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = SearchQuery().source(exclude=[f.name, f.company])
        self.assert_expression(
            sq,
            {
                "_source": {
                    "exclude": ["name", "company"]
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .source(
                include=[f.obj1.wildcard('*'), f.obj2.wildcard('*')],
                # FIXME: f.wildcard('*')
                exclude=DynamicDocument.wildcard('*').description
            )
        )
        self.assert_expression(
            sq,
            {
                "_source": {
                    "include": ["obj1.*", "obj2.*"],
                    "exclude": "*.description"
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .source(None)
            .source(f.name, f.company)
            .source(None)
        )
        self.assert_expression(sq, {})
        self.assertEqual(collect_doc_classes(sq), set())

        sq = (
            SearchQuery()
            .source(f.name, f.company)
            .source(False)
        )
        self.assert_expression(
            sq,
            {
                "_source": False
            }
        )
        self.assertEqual(collect_doc_classes(sq), set())

        sq = (
            SearchQuery()
            .source(True)
        )
        self.assert_expression(
            sq,
            {
                "_source": True
            }
        )
        self.assertEqual(collect_doc_classes(sq), set())

        sq = SearchQuery().fields(f.name, f.company)
        self.assert_expression(
            sq,
            {
                "stored_fields": ["name", "company"]
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .fields("*")
        )
        self.assert_expression(
            sq,
            {
                "stored_fields": ['*']
            }
        )
        self.assertEqual(collect_doc_classes(sq), set())

        sq = (
            SearchQuery()
            .fields(None)
            .fields(f.name, f.company)
            .fields(None)
        )
        self.assert_expression(sq, {})
        self.assertEqual(collect_doc_classes(sq), set())

        sq = (
            SearchQuery()
            .fields(f.name, f.company)
            .fields(f.keywords)
        )
        self.assert_expression(
            sq,
            {
                "stored_fields": ["name", "company", "keywords"]
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .function_score({'random_score': {"seed": 1234}})
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), set())

        sq = (
            SearchQuery()
            .function_score({'field_value_factor': {'field': f.popularity}})
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

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
        self.assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
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
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .filter(f.status == 0)
            .boost_score(
                {'filter': f.discount_percent >= 10, 'weight': 1000},
                {'filter': f.discount_percent >= 50, 'weight': 2000},
                {'filter': f.presence == 'available', 'weight': 10000},
            )
        )
        self.assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
                            "function_score": {
                                "functions": [
                                    {
                                        "filter": {"range": {"discount_percent": {"gte": 10}}},
                                        "weight": 1000
                                    },
                                    {
                                        "filter": {"range": {"discount_percent": {"gte": 50}}},
                                        "weight": 2000
                                    },
                                    {
                                        "filter": {"term": {"presence": "available"}},
                                        "weight": 10000
                                    },
                                ],
                                "score_mode": "sum",
                                "boost_mode": "sum"
                            }
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            }
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
        self.assert_expression(
            sq,
            {
                "query": {
                    "bool": {
                        "must": {
                            "function_score": {
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
                                        "filter": {"range": {"discount_percent": {"gte": 10}}},
                                        "weight": 1000
                                    },
                                    {
                                        "filter": {"range": {"discount_percent": {"gte": 50}}},
                                        "weight": 2000
                                    },
                                ],
                                "score_mode": "max",
                                "boost_mode": "sum"
                            }
                        },
                        "filter": {
                            "term": {"status": 0}
                        }
                    }
                }
            }
        )

        sq = (
            SearchQuery()
            .rescore(
                QueryRescorer(
                    self.index['type'].field1.match('the quick brown', type='phrase', slop=2)
                )
            )
            .rescore(None)
            .rescore(
                QueryRescorer(
                    self.index['type'].field1.match('the quick brown fox', type='phrase', slop=2),
                    window_size=100,
                    query_weight=0.7,
                    rescore_query_weight=1.2
                ),
            )
            .rescore(
                QueryRescorer(
                    FunctionScore(script_score={'script': "log10(doc['numeric'].value + 2)"}),
                    window_size=10,
                    score_mode='multiply'
                ),
            )
        )
        self.assert_expression(
            sq,
            {
                "rescore": [
                    {
                        "query": {
                            "rescore_query": {
                                "match_phrase": {
                                    "field1": {
                                        "query": "the quick brown fox",
                                        "slop": 2
                                    }
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {self.index['type']})
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
        self.assert_expression(
            sq,
            dict(
                script_fields=dict(
                    rating=dict(
                        script=dict(
                            source='doc[params.brand].value +'
                                   'doc[params.color].value',
                            params=dict(
                                brand='brand',
                                color='color',
                            )
                        )
                    )
                )
            )
        )
        sq = sq.script_fields(
            brand=dict(
                source='doc[params.brand].value',
                params=dict(brand='brand')
            )
        )
        self.assert_expression(
            sq,
            dict(
                script_fields=dict(
                    rating=dict(
                        script=dict(
                            source='doc[params.brand].value +'
                                   'doc[params.color].value',
                            params=dict(
                                brand='brand',
                                color='color',
                            )
                        )
                    ),
                    brand=dict(
                        source='doc[params.brand].value',
                        params=dict(brand='brand')
                    )
                )
            )
        )
        sq = sq.script_fields(True)
        self.assert_expression(
            sq,
            dict(
                script_fields=dict(
                    rating=dict(
                        script=dict(
                            source='doc[params.brand].value +'
                                   'doc[params.color].value',
                            params=dict(
                                brand='brand',
                                color='color',
                            )
                        )
                    ),
                    brand=dict(
                        source='doc[params.brand].value',
                        params=dict(brand='brand')
                    )
                )
            )
        )
        sq = sq.script_fields(None)
        self.assert_expression(
            sq,
            dict()
        )
        sq = SearchQuery().post_filter(self.index['shirt'].color == 'red')
        self.assert_expression(
            sq,
            {
                "post_filter": {
                    "term": {"color": "red"}
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {self.index['shirt']})

        sq = (
            SearchQuery()
            .filter(self.index['shirt'].brand == 'gucci')
            .post_filter(self.index['shirt'].color == 'red')
            .post_filter(self.index['shirt'].model == 't-shirt')
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {self.index['shirt']})

        sq = SearchQuery()
        self.assert_expression(
            sq,
            {},
            compiler=Compiler_7_0,
        )

        sq = SearchQuery(track_total_hits=True)
        self.assert_expression(
            sq,
            {
                "track_total_hits": True,
            },
            compiler=Compiler_7_0,
        )

        sq = SearchQuery(track_total_hits=False)
        self.assert_expression(
            sq,
            {
                "track_total_hits": False,
            },
            compiler=Compiler_7_0,
        )

        sq = SearchQuery(track_total_hits=100)
        self.assert_expression(
            sq,
            {
                "track_total_hits": 100,
            },
            compiler=Compiler_7_0,
        )

    def test_docvalue_fields(self):
        f = DynamicDocument.fields

        sq = SearchQuery().docvalue_fields(
            f.price,
            f.discount,
        )
        self.assert_expression(
            sq,
            dict(
                docvalue_fields=['price', 'discount']
            )
        )

    def test_aggregations(self):
        f = DynamicDocument.fields

        sq = SearchQuery().aggregations(min_price=agg.Min(f.price))
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "min_price": {
                        "min": {"field": "price"}
                    }
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = SearchQuery().aggregations(genders=agg.Terms(f.gender))
        self.assert_expression(
            sq,
            {
                "aggregations": {
                    "genders": {
                        "terms": {"field": "gender"}
                    }
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery().ext(
                dict(
                    collapse=dict(
                        field='model_id',
                        size=10000,
                        shard_size=1000,
                        sort=SortScript(
                            script=Script(
                                lang='painless',
                                params={
                                    'a': 1,
                                },
                                inline='score * params.a'
                            ),
                            order='asc',
                            script_type='number',
                        )

                    )
                )
            )
        )
        self.assert_expression(
            sq,
            {
                'ext': {
                    "collapse": {
                        'field': 'model_id',
                        'size': 10000,
                        'shard_size': 1000,
                        'sort': {
                            '_script': {
                                'script': {
                                    'source': 'score * params.a',
                                    'lang': 'painless',
                                    'params': {
                                        'a': 1,
                                    },
                                },
                                'order': 'asc',
                                'type': 'number',
                            }
                        }

                    }
                },
            }
        )

        sq = (
            SearchQuery()
            .aggregations(
                type=agg.Terms(f.type, aggs={'min_price': agg.Min(f.price)})
            )
        )
        self.assert_expression(
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
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

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
        self.assert_expression(
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
                                    "size" : 1
                                }
                            }
                        }
                    }
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

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
        self.assert_expression(
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
                            "top_hit" : {
                                "max": {
                                    "script": "_doc.score"
                                }
                            }
                        }
                    }
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

    def test_custom_function_scores(self):
        AD_ONLY_FUNCTION_SCORE = FunctionScoreSettings(
            'AD_ONLY', score_mode='min', boost_mode='replace', min_score=1e-6
        )
        sq = SearchQuery()
        sq = sq.function_score_settings(AD_ONLY_FUNCTION_SCORE)
        self.assert_expression(
            sq,
            {}
        )
        self.assert_expression(
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
            }
        )
        self.assert_expression(
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
            }
        )
        self.assert_expression(
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
            }
        )
        with self.assertRaises(ValueError):
            ANOTHER_FUNCTION_SCORE = FunctionScoreSettings('ANOTHER')
            sq.function_score(ANOTHER_FUNCTION_SCORE, Weight(1))

    def test_count(self):
        self.client.count.return_value = {
            "count" : 1024,
            "_shards" : {
                "total" : 5,
                "successful" : 5,
                "failed" : 0
            }
        }
        self.assertEqual(
            SearchQuery(index=self.index, doc_cls=self.index['car'])
            .count(),
            1024
        )
        self.client.count.assert_called_with(
            index='test',
            body={},
        )

        self.client.count.return_value = {
            "count" : 2,
            "_shards" : {
                "total" : 5,
                "successful" : 5,
                "failed" : 0
            }
        }
        self.assertEqual(
            SearchQuery(index=self.index)
            .filter(self.index['car'].status == 1)
            .function_score({'boost_factor': 3})
            .count(),
            2
        )
        self.client.count.assert_called_with(
            index='test',
            body={
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
        )

    def test_exists(self):
        self.client.search.return_value = {
            "hits": {"total": 1, "max_score": 1.0, "hits": []}
        }
        self.assertEqual(
            SearchQuery(index=self.index, doc_cls=self.index['car']).exists(),
            True
        )
        self.client.search.assert_called_with(
            index='test',
            body={
                'size': 0,
                'terminate_after': 1,
            },
        )

        self.client.search.return_value = {
            "hits": {"total": 0, "max_score": 0.0, "hits": []}
        }
        self.assertEqual(
            SearchQuery(index=self.index)
            .filter(self.index['car'].status == 1)
            .function_score({'boost_factor': 3})
            .exists(),
            False
        )
        self.client.search.assert_called_with(
            index='test',
            body={
                "size": 0,
                "terminate_after": 1,
                "query": {
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
            },
        )

    def test_search(self):
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

        self.client.search = Mock(
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
            self.index.query(
                CarDocument.seller.name.first.match('Alex'),
                search_type='dfs_query_then_fetch',
            )
            .filter(CarDocument.seller.rating > 4)
            .with_instance_mapper(obj_mapper)
        )
        self.assertEqual(collect_doc_classes(sq), {CarDocument})
        get_result = sq.get_result()
        self.assertEqual(len(get_result), 2)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self.assertEqual(len(sq.results), 2)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self.assertEqual(len(sq.result), 2)
        self.assertEqual(type(sq.get_query_compiler()), type(Compiler_7_0))
        self.client.search.assert_called_with(
            index='test',
            body={
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
            },
            search_type='dfs_query_then_fetch',
        )

        self.assertEqual(len(sq.get_result().hits), 2)
        doc = sq.get_result().hits[0]
        self.assertIsInstance(doc, CarDocument)
        self.assertEqual(doc._id, '31888815')
        self.assertEqual(doc._type, 'car')
        self.assertEqual(doc._index, 'ads')
        self.assertAlmostEqual(doc._score, 4.675524)
        self.assertEqual(doc.vendor, 'Subaru')
        self.assertEqual(doc.model, 'Imprezza')
        self.assertEqual(doc.year, 2004)
        self.assertEqual(doc.instance.id, 31888815)
        self.assertEqual(doc.instance.name, '31888815:31888815')
        doc = sq.get_result().hits[1]
        self.assertIsInstance(doc, CarDocument)
        self.assertEqual(doc._id, '987321')
        self.assertEqual(doc._type, 'car')
        self.assertEqual(doc._index, 'ads')
        self.assertAlmostEqual(doc._score, 3.654321)
        self.assertEqual(doc.vendor, 'Subaru')
        self.assertEqual(doc.model, 'Forester')
        self.assertEqual(doc.year, 2007)
        self.assertEqual(doc.instance.id, 987321)
        self.assertEqual(doc.instance.name, '987321:987321')
        self.assertEqual(obj_mapper.call_count, 1)

    def test_multi_type_search(self):
        def seller_mapper(ids):
            return {id: '{0}-{0}'.format(id) for id in ids}

        def customer_mapper(ids):
            return {id: '{0}:{0}'.format(id) for id in ids}

        sq = (
            self.index.query(
                self.index['seller'].name.first.match('Alex'),
                doc_cls=(self.index['seller'], self.index['customer'])
            )
            .with_instance_mapper({self.index['seller']: seller_mapper,
                                   self.index['customer']: customer_mapper})
            .filter(self.index['customer'].birthday >= datetime.date(1960, 1, 1))
            .limit(2)
        )
        self.assertEqual(collect_doc_classes(sq), {self.index['seller'], self.index['customer']})

        self.client.search = Mock(
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
        self.assertEqual(len(results), 2)

        self.client.search.assert_called_with(
            index='test',
            body={
                'query': {
                    'bool': {
                        'must': {
                            'match': {'name.first': 'Alex'}
                        },
                        'filter': {
                            'range': {'birthday': {'gte': datetime.date(1960, 1, 1)}}
                        }
                    },
                },
                'size': 2
            }
        )

        self.assertEqual(len(sq.get_result().hits), 2)
        doc = sq.get_result().hits[0]
        self.assertIsInstance(doc, self.index['customer'])
        self.assertEqual(doc._id, '3')
        self.assertEqual(doc._type, 'customer')
        self.assertEqual(doc._index, 'test')
        self.assertAlmostEqual(doc._score, 2.437682)
        self.assertEqual(doc.name.first, 'Alex')
        self.assertEqual(doc.name.last, 'Exler')
        self.assertEqual(doc.birthday, '1966-10-04')
        self.assertEqual(doc.instance, '3:3')
        doc = sq.get_result().hits[1]
        self.assertIsInstance(doc, self.index['seller'])
        self.assertEqual(doc._id, '21')
        self.assertEqual(doc._type, 'seller')
        self.assertEqual(doc._index, 'test')
        self.assertAlmostEqual(doc._score, 2.290845)
        self.assertEqual(doc.name.first, 'Alexa')
        self.assertEqual(doc.name.last, 'Chung')
        self.assertEqual(doc.birthday, '1983-10-05')
        self.assertAlmostEqual(doc.rating, 4.8)
        self.assertEqual(doc.instance, '21-21')

    def test_search_scroll(self):
        self.client.search = Mock(
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
            self.index.search_query(
                search_type='scan', scroll='1m', doc_cls=self.index['type']
            )
            .limit(1000)
        )
        result = sq.get_result()

        self.client.search.assert_called_with(
            index='test',
            body={
                'size': 1000
            },
            search_type='scan',
            scroll='1m',
        )
        self.assertEqual(result.scroll_id, 'c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1')
        self.assertEqual(list(result), [])

    def test_delete(self):
        self.index.query(self.index['car'].vendor == 'Focus').delete()
        self.client.delete_by_query.assert_called_with(
            index='test',
            body={
                'query': {
                    'term': {'vendor': 'Focus'}
                }
            },
        )

        self.index.query(self.index['car'].vendor == 'Focus') \
            .filter(self.index['car'].status == 0) \
            .limit(20) \
            .delete(timeout='1m', replication='async')
        self.client.delete_by_query.assert_called_with(
            index='test',
            body={
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
            },
            timeout='1m',
            replication='async',
        )

    def test_as_bool(self):
        self.assertTrue(bool(self.index.search_query()))
        self.assertFalse(self.client.count.called)
        self.assertFalse(self.client.search.called)

    def test_search_params(self):
        sq = SearchQuery()
        self.assertEqual(
            sq._search_params,
            {}
        )

        sq = SearchQuery(search_type='count')
        self.assertEqual(
            sq._search_params,
            {
                'search_type': 'count'
            }
        )
        sq = sq.with_search_type(None)
        self.assertEqual(
            sq._search_params,
            {}
        )
        sq = SearchQuery(stats='tag')
        self.assertEqual(
            sq._search_params,
            {
                'stats': 'tag'
            }
        )
        sq = sq.with_stats(None)
        self.assertEqual(
            sq._search_params,
            {}
        )
        sq = sq.with_search_params({'search_type': 'count', 'query_cache': True}, unknown_param='none')
        self.assertEqual(
            sq._search_params,
            {
                'search_type': 'count',
                'query_cache': True,
                'unknown_param': 'none',
            }
        )
        sq = sq.with_routing(1234)
        self.assertEqual(
            sq._search_params,
            {
                'routing': 1234,
                'search_type': 'count',
                'query_cache': True,
                'unknown_param': 'none',
            }
        )

    def test_suggest(self):
        sq = SearchQuery()
        sq = sq.suggest(text="Complete",
                        in_title={'term': {'size': 3, 'field': 'title'}})

        self.assert_expression(
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
            }
        )

        sq = sq.suggest(in_body={'completion': {'field': 'body'}})
        self.assert_expression(
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
            }
        )

        sq = sq.suggest(None)
        self.assert_expression(sq, {})

    def test_highlight(self):
        sq = SearchQuery()
        sq = sq.highlight(fields={'content': {}})
        self.assertEqual(collect_doc_classes(sq), set())
        self.assert_expression(
            sq,
            {
                "highlight": {
                    "fields": {
                        "content": {}
                    }
                }
            }
        )

        sq = SearchQuery()
        sq = sq.highlight(
            fields=[self.index['test'].content],
            pre_tags=['[em]'],
            post_tags=['[/em]']
        )
        self.assertEqual(collect_doc_classes(sq), {self.index['test']})
        self.assert_expression(
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
            }
        )

        sq = SearchQuery()
        sq = sq.highlight(
            fields=[
                self.index['test'].content.highlight(
                    matched_fields=[self.index['test'].content, self.index['test'].content.plain],
                    type='fvh',
                )
            ]
        )
        self.assertEqual(collect_doc_classes(sq), {self.index['test']})
        self.assert_expression(
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
            }
        )
        
    def test_search_after(self):
        sq = SearchQuery()
        self.assert_expression(
            sq,
            {},
            compiler=Compiler_7_0,
        )
        
        

        sq = SearchQuery()
        sq.search_after(1,2,3)
        self.assert_expression(
            sq,
            {
                "search_after": (1, 2, 3),
            },
            compiler=Compiler_7_0,
        )

        sq.search_after(None)
        self.assert_expression(
            sq,
            {},
            compiler=Compiler_7_0,
        )
