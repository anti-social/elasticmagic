from mock import Mock, MagicMock

from elasticmagic import (
    Index, Document, SearchQuery, Params, Term, Bool, MultiMatch,
    FunctionScore, Sort, agg
)
from elasticmagic.types import String, Integer, Float, Object
from elasticmagic.expression import Fields, Field

from .base import BaseTestCase


class SearchQueryTest(BaseTestCase):
    def test_search_query_compile(self):
        f = Fields()

        self.assert_expression(
            SearchQuery(),
            {}
        )

        self.assert_expression(
            SearchQuery(Term(f.user, 'kimchy')).limit(10).offset(0),
            {
                "from": 0,
                "size": 10,
                "query": {
                    "term": {"user": "kimchy"}
                }
            }
        )

        self.assert_expression(
            SearchQuery(Term(f.user, 'kimchy')).filter(f.age >= 16),
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
            }
        )
        self.assert_expression(
            SearchQuery(Term(f.user, 'kimchy'))
            .filter(f.age >= 16)
            .filter(f.lang == 'English'),
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
            }
        )

        self.assert_expression(
            SearchQuery().order_by(
                f.opinion_rating.desc(missing='_last'),
                f.opinion_count.desc(),
                f.id
            ),
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
        self.assert_expression(
            (
                SearchQuery()
                .order_by(
                    f.opinion_rating.desc(missing='_last'),
                    f.opinion_count.desc(),
                    f.id
                )
                .order_by(None)
                .order_by(None)
            ),
            {}
        )

        self.assert_expression(
            SearchQuery().fields(f.name, f.company),
            {
                "_source": ["name", "company"]
            }
        )
        self.assert_expression(
            SearchQuery().fields(f.name, f.company).fields(None),
            {}
        )
        self.assert_expression(
            SearchQuery().fields(f.name, f.company).fields(False),
            {
                "_source": False
            }
        )

        self.assert_expression(
            SearchQuery().boost_function({'random_score': {"seed": 1234}}),
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
        self.assert_expression(
            (
                SearchQuery(MultiMatch('Iphone 6', fields=[f.name, f.description]))
                .filter(f.status == 0)
                .boost_function({'_score': {"seed": 1234}})
                .boost_function(None)
                .boost_function({'field_value_factor': {'field': f.popularity,
                                                        'factor': 1.2,
                                                        'modifier': 'sqrt'}},
                                boost_mode='sum')
                .boost_function({'boost_factor': 3,
                                 'filter': f.region == 12})
            ),
            {
                "query": {
                    "filtered": {
                        "query": {
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

        self.assert_expression(
            SearchQuery(self.index.t.field1.match('the quick brown', type='boolean', operator='or'))
            .rescore(self.index.t.field1.match('the quick brown', type='phrase', slop=2),
                     window_size=100,
                     query_weight=0.7,
                     rescore_query_weight=1.2)
            .rescore(FunctionScore(script_score={'script': "log10(doc['numeric'].value + 2)"}),
                     window_size=10,
                     score_mode='multiply'),
            {
                "query": {
                    "match": {
                        "field1": {
                            "operator": "or",
                            "query": "the quick brown",
                            "type": "boolean"
                        }
                    }
                },
                "rescore": [
                    {
                        "window_size": 100,
                        "query": {
                        "rescore_query": {
                            "match": {
                                "field1": {
                                    "query": "the quick brown",
                                    "type": "phrase",
                                    "slop": 2
                                }
                            }
                        },
                            "query_weight": 0.7,
                            "rescore_query_weight": 1.2
                        }
                    },
                    {
                        "window_size": 10,
                        "query": {
                            "score_mode": "multiply",
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

        self.assert_expression(
            SearchQuery().post_filter(self.index.shirt.color == 'red'),
            {
                "post_filter": {
                    "term": {"color": "red"}
                }
            }
        )
        self.assert_expression(
            SearchQuery()
            .filter(self.index.shirt.brand == 'gucci')
            .post_filter(self.index.shirt.color == 'red')
            .post_filter(self.index.shirt.model == 't-shirt'),
            {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {"brand": "gucci"}
                        }
                    }
                },
                "post_filter": {
                    "and": [
                        {
                            "term": {"color": "red"}
                        },
                        {
                            "term": {"model": "t-shirt"}
                        }
                    ]
                }
            }
        )

    def test_aggregations(self):
        f = Fields()

        self.assert_expression(
            SearchQuery().aggregations(min_price=agg.Min(f.price)),
            {
                "aggregations": {
                    "min_price": {
                        "min": {"field": "price"}
                    }
                }
            }
        )

        self.assert_expression(
            SearchQuery().aggregations(genders=agg.Terms(f.gender)),
            {
                "aggregations": {
                    "genders": {
                        "terms": {"field": "gender"}
                    }
                }
            }
        )

        self.assert_expression(
            SearchQuery().aggregations(type=agg.Terms(f.type, aggs={'min_price': agg.Min(f.price)})),
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

        self.assert_expression(
            SearchQuery().aggregations(
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
            ),
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
        self.assert_expression(
            SearchQuery().aggregations(
                top_sites=(
                    agg.Terms(
                        f.domain,
                        order=Sort('top_hit', 'desc'),
                        aggs={
                            'top_tags_hits': agg.TopHits(),
                            'top_hit': agg.Max(script='_doc.score'),
                        }
                    )
                )
            ),
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

    def test_count(self):
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
            .filter(self.index.car.status == 1)
            .boost_function({'boost_factor': 3})
            .count(),
            2
        )
        self.client.count.assert_called_with(
            index='test',
            doc_type='car',
            body={
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {"status": 1}
                        }
                    }
                }
            }
        )

    def test_search_index(self):
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
            
        es_client = MagicMock()
        es_client.search = MagicMock(
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
        es_index = Index(es_client, 'ads')
        sq = (
            es_index.query(
                CarDocument.seller.name.first.match('Alex'),
            )
            .filter(CarDocument.seller.rating > 4)
            .with_instance_mapper(obj_mapper)
        )
        results = sq.results

        es_client.search.assert_called_with(
            index='ads',
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
        )

        self.assertEqual(len(sq.results.hits), 2)
        doc = sq.results.hits[0]
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
        doc = sq.results.hits[1]
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

    def test_delete(self):
        es_client = MagicMock()
        es_index = Index(es_client, 'ads')

        es_index.query(es_index.car.vendor == 'Focus').delete()
        es_client.delete_by_query.assert_called_with(
            index='ads',
            doc_type='car',
            body={
                'query': {
                    'term': {'vendor': 'Focus'}
                }
            },
        )

        es_index.query(es_index.car.vendor == 'Focus') \
                .filter(es_index.car.status == 0) \
                .limit(20) \
                .delete(timeout='1m', replication='async')
        es_client.delete_by_query.assert_called_with(
            index='ads',
            doc_type='car',
            body={
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
            },
            timeout='1m',
            replication='async',
        )
