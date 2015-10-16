import datetime
from mock import Mock, MagicMock

from elasticmagic import Index
from elasticmagic import (
    Index, Document, DynamicDocument,
    SearchQuery, Params, Term, Bool, MultiMatch,
    FunctionScore, Sort, QueryRescorer, agg
)
from elasticmagic.util import collect_doc_classes
from elasticmagic.types import String, Integer, Float, Object
from elasticmagic.expression import Field

from .base import BaseTestCase


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

        self.assert_expression(
            SearchQuery()
            .function_score({'random_score': {"seed": 1234}}),
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
        self.assertEqual(collect_doc_classes(sq), {DynamicDocument})

        sq = (
            SearchQuery()
            .rescore(
                QueryRescorer(
                    self.index.t.field1.match('the quick brown', type='phrase', slop=2)
                )
            )
            .rescore(None)
            .rescore(
                QueryRescorer(
                    self.index.t.field1.match('the quick brown fox', type='phrase', slop=2),
                    query_weight=0.7,
                    rescore_query_weight=1.2
                ),
                window_size=100,
            )
            .rescore(
                QueryRescorer(
                    FunctionScore(script_score={'script': "log10(doc['numeric'].value + 2)"}),
                    score_mode='multiply'
                ),
                window_size=10,
            )
        )
        self.assert_expression(
            sq,
            {
                "rescore": [
                    {
                        "window_size": 100,
                        "query": {
                            "rescore_query": {
                                "match": {
                                    "field1": {
                                        "query": "the quick brown fox",
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
        self.assertEqual(collect_doc_classes(sq), {self.index.t})

        sq = SearchQuery().post_filter(self.index.shirt.color == 'red')
        self.assert_expression(
            sq,
            {
                "post_filter": {
                    "term": {"color": "red"}
                }
            }
        )
        self.assertEqual(collect_doc_classes(sq), {self.index.shirt})

        sq = (
            SearchQuery()
            .filter(self.index.shirt.brand == 'gucci')
            .post_filter(self.index.shirt.color == 'red')
            .post_filter(self.index.shirt.model == 't-shirt')
        )
        self.assert_expression(
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
            }
        )
        self.assertEqual(collect_doc_classes(sq), {self.index.shirt})

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
            SearchQuery(index=self.index, doc_cls=self.index.car)
            .count(),
            1024
        )
        self.client.count.assert_called_with(
            index='test',
            doc_type='car',
            body=None,
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
            .filter(self.index.car.status == 1)
            .function_score({'boost_factor': 3})
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

    def test_exists(self):
        self.client.search_exists.return_value = {"exists" : True}
        self.assertEqual(
            SearchQuery(index=self.index, doc_cls=self.index.car).exists(refresh=True),
            True
        )
        self.client.search_exists.assert_called_with(
            index='test',
            doc_type='car',
            body=None,
            refresh=True
        )

        self.client.search_exists.return_value = {"exists" : False}
        self.assertEqual(
            SearchQuery(index=self.index)
            .filter(self.index.car.status == 1)
            .function_score({'boost_factor': 3})
            .exists(),
            False
        )
        self.client.search_exists.assert_called_with(
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
            
        self.client.search = MagicMock(
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
        results = sq.result

        self.client.search.assert_called_with(
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

        self.assertEqual(len(sq.result.hits), 2)
        doc = sq.result.hits[0]
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
        doc = sq.result.hits[1]
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
                self.index.seller.name.first.match('Alex'),
                doc_cls=(self.index.seller, self.index.customer)
            )
            .with_instance_mapper({self.index.seller: seller_mapper,
                                   self.index.customer: customer_mapper})
            .filter(self.index.customer.birthday >= datetime.date(1960, 1, 1))
            .limit(2)
        )
        self.assertEqual(collect_doc_classes(sq), {self.index.seller, self.index.customer})

        self.client.search = MagicMock(
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
        results = sq.result

        self.client.search.assert_called_with(
            index='test',
            doc_type='seller,customer',
            body={
                'query': {
                    'filtered': {
                        'query': {
                            'match': {'name.first': 'Alex'}
                        },
                        'filter': {
                            'range': {'birthday': {'gte': datetime.date(1960, 1, 1)}}
                        }
                    }
                },
                'size': 2
            },
        )

        self.assertEqual(len(sq.result.hits), 2)
        doc = sq.result.hits[0]
        self.assertIsInstance(doc, self.index.customer)
        self.assertEqual(doc._id, '3')
        self.assertEqual(doc._type, 'customer')
        self.assertEqual(doc._index, 'test')
        self.assertAlmostEqual(doc._score, 2.437682)
        self.assertEqual(doc.name.first, 'Alex')
        self.assertEqual(doc.name.last, 'Exler')
        self.assertEqual(doc.birthday, '1966-10-04')
        self.assertEqual(doc.instance, '3:3')
        doc = sq.result.hits[1]
        self.assertIsInstance(doc, self.index.seller)
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
        self.client.search = MagicMock(
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
            self.index.search_query(search_type='scan', scroll='1m')
            .limit(1000)
        )
        result = sq.result

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
        self.index.query(self.index.car.vendor == 'Focus').delete()
        self.client.delete_by_query.assert_called_with(
            index='test',
            doc_type='car',
            body={
                'query': {
                    'term': {'vendor': 'Focus'}
                }
            },
        )

        self.index.query(self.index.car.vendor == 'Focus') \
                .filter(self.index.car.status == 0) \
                .limit(20) \
                .delete(timeout='1m', replication='async')
        self.client.delete_by_query.assert_called_with(
            index='test',
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
