from mock import Mock, MagicMock

from elasticmagic import Index, Document, SearchQuery, Params, Term, Bool, Sort, agg
from elasticmagic.types import String, Integer
from elasticmagic.expression import Fields, Field

from .base import BaseTestCase


class SearchQueryTest(BaseTestCase):
    def test(self):
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
                                        "include": "title"
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

    def test_index_search(self):
        class CarObject(object):
            def __init__(self, id):
                self.id = id
                self.name = '{0}:{0}'.format(id)

        def _obj_mapper(ids):
            return {id: CarObject(int(id)) for id in ids}
        obj_mapper = Mock(wraps=_obj_mapper)

        class CarDocument(Document):
            __doc_type__ = 'car'
            vendor = Field(String)
            model = Field(String)
            year = Field(Integer)

            # @staticmethod
            # def _instance_mapper(ids):
            #     return {id: CarObject(int(id)) for id in ids}

            # instance_mapper = Mock(wraps=_obj_mapper)

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
            es_index.search(
                Bool(
                    must=CarDocument.vendor == 'Subaru',
                    should=[CarDocument.model.in_(['Imprezza', 'Forester']),
                            CarDocument.year == 2004]
                )
            )
            .with_instance_mapper(obj_mapper)
        )
        results = sq.results

        es_client.search.assert_called_with(
            index='ads',
            doc_type='car',
            body={
                'query': {
                    'bool': {
                        'must': {
                            'term': {'vendor': 'Subaru'}
                        },
                        'should': [
                            {
                                'terms': {'model': ['Imprezza', 'Forester']}
                            },
                            {
                                'term': {'year': 2004}
                            }
                        ]
                    }
                }
            }
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
