from elasticmagic import DynamicDocument
from elasticmagic.types import Type, String, Integer, List
from elasticmagic.expression import (
    Params, Term, Terms, Exists, Missing, Match, MatchAll, MultiMatch, Range,
    Bool, Query, BooleanExpression, And, Or, Not, Sort, Field,
    Boosting, Common, ConstantScore, FunctionScore, DisMax, Filtered, Ids, Prefix,
    SpanFirst, SpanMulti, SpanNear, SpanNot, SpanOr, SpanTerm,
)

from .base import BaseTestCase


class ExpressionTestCase(BaseTestCase):
    def test_expression(self):
        f = DynamicDocument.fields

        e = Params({'foo': 'bar'})
        self.assert_expression(
            e,
            {"foo": "bar"}
        )
        self.assertEqual(e['foo'], 'bar')
        self.assertTrue('foo' in e)

        self.assert_expression(
            Match(f.message, 'this is a test'),
            {
                "match": {
                    "message": "this is a test",
                }
            }
        )
        self.assert_expression(
            Match(
                f.message, 'this is a test',
                minimum_should_match='100%',
                cutoff_frequency=0.001,
                boost=2.1
            ),
            {
                "match": {
                    "message": {
                        "query": "this is a test",
                        "minimum_should_match": "100%",
                        "cutoff_frequency": 0.001,
                        "boost": 2.1,
                    }
                }
            }
        )

        self.assert_expression(
            Term(f.user, 'kimchy'),
            {
                "term": {"user": "kimchy"}
            }
        )
        self.assert_expression(
            Term(f.user, 'kimchy', boost=1.2),
            {
                "term": {"user": {"value": "kimchy", "boost": 1.2}}
            }
        )
        self.assert_expression(
            Term('user.login', 'kimchy'),
            {
                "term": {"user.login": "kimchy"}
            }
        )

        self.assert_expression(
            Terms(f.status, [0]),
            {
                "terms": {
                    "status": [0]
                }
            }
        )
        self.assert_expression(
            Terms(f.tags, ['blue', 'pill'], minimum_should_match=1),
            {
                "terms": {
                    "tags": ["blue", "pill"],
                    "minimum_should_match": 1
                }
            }
        )

        self.assert_expression(
            Exists(f.tags),
            {
                "exists": {"field": "tags"}
            }
        )
        self.assert_expression(
            Missing(f.tags, _cache=True),
            {
                "missing": {
                    "field": "tags",
                    "_cache": True
                }
            }
        )

        self.assert_expression(
            Bool(
                must=Term(f.user, 'kimchy'),
                must_not=Range(f.age, from_=10, to=20),
                should=[Term(f.tag, 'wow'), Term(f.tag, 'elasticsearch', boost=2.1)],
                minimum_should_match=1,
                boost=1.0,
            ),
            {
                "bool": {
                    "must": {
                        "term": {"user": "kimchy"}
                    },
                    "must_not": {
                        "range": {
                            "age": {"from": 10, "to": 20}
                        }
                    },
                    "should": [
                        {
                            "term": {"tag": "wow"}
                        },
                        {
                            "term": {"tag": {"value": "elasticsearch", "boost": 2.1}}
                        }
                    ],
                    "minimum_should_match": 1,
                    "boost": 1.0
                }
            }
        )

        e = MultiMatch(
            "Will Smith",
            [self.index.star.title.boost(4), self.index.star.wildcard('*_name').boost(2)],
            minimum_should_match='100%'
        )
        self.assert_expression(
            e,
            {
                "multi_match": {
                    "query": "Will Smith",
                    "fields": ["title^4", "*_name^2"],
                    "minimum_should_match": "100%"
                }
            }
        )
        self.assertEqual(
            e._collect_doc_classes(),
            {self.index.star}
        )

        self.assert_expression(
            Range(self.index.product.price, lte=100, boost=2.2, execution='index', _cache=False),
            {
                "range": {
                    "price": {"lte": 100, "boost": 2.2},
                    "execution": "index",
                    "_cache": False
                }
            }
        )

        self.assert_expression(
            Boosting(
                positive=Term(f.field1, 'value1'),
                negative=Term(f.field2, 'value2'),
                negative_boost=0.2
            ),
            {
                "boosting": {
                    "positive": {
                        "term": {
                            "field1": "value1"
                        }
                    },
                    "negative": {
                        "term": {
                            "field2": "value2"
                        }
                    },
                    "negative_boost": 0.2
                }
            }
        )

        self.assert_expression(
            Common(
                f.body, 'nelly the elephant not as a cartoon',
                cutoff_frequency=0.001,
                minimum_should_match=dict(low_freq=2, high_freq=3),
            ),
            {
                "common": {
                    "body": {
                        "query": "nelly the elephant not as a cartoon",
                        "cutoff_frequency": 0.001,
                        "minimum_should_match": {
                            "low_freq": 2,
                            "high_freq": 3
                        }
                    }
                }
            }
        )

        self.assert_expression(
            ConstantScore(filter=Term(f.user, 'kimchy'), boost=1.2),
            {
                "constant_score": {
                    "filter": {
                        "term": { "user": "kimchy"}
                    },
                    "boost": 1.2
                }
            }
        )
        self.assert_expression(
            FunctionScore(
                query=MatchAll(),
                field_value_factor={
                    'field': f.popularity,
                    'factor': 1.2,
                    'modifier': 'sqrt',
                }
            ),
            {
                "function_score": {
                    "query": {"match_all": {}},
                    "field_value_factor": {
                        "field": "popularity",
                        "factor": 1.2,
                        "modifier": "sqrt"
                    }
                }
            }
        )

        self.assert_expression(
            DisMax([Term(f.age, 34), Term(f.age, 35)], boost=1.2, tie_breaker=0.7),
            {
                "dis_max": {
                    "tie_breaker": 0.7,
                    "boost": 1.2,
                    "queries": [
                        {
                            "term" : { "age" : 34 }
                        },
                        {
                            "term" : { "age" : 35 }
                        }
                    ]
                }
            }
        )

        self.assert_expression(
            Filtered(
                filter=Range(f.created, gte='now - 1d / d'),
                query=Match(f.tweet, 'full text search')
            ),
            {
                "filtered": {
                    "query": {
                        "match": { "tweet": "full text search" }
                    },
                    "filter": {
                        "range": { "created": { "gte": "now - 1d / d" }}
                    }
                }
            }
        )

        self.assert_expression(
            Ids(['123456']),
            {
                "ids": {
                    "values": ["123456"]
                }
            }
        )
        self.assert_expression(
            Ids(['1', '4', '100'], type="my_type"),
            {
                "ids": {
                    "type": "my_type",
                    "values": ["1", "4", "100"]
                }
            }
        )

        self.assert_expression(
            Prefix(f.user, 'ki', boost=2.0),
            {
                "prefix": { "user":  { "value": "ki", "boost": 2.0 } }
            }
        )

        self.assert_expression(
            MatchAll(),
            {"match_all": {}}
        )
        self.assert_expression(
            MatchAll(boost=1.2),
            {
                "match_all": { "boost" : 1.2 }
            }
        )

        self.assert_expression(
            Query(Match(f.title, 'this that thus')),
            {
                "query": {
                    "match": {
                        "title": "this that thus"
                    }
                }
            }
        )
        self.assert_expression(
            Query(Match(f.title, 'this that thus'), _cache=True),
            {
                "fquery": {
                    "query": {
                        "match": {
                            "title": "this that thus"
                        }
                    },
                    "_cache": True
                }
            }
        )

        self.assertRaises(NotImplementedError, BooleanExpression)

        self.assert_expression(
            And(
                Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
                Prefix(f.name.second, 'ba')
            ),
            {
                "and": [
                    {
                        "range": {
                            "post_date": {
                                "from": "2010-03-01",
                                "to": "2010-04-01"
                            }
                        }
                    },
                    {
                        "prefix" : { "name.second" : "ba" }
                    }
                ]
            }
        )
        self.assert_expression(
            And(
                Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
                Prefix(f.name.second, 'ba'),
                _cache=True
            ),
            {
                "and": {
                    "filters": [
                        {
                            "range": {
                                "post_date": {
                                    "from": "2010-03-01",
                                    "to": "2010-04-01"
                                }
                            }
                        },
                        {
                            "prefix" : { "name.second" : "ba" }
                        }
                    ],
                    "_cache": True
                }
            }
        )

        self.assert_expression(
            Or(Term(f.name.second, 'banon'), Term(f.name.nick, 'kimchy')),
            {
                "or": [
                    {
                        "term": {"name.second": "banon"}
                    },
                    {
                        "term": {"name.nick": "kimchy"}
                    }
                ]
            }
        )
        self.assert_expression(
            And(Or(Term(f.name.nick, 'kimchy'))),
            {
                "term": {"name.nick": "kimchy"}
            }
        )

        self.assert_expression(
            Not(
                Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
            ),
            {
                "not": {
                    "range": {
                        "post_date": {
                            "from": "2010-03-01",
                            "to": "2010-04-01"
                        }
                    }
                }
            }
        )
        self.assert_expression(
            Not(
                Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
                _cache=True,
            ),
            {
                "not": {
                    "filter":  {
                        "range": {
                            "post_date": {
                                "from": "2010-03-01",
                                "to": "2010-04-01"
                            }
                        }
                    },
                    "_cache": True
                }
            }
        )

        self.assert_expression(
            Sort(f.post_date),
            "post_date"
        )
        self.assert_expression(
            Sort(f.age, 'desc'),
            {
                "age": "desc"
            }
        )
        self.assert_expression(
            Sort(f.price, 'asc', mode='avg'),
            {
                "price": {
                    "order": "asc",
                    "mode": "avg"
                }
            }
        )
        self.assert_expression(
            Sort(
                f.offer.price.sort, 'asc', mode='avg',
                nested_filter=Term(f.offer.color, 'blue')
            ),
            {
                "offer.price.sort": {
                    "order": "asc",
                    "mode": "avg",
                    "nested_filter": {
                        "term": {"offer.color": "blue"}
                    }
                }
            }
        )

        self.assert_expression(
            SpanFirst(SpanTerm(f.user, 'kimchy'), end=3),
            {
                "span_first": {
                    "match": {
                        "span_term": {"user": "kimchy"}
                    },
                    "end": 3
                }
            }
        )

        self.assert_expression(
            SpanMulti(Prefix(f.user, 'ki', boost=1.08)),
            {
                "span_multi": {
                    "match": {
                        "prefix": {
                            "user":  {"value": "ki", "boost": 1.08}
                        }
                    }
                }
            }
        )

        self.assert_expression(
            SpanNear(
                [SpanTerm(f.field, 'value1'),
                 SpanTerm(f.field, 'value2'),
                 SpanTerm(f.field, 'value3')],
                slop=12,
                in_order=False,
                collect_payloads=False,
            ),
            {
                "span_near": {
                    "clauses": [
                        {"span_term": {"field": "value1"}},
                        {"span_term": {"field": "value2"}},
                        {"span_term": {"field": "value3"}}
                    ],
                    "slop": 12,
                    "in_order": False,
                    "collect_payloads": False
                }
            }
        )
        
        self.assert_expression(
            SpanNot(
                SpanTerm(f.field1, 'hoya'),
                SpanNear([SpanTerm(f.field1, 'la'), SpanTerm(f.field1, 'hoya')], slop=0, in_order=True),
            ),
            {
                "span_not": {
                    "include": {
                        "span_term": {"field1": "hoya"}
                    },
                    "exclude": {
                        "span_near": {
                            "clauses": [
                                {"span_term": {"field1": "la"}},
                                {"span_term": {"field1": "hoya"}}
                            ],
                            "slop": 0,
                            "in_order": True
                        }
                    }
                }
            }
        )

        self.assert_expression(
            SpanOr(
                [SpanTerm(f.field, 'value1'),
                 SpanTerm(f.field, 'value2'),
                 SpanTerm(f.field, 'value3')]
            ),
            {
                "span_or": {
                    "clauses": [
                        {"span_term": {"field": "value1"}},
                        {"span_term": {"field": "value2"}},
                        {"span_term": {"field": "value3"}}
                    ]
                }
            }
        )


    def test_field(self):
        self.assertEqual(Field().get_type().__class__, Type)
        self.assertIs(Field().get_name(), None)

        self.assertRaises(TypeError, Field, [])
        self.assertRaises(TypeError, Field, 'name', 1, 2)
        self.assert_expression(Field('name'), "name")
        
        self.assert_expression(
            Field('status') == 0,
            {
                "term": {"status": 0}
            }
        )
        self.assert_expression(
            Field('presence') == None,
            {
                "missing": {"field": "presence"}
            }
        )
        self.assert_expression(
            Field('status') != 1,
            {
                "bool": {
                    "must_not": [
                        {
                            "term": {"status": 1}
                        }
                    ]
                }
            }
        )
        self.assert_expression(
            Field('presence') != None,
            {
                "exists": {"field": "presence"}
            }
        )
        self.assert_expression(
            Field('name').span_first("iphone", 2),
            {
                "span_first": {
                    "match": {
                        "span_term": {"name": "iphone"}
                    },
                    "end": 2
                }
            }
        )
        self.assert_expression(
            Field('status').in_([0, 2, 3]),
            {
                "terms": {"status": [0, 2, 3]}
            }
        )
        self.assert_expression(
            Field('status').in_(iter([0, 2, 3])),
            {
                "terms": {"status": [0, 2, 3]}
            }
        )
        self.assert_expression(
            Field('price') > 99.9,
            {
                "range": {
                    "price": {"gt": 99.9}
                }
            }
        )
        self.assert_expression(
            Field('price') < 101,
            {
                "range": {
                    "price": {"lt": 101}
                }
            }
        )
        self.assert_expression(
            Field('price') <= 1000,
            {
                "range": {
                    "price": {"lte": 1000}
                }
            }
        )
        self.assert_expression(
            Field('price').range(gte=100, lt=200),
            {
                "range": {
                    "price": {"gte": 100, "lt": 200}
                }
            }
        )
        self.assert_expression(
            Field('name').match('Hello kitty', minimum_should_match=2),
            {
                "match": {
                    "name": {
                        "query": "Hello kitty",
                        "minimum_should_match": 2
                    }
                }
            }
        )
        self.assert_expression(
            Field('price').asc(mode='min'),
            {
                "price": {
                    "order": "asc",
                    "mode": "min"
                }
            }
        )
        self.assert_expression(
            Field('price').desc(),
            {
                "price": "desc"
            }
        )
        self.assert_expression(
            Field('description').boost(0.1),
            "description^0.1"
        )

    def test_field_mapping(self):
        f = Field('name', String)
        self.assertEqual(
            f.to_mapping(),
            {
                "name": {
                    "type": "string"
                }
            }
        )

        f = Field('name', String, fields={'sort': Field(String)})
        self.assertEqual(
            f.to_mapping(),
            {
                "name": {
                    "type": "string",
                    "fields": {
                        "sort": {
                            "type": "string",
                        }
                    }
                }
            }
        )

        f = Field('name', String, fields={'sort': Field('ordering', String)})
        self.assertEqual(
            f.to_mapping(),
            {
                "name": {
                    "type": "string",
                    "fields": {
                        "ordering": {
                            "type": "string",
                        }
                    }
                }
            }
        )
        
        f = Field(
            'name', String,
            fields=[Field('raw', String, index='not_analyzed')]
        )
        self.assertEqual(
            f.to_mapping(),
            {
                "name": {
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                }
            }
        )

        f = Field('status', Integer)
        self.assertEqual(
            f.to_mapping(),
            {
                "status": {
                    "type": "integer"
                }
            }
        )

        f = Field('tag', List(Integer))
        self.assertEqual(
            f.to_mapping(),
            {
                "tag": {
                    "type": "integer"
                }
            }
        )
