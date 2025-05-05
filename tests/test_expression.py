from elasticmagic import Document
from elasticmagic import DynamicDocument
from elasticmagic import (
    Params, Term, Terms, Exists, Missing, Match, MatchPhrase,
    MatchPhrasePrefix, MatchAll, MultiMatch, Range,
    Bool, Query, Sort, Field, Limit,
    Boosting, Common, ConstantScore, FunctionScore, DisMax, Ids, Prefix,
    SpanFirst, SpanMulti, SpanNear, SpanNot, SpanOr, SpanTerm, 
    Nested, HasParent, HasChild, SortScript,
)
from elasticmagic.compiler import CompilationError
from elasticmagic.compiler import Compiler_6_0
from elasticmagic.compiler import Compiler_7_0
from elasticmagic.expression import Script
from elasticmagic.types import (
    Boolean, Type, String, Integer, List, GeoPoint, Completion, Text
)

from .base import BaseTestCase


class ExpressionTestCase(BaseTestCase):
    def test_bool_field_expression(self):
        class Doc(Document):
            flag = Field(Boolean)

        self.assert_expression(
            Doc.flag == True,
            {"term": {"flag": True}}
        )

        # TODO: Reduce expression
        # self.assert_expression(
        #     Bool(should=[
        #         Doc.flag == True,
        #         Doc.flag == True,
        #     ]),
        #     {"term": {"flag": True}}
        # )

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
                'bool': {
                    'must_not': [
                        {'exists': {'_cache': True, 'field': 'tags'}}]
                }
            }
        )

        self.assert_expression(
            Bool(
                must=Term(f.user, 'kimchy'),
                filter=Term(f.tag, 'tech'),
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
                    "filter": {
                        "term": {"tag": "tech"}
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

        script_inline = "(doc[params.field_name].size() > 0 && " \
                        "doc[params.field_name].value > 0) ? " \
                        "params.adv_boost : 0"
        script_params = dict(adv_boost=70, field_name='advert_weight')
        self.assert_expression(
            Script(inline=script_inline, lang='painless', params=script_params),
            dict(source=script_inline, lang='painless', params=script_params)
        )
        self.assert_expression(
            Script(inline=script_inline, params=script_params),
            dict(source=script_inline, params=script_params)
        )
        self.assert_expression(
            Script(inline=script_inline),
            dict(source=script_inline)
        )
        self.assert_expression(
            Script(id="ajkshfajsndajvn2143jlan"),
            dict(id="ajkshfajsndajvn2143jlan"),
            compiler=Compiler_6_0
        )
        self.assert_expression(
            Script(id="ajkshfajsndajvn2143jlan"),
            dict(id="ajkshfajsndajvn2143jlan"),
            compiler=Compiler_7_0
        )
        with self.assertRaises(CompilationError):
            Script(params=dict(hello='no')).to_dict(compiler=Compiler_7_0)
        with self.assertRaises(CompilationError):
            Script(params=dict(file='no')).to_dict(compiler=Compiler_6_0)

        e = MultiMatch(
            "Will Smith",
            [self.index['star'].title.boost(4), self.index['star'].wildcard('*_name').boost(2)],
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
            {self.index['star']}
        )

        self.assert_expression(
            Range(self.index['product'].price, lte=100, boost=2.2, execution='index', _cache=False),
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

        self.assert_expression(
            SortScript(script=Script(inline='score')),
            {
                '_script': {
                    'script': {
                        'source': 'score',
                    },
                },
            },
        )
        self.assert_expression(
            SortScript(
                script=Script(inline='score * 2', params={'a': 1}),
                script_type='number'
            ),
            {
                '_script': {
                    'script': {
                        'source': 'score * 2',
                        'params': {
                            'a': 1,
                        },
                    },
                    'type': 'number',
                }
            },
        )
        self.assert_expression(
            SortScript(
                script=Script(
                    inline='score * 2', params={'a': 1}, lang='painless'
                ),
                script_type='number',
                order='asc',
            ),
            {
                '_script': {
                    'script': {
                        'source': 'score * 2',
                        'params': {
                            'a': 1,
                        },
                        'lang': 'painless',
                    },
                    'type': 'number',
                    'order': 'asc',
                }
            },
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
                [
                    SpanTerm(f.field, 'value1'),
                    SpanTerm(f.field, 'value2'),
                    SpanTerm(f.field, 'value3')
                ],
                boost=2,
            ),
            {
                "span_or": {
                    "clauses": [
                        {"span_term": {"field": "value1"}},
                        {"span_term": {"field": "value2"}},
                        {"span_term": {"field": "value3"}}
                    ],
                    "boost": 2
                }
            }
        )

        self.assert_expression(
            Limit(1000),
            {
                "limit": {
                    "value": 1000
                }
            }
        )

        e = Nested(
            self.index['movie'].stars,
            Match(self.index['movie'].stars.full_name, 'Will Smith'),
            score_mode='max',
        )
        self.assert_expression(
            e,
            {
                "nested": {
                    "path": "stars",
                    "query": {
                        "match": {
                            "stars.full_name": "Will Smith"
                        }
                    },
                    "score_mode": "max"
                }
            }
        )
        self.assertEqual(
            e._collect_doc_classes(),
            {self.index['movie']}
        )

        e = HasParent(
            self.index['blog'].tag == 'something',
            parent_type=self.index['blog'],
            score_mode='score',
        )
        self.assert_expression(
            e,
            {
                "has_parent": {
                    "parent_type": "blog",
                    "query": {
                        "term": {
                            "tag": "something"
                        }
                    },
                    "score_mode": "score"
                }
            }
        )
        self.assertEqual(
            e._collect_doc_classes(),
            set()
        )

        e = HasParent(
            self.index['blog'].tag == 'something',
            score_mode='score',
        )
        self.assert_expression(
            e,
            {
                "has_parent": {
                    "parent_type": "blog",
                    "query": {
                        "term": {
                            "tag": "something"
                        }
                    },
                    "score_mode": "score"
                }
            }
        )
        self.assertEqual(
            e._collect_doc_classes(),
            set()
        )

        e = HasChild(
            self.index['blog_tag'].tag == 'something',
            type=self.index['blog_tag'],
            score_mode='sum',
        )
        self.assert_expression(
            e,
            {
                "has_child": {
                    "type": "blog_tag",
                    "query": {
                        "term": {
                            "tag": "something"
                        }
                    },
                    "score_mode": "sum"
                }
            }
        )
        self.assertEqual(
            e._collect_doc_classes(),
            set()
        )

        e = HasChild(
            self.index['blog_tag'].tag == 'something',
            score_mode='sum',
        )
        self.assert_expression(
            e,
            {
                "has_child": {
                    "type": "blog_tag",
                    "query": {
                        "term": {
                            "tag": "something"
                        }
                    },
                    "score_mode": "sum"
                }
            }
        )
        self.assertEqual(
            e._collect_doc_classes(),
            set()
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
                 'bool': {'must_not': [{'exists': {'field': 'presence'}}]}
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
            ~(Field('status') == 0),
            {
                "bool": {
                    "must_not": [
                        {
                            "term": {"status": 0}
                        }
                    ]
                }
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
            Field('status').not_in_([0, 5, 10]),
            {
                "bool": {
                    "must_not": [{
                        "terms": {"status": [0, 5, 10]}
                    }]
                }
            }
        )
        self.assert_expression(
            Field('status').not_in_(iter([0, 5, 10])),
            {
                "bool": {
                    "must_not": [{
                        "terms": {"status": [0, 5, 10]}
                    }]
                }
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
            f.to_mapping(Compiler_7_0),
            {
                "name": {
                    "type": "string"
                }
            }
        )

        f = Field('name', String, fields={'sort': Field(String)})
        self.assertEqual(
            f.to_mapping(Compiler_7_0),
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
            f.to_mapping(Compiler_7_0),
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
            f.to_mapping(Compiler_7_0),
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
            f.to_mapping(Compiler_7_0),
            {
                "status": {
                    "type": "integer"
                }
            }
        )

        f = Field('tag', List(Integer))
        self.assertEqual(
            f.to_mapping(Compiler_7_0),
            {
                "tag": {
                    "type": "integer"
                }
            }
        )

        f = Field('pin', GeoPoint())
        self.assertEqual(
            f.to_mapping(Compiler_7_0),
            {
                "pin": {
                    "type": "geo_point"
                }
            }
        )
        f = Field('pin', GeoPoint(), lat_lon=True)
        self.assertEqual(
            f.to_mapping(Compiler_7_0),
            {
                "pin": {
                    "type": "geo_point",
                    "lat_lon": True,
                }
            }
        )

        f = Field('suggest', Completion())
        self.assertEqual(
            f.to_mapping(Compiler_7_0),
            {
                'suggest': {
                    'type': 'completion',
                }
            }
        )
        f = Field('suggest', Completion(), payloads=True)
        self.assertEqual(
            f.to_mapping(Compiler_7_0),
            {
                'suggest': {
                    'type': 'completion',
                    'payloads': True,
                }
            }
        )


def test_match_phrase(compiler):
    expr = MatchPhrase(
        Field('name', Text()),
        'Ha-ha (c)'
    )
    assert expr.to_elastic(compiler) == {
        'match_phrase': {
            'name': 'Ha-ha (c)'
        }
    }

    expr = MatchPhrase(
        Field('name', Text()),
        'Ha-ha (c)',
        slop=2, boost=10, analyzer='name_text'
    )
    field_expr = Field('name', Text()).match_phrase(
        'Ha-ha (c)', slop=2, boost=10, analyzer='name_text'
    )
    assert expr.to_elastic(compiler) == {
        'match_phrase': {
            'name': {
                'query': 'Ha-ha (c)',
                'slop': 2,
                'boost': 10,
                'analyzer': 'name_text',
            }
        }
    }
    assert expr.to_elastic(compiler) == \
        field_expr.to_elastic(compiler)

    field_expr = Field('name', Text()).match_phrase(
        'Ha-ha (c)',
        slop=2, boost=10, analyzer='name_text'
    )
    assert expr.to_elastic(compiler) == \
        field_expr.to_elastic(compiler)


def test_match_phrase_prefix(compiler):
    expr = MatchPhrasePrefix(
        Field('name', Text()),
        'Hi ther'
    )
    assert expr.to_elastic(compiler) == {
        'match_phrase_prefix': {
            'name': 'Hi ther'
        }
    }

    expr = MatchPhrasePrefix(
        Field('name', Text()),
        'Hi ther',
        slop=2, boost=10, analyzer='name_text', max_expansions=100
    )
    assert expr.to_elastic(compiler) == {
        'match_phrase_prefix': {
            'name': {
                'query': 'Hi ther',
                'slop': 2,
                'boost': 10,
                'analyzer': 'name_text',
                'max_expansions': 100,
            }
        }
    }

    field_expr = Field('name', Text()).match_phrase_prefix(
        'Hi ther', slop=2, boost=10, analyzer='name_text', max_expansions=100
    )
    assert expr.to_elastic(compiler) == \
        field_expr.to_elastic(compiler)


def test_match_with_type():
    expr = Match(Field('name', Text()), 'Test match type', type='phrase')
    assert expr.to_elastic(Compiler_7_0) == {
        'match_phrase': {
            'name': 'Test match type',
        }
    }
