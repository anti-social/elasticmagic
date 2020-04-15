import pytest

from elasticmagic import DynamicDocument
from elasticmagic import (
    Params, Term, Terms, Exists, Missing, Match, MatchPhrase,
    MatchPhrasePrefix, MatchAll, MultiMatch, Range,
    Bool, Query, And, Or, Not, Sort, Field, Limit,
    Boosting, Common, ConstantScore, FunctionScore, DisMax, Filtered, Ids,
    Prefix, SpanFirst, SpanMulti, SpanNear, SpanNot, SpanOr, SpanTerm,
    Nested, HasParent, HasChild
)
from elasticmagic.compiler import CompilationError
from elasticmagic.expression import BooleanExpression, Script
from elasticmagic.types import (
    Type, String, Integer, List, GeoPoint, Completion, Text
)

from .conftest import assert_expression


f = DynamicDocument.fields


def test_params(compiler):
    e = Params()
    assert dict(e) == {}
    assert_expression(e, {}, compiler)

    e = Params({'foo': 'bar'})
    assert dict(e) == {'foo': 'bar'}
    assert_expression(e, {"foo": "bar"}, compiler)
    assert e['foo'] == 'bar'
    assert 'foo' in e

    e = Params({'foo': None})
    assert dict(e) == {}
    assert_expression(e, {}, compiler)
    assert 'foo' not in e


def test_match(compiler):
    assert_expression(
        Match(f.message, 'this is a test'),
        {
            "match": {
                "message": "this is a test",
            }
        },
        compiler
    )

    assert_expression(
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
        },
        compiler
    )

    expr = Match(f.message, 'this is a test', type='phrase')
    if compiler.features.supports_match_type:
        assert_expression(
            expr,
            {
                "match": {
                    "message": {
                        "query": "this is a test",
                        "type": "phrase",
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "match_phrase": {
                    "message": "this is a test",
                }
            },
            compiler
        )

    expr = Match(f.message, 'this is a te', type='phrase_prefix')
    if compiler.features.supports_match_type:
        assert_expression(
            expr,
            {
                "match": {
                    "message": {
                        "query": "this is a te",
                        "type": "phrase_prefix",
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "match_phrase_prefix": {
                    "message": "this is a te",
                }
            },
            compiler
        )


def test_term(compiler):
    assert_expression(
        Term(f.user, 'kimchy'),
        {
            "term": {"user": "kimchy"}
        },
        compiler
    )
    assert_expression(
        Term(f.user, 'kimchy', boost=1.2),
        {
            "term": {"user": {"value": "kimchy", "boost": 1.2}}
        },
        compiler
    )
    assert_expression(
        Term('user.login', 'kimchy'),
        {
            "term": {"user.login": "kimchy"}
        },
        compiler
    )


def test_terms(compiler):
    assert_expression(
        Terms(f.status, [0]),
        {
            "terms": {
                "status": [0]
            }
        },
        compiler
    )
    assert_expression(
        Terms(f.tags, ['blue', 'pill'], minimum_should_match=1),
        {
            "terms": {
                "tags": ["blue", "pill"],
                "minimum_should_match": 1
            }
        },
        compiler
    )


def test_exists(compiler):
    assert_expression(
        Exists(f.tags),
        {
            "exists": {"field": "tags"}
        },
        compiler
    )


def test_missing(compiler):
    expr = Missing(f.tags, _cache=True)
    if compiler.features.supports_missing_query:
        assert_expression(
            expr,
            {
                'missing': {'_cache': True, 'field': 'tags'}
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                'bool': {
                    'must_not': [
                        {'exists': {'_cache': True, 'field': 'tags'}}]
                }
            },
            compiler
        )


def test_bool(compiler):
    assert_expression(
        Bool(
            must=Term(f.user, 'kimchy'),
            filter=Term(f.tag, 'tech'),
            must_not=Range(f.age, from_=10, to=20),
            should=[
                Term(f.tag, 'wow'),
                Term(f.tag, 'elasticsearch', boost=2.1),
            ],
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
                        "term": {
                            "tag": {"value": "elasticsearch", "boost": 2.1}
                        }
                    }
                ],
                "minimum_should_match": 1,
                "boost": 1.0
            }
        },
        compiler
    )


def test_script(compiler):
    script_inline = (
        "(doc[params.field_name].size() > 0 && "
        "doc[params.field_name].value > 0) ? "
        "params.adv_boost : 0"
    )
    script_params = dict(adv_boost=70, field_name='advert_weight')
    expr = Script(inline=script_inline, lang='painless', params=script_params)
    if compiler.min_es_version < (2,):
        with pytest.raises(CompilationError):
            expr.to_dict(compiler)
    elif compiler.min_es_version < (5, 6):
        assert_expression(
            expr,
            {
                'inline': script_inline,
                'lang': 'painless',
                'params': script_params
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                'source': script_inline,
                'lang': 'painless',
                'params': script_params
            },
            compiler
        )

    expr = Script(id="adv_boost")
    if compiler.min_es_version < (2,):
        with pytest.raises(CompilationError):
            expr.to_dict(compiler)
    elif compiler.min_es_version < (5, 0):
        assert_expression(
            expr,
            {'id': 'adv_boost'},
            compiler
        )
    elif compiler.min_es_version < (5, 6):
        assert_expression(
            expr,
            {'stored': 'adv_boost'},
            compiler
        )
    else:
        assert_expression(
            expr,
            {'id': 'adv_boost'},
            compiler
        )

    expr = Script(file="home/es/test.py")
    if compiler.min_es_version < (2,):
        with pytest.raises(CompilationError):
            expr.to_dict(compiler)
    elif compiler.min_es_version < (6,):
        assert_expression(
            expr,
            {'file': 'home/es/test.py'},
            compiler
        )
    else:
        with pytest.raises(CompilationError):
            expr.to_dict(compiler)


def test_multi_match(index, compiler):
    e = MultiMatch(
        "Will Smith",
        [
            index['star'].title.boost(4),
            index['star'].wildcard('*_name').boost(2)
        ],
        minimum_should_match='100%'
    )
    assert_expression(
        e,
        {
            "multi_match": {
                "query": "Will Smith",
                "fields": ["title^4", "*_name^2"],
                "minimum_should_match": "100%"
            }
        },
        compiler
    )
    assert e._collect_doc_classes() == {index['star']}


def test_range(index, compiler):
    assert_expression(
        Range(
            index['product'].price, lte=100, boost=2.2,
            execution='index', _cache=False
        ),
        {
            "range": {
                "price": {"lte": 100, "boost": 2.2},
                "execution": "index",
                "_cache": False
            }
        },
        compiler
    )


def test_boosting(compiler):
    assert_expression(
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
        },
        compiler
    )


def test_common(compiler):
    assert_expression(
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
        },
        compiler
    )


def test_constant_score(compiler):
    assert_expression(
        ConstantScore(filter=Term(f.user, 'kimchy'), boost=1.2),
        {
            "constant_score": {
                "filter": {
                    "term": {"user": "kimchy"}
                },
                "boost": 1.2
            }
        },
        compiler
    )


def test_function_score(compiler):
    assert_expression(
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
        },
        compiler
    )


def test_dis_max(compiler):
    assert_expression(
        DisMax([Term(f.age, 34), Term(f.age, 35)], boost=1.2, tie_breaker=0.7),
        {
            "dis_max": {
                "tie_breaker": 0.7,
                "boost": 1.2,
                "queries": [
                    {
                        "term": {"age": 34}
                    },
                    {
                        "term": {"age": 35}
                    }
                ]
            }
        },
        compiler
    )


def test_filtered(compiler):
    assert_expression(
        Filtered(
            filter=Range(f.created, gte='now - 1d / d'),
            query=Match(f.tweet, 'full text search')
        ),
        {
            "filtered": {
                "query": {
                    "match": {"tweet": "full text search"}
                },
                "filter": {
                    "range": {"created": {"gte": "now - 1d / d"}}
                }
            }
        },
        compiler
    )


def test_ids(compiler):
    assert_expression(
        Ids(['123456']),
        {
            "ids": {
                "values": ["123456"]
            }
        },
        compiler
    )
    assert_expression(
        Ids(['1', '4', '100'], type="my_type"),
        {
            "ids": {
                "type": "my_type",
                "values": ["1", "4", "100"]
            }
        },
        compiler
    )


def test_prefix(compiler):
    assert_expression(
        Prefix(f.user, 'ki', boost=2.0),
        {
            "prefix": {"user": {"value": "ki", "boost": 2.0}}
        },
        compiler
    )


def test_match_all(compiler):
    assert_expression(
        MatchAll(),
        {"match_all": {}},
        compiler
    )
    assert_expression(
        MatchAll(boost=1.2),
        {
            "match_all": {"boost": 1.2}
        },
        compiler
    )


def test_query(compiler):
    assert_expression(
        Query(Match(f.title, 'this that thus')),
        {
            "query": {
                "match": {
                    "title": "this that thus"
                }
            }
        },
        compiler
    )
    assert_expression(
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
        },
        compiler
    )


def test_boolean_expression(compiler):
    with pytest.raises(NotImplementedError):
        BooleanExpression()

    expr = And(
        Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
        Prefix(f.name.second, 'ba'),
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            expr,
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
                        "prefix": {"name.second": "ba"}
                    }
                ]
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "post_date": {
                                    "from": "2010-03-01",
                                    "to": "2010-04-01"
                                }
                            }
                        },
                        {
                            "prefix": {"name.second": "ba"}
                        }
                    ]
                }
            },
            compiler
        )

    expr = And(
        Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
        Prefix(f.name.second, 'ba'),
        _cache=True
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            expr,
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
                            "prefix": {"name.second": "ba"}
                        }
                    ],
                    "_cache": True
                }
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "post_date": {
                                    "from": "2010-03-01",
                                    "to": "2010-04-01"
                                }
                            }
                        },
                        {
                            "prefix": {"name.second": "ba"}
                        }
                    ]
                }
            },
            compiler
        )

    expr = Or(Term(f.name.second, 'banon'), Term(f.name.nick, 'kimchy'))
    if compiler.min_es_version < (2,):
        assert_expression(
            expr,
            {
                "or": [
                    {
                        "term": {"name.second": "banon"}
                    },
                    {
                        "term": {"name.nick": "kimchy"}
                    }
                ]
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "bool": {
                    "should": [
                        {
                            "term": {"name.second": "banon"}
                        },
                        {
                            "term": {"name.nick": "kimchy"}
                        }
                    ]
                }
            },
            compiler
        )

    assert_expression(
        And(Or(Term(f.name.nick, 'kimchy'))),
        {
            "term": {"name.nick": "kimchy"}
        },
        compiler
    )

    expr = Not(Range(f.post_date, from_='2010-03-01', to='2010-04-01'))
    if compiler.min_es_version < (2,):
        assert_expression(
            expr,
            {
                "not": {
                    "range": {
                        "post_date": {
                            "from": "2010-03-01",
                            "to": "2010-04-01"
                        }
                    }
                }
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "bool": {
                    "must_not": [
                        {
                            "range": {
                                "post_date": {
                                    "from": "2010-03-01",
                                    "to": "2010-04-01"
                                }
                            }
                        }
                    ]
                }
            },
            compiler
        )

    expr = Not(
        Range(f.post_date, from_='2010-03-01', to='2010-04-01'),
        _cache=True,
    )
    if compiler.min_es_version < (2,):
        assert_expression(
            expr,
            {
                "not": {
                    "filter": {
                        "range": {
                            "post_date": {
                                "from": "2010-03-01",
                                "to": "2010-04-01"
                            }
                        }
                    },
                    "_cache": True
                }
            },
            compiler
        )
    else:
        assert_expression(
            expr,
            {
                "bool": {
                    "must_not": [
                        {
                            "range": {
                                "post_date": {
                                    "from": "2010-03-01",
                                    "to": "2010-04-01"
                                }
                            }
                        }
                    ]
                }
            },
            compiler
        )


def test_sort(compiler):
    assert_expression(
        Sort(f.post_date),
        "post_date",
        compiler
    )
    assert_expression(
        Sort(f.age, 'desc'),
        {
            "age": "desc"
        },
        compiler
    )
    assert_expression(
        Sort(f.price, 'asc', mode='avg'),
        {
            "price": {
                "order": "asc",
                "mode": "avg"
            }
        },
        compiler
    )
    assert_expression(
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
        },
        compiler
    )


def test_span_first(compiler):
    assert_expression(
        SpanFirst(SpanTerm(f.user, 'kimchy'), end=3),
        {
            "span_first": {
                "match": {
                    "span_term": {"user": "kimchy"}
                },
                "end": 3
            }
        },
        compiler
    )


def test_span_multi(compiler):
    assert_expression(
        SpanMulti(Prefix(f.user, 'ki', boost=1.08)),
        {
            "span_multi": {
                "match": {
                    "prefix": {
                        "user": {"value": "ki", "boost": 1.08}
                    }
                }
            }
        },
        compiler
    )


def test_span_near(compiler):
    assert_expression(
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
        },
        compiler
    )


def test_span_not(compiler):
    assert_expression(
        SpanNot(
            SpanTerm(f.field1, 'hoya'),
            SpanNear(
                [
                    SpanTerm(f.field1, 'la'),
                    SpanTerm(f.field1, 'hoya')
                ],
                slop=0,
                in_order=True
            ),
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
        },
        compiler
    )


def test_span_or(compiler):
    assert_expression(
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
        },
        compiler
    )


def test_limit(compiler):
    assert_expression(
        Limit(1000),
        {
            "limit": {
                "value": 1000
            }
        },
        compiler
    )


def test_nested(index, compiler):
    expr = Nested(
        index['movie'].stars,
        Match(index['movie'].stars.full_name, 'Will Smith'),
        score_mode='max',
    )
    assert_expression(
        expr,
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
        },
        compiler
    )
    assert expr._collect_doc_classes() == {index['movie']}


def test_has_parent(index, compiler):
    expr = HasParent(
        index['blog'].tag == 'something',
        parent_type=index['blog'],
        score_mode='score',
    )
    assert_expression(
        expr,
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
        },
        compiler
    )
    assert expr._collect_doc_classes() == set()

    expr = HasParent(
        index['blog'].tag == 'something',
        score_mode='score',
    )
    assert_expression(
        expr,
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
        },
        compiler
    )
    assert expr._collect_doc_classes() == set()


def test_has_child(index, compiler):
    expr = HasChild(
        index['blog_tag'].tag == 'something',
        type=index['blog_tag'],
        score_mode='sum',
    )
    assert_expression(
        expr,
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
        },
        compiler
    )
    assert expr._collect_doc_classes() == set()

    expr = HasChild(
        index['blog_tag'].tag == 'something',
        score_mode='sum',
    )
    assert_expression(
        expr,
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
        },
        compiler
    )
    assert expr._collect_doc_classes() == set()


def test_field(compiler):
    assert Field().get_type().__class__ is Type
    assert Field().get_name() is None

    with pytest.raises(TypeError):
        Field([])
    with pytest.raises(TypeError):
        Field('name', 1, 2)

    assert_expression(Field('name'), "name", compiler)

    assert_expression(
        Field('status') == 0,
        {
            "term": {"status": 0}
        },
        compiler
    )
    if compiler.min_es_version < (5,):
        assert_expression(
            Field('presence') == None,
            {
                'missing': {'field': 'presence'}
            },
            compiler
        )
    else:
        assert_expression(
            Field('presence') == None,
            {
                'bool': {'must_not': [{'exists': {'field': 'presence'}}]}
            },
            compiler
        )
    assert_expression(
        Field('status') != 1,
        {
            "bool": {
                "must_not": [
                    {
                        "term": {"status": 1}
                    }
                ]
            }
        },
        compiler
    )
    assert_expression(
        Field('presence') != None,
        {
            "exists": {"field": "presence"}
        },
        compiler
    )
    assert_expression(
        Field('name').span_first("iphone", 2),
        {
            "span_first": {
                "match": {
                    "span_term": {"name": "iphone"}
                },
                "end": 2
            }
        },
        compiler
    )
    assert_expression(
        ~(Field('status') == 0),
        {
            "bool": {
                "must_not": [
                    {
                        "term": {"status": 0}
                    }
                ]
            }
        },
        compiler
    )
    assert_expression(
        Field('name').span_first("iphone", 2),
        {
            "span_first": {
                "match": {
                    "span_term": {"name": "iphone"}
                },
                "end": 2
            }
        },
        compiler
    )
    assert_expression(
        Field('status').in_([0, 2, 3]),
        {
            "terms": {"status": [0, 2, 3]}
        },
        compiler
    )
    assert_expression(
        Field('status').in_(iter([0, 2, 3])),
        {
            "terms": {"status": [0, 2, 3]}
        },
        compiler
    )
    assert_expression(
        Field('status').not_in_([0, 5, 10]),
        {
            "bool": {
                "must_not": [{
                    "terms": {"status": [0, 5, 10]}
                }]
            }
        },
        compiler
    )
    assert_expression(
        Field('status').not_in_(iter([0, 5, 10])),
        {
            "bool": {
                "must_not": [{
                    "terms": {"status": [0, 5, 10]}
                }]
            }
        },
        compiler
    )

    assert_expression(
        Field('price') > 99.9,
        {
            "range": {
                "price": {"gt": 99.9}
            }
        },
        compiler
    )
    assert_expression(
        Field('price') < 101,
        {
            "range": {
                "price": {"lt": 101}
            }
        },
        compiler
    )
    assert_expression(
        Field('price') <= 1000,
        {
            "range": {
                "price": {"lte": 1000}
            }
        },
        compiler
    )
    assert_expression(
        Field('price').range(gte=100, lt=200),
        {
            "range": {
                "price": {"gte": 100, "lt": 200}
            }
        },
        compiler
    )
    assert_expression(
        Field('name').match('Hello kitty', minimum_should_match=2),
        {
            "match": {
                "name": {
                    "query": "Hello kitty",
                    "minimum_should_match": 2
                }
            }
        },
        compiler
    )
    assert_expression(
        Field('price').asc(mode='min'),
        {
            "price": {
                "order": "asc",
                "mode": "min"
            }
        },
        compiler
    )
    assert_expression(
        Field('price').desc(),
        {
            "price": "desc"
        },
        compiler
    )
    assert_expression(
        Field('description').boost(0.1),
        "description^0.1",
        compiler
    )


def test_field_mapping(compiler):
    assert Field('name', String).to_mapping(compiler) == {
        "name": {
            "type": "string"
        }
    }

    field = Field('name', String, fields={'sort': Field(String)})
    assert field.to_mapping(compiler) == {
        "name": {
            "type": "string",
            "fields": {
                "sort": {
                    "type": "string",
                }
            }
        }
    }

    field = Field('name', String, fields={'sort': Field('ordering', String)})
    assert field.to_mapping(compiler) == {
        "name": {
            "type": "string",
            "fields": {
                "ordering": {
                    "type": "string",
                }
            }
        }
    }

    field = Field(
        'name', String,
        fields=[Field('raw', String, index='not_analyzed')]
    )
    assert field.to_mapping(compiler) == {
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

    assert Field('status', Integer).to_mapping(compiler) == {
        "status": {
            "type": "integer"
        }
    }

    assert Field('tag', List(Integer)).to_mapping(compiler) == {
        "tag": {
            "type": "integer"
        }
    }

    assert Field('pin', GeoPoint()).to_mapping(compiler) == {
        "pin": {
            "type": "geo_point"
        }
    }
    assert Field('pin', GeoPoint(), lat_lon=True).to_mapping(compiler) == {
        "pin": {
            "type": "geo_point",
            "lat_lon": True,
        }
    }

    assert Field('suggest', Completion()).to_mapping(compiler) == {
        'suggest': {
            'type': 'completion',
        }
    }
    field = Field('suggest', Completion(), payloads=True)
    assert field.to_mapping(compiler) == {
        'suggest': {
            'type': 'completion',
            'payloads': True,
        }
    }


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
