import pytest

from elasticmagic import (
    Document,
    Field,
)
from elasticmagic.agg import (
    TopHits,
    TopHitsResult,
)
from elasticmagic.compiler import (
    CompilationError,
    Compiler_6_0,
    Compiler_7_0,
)
from elasticmagic.expression import (
    Ids,
    ParentId,
)
from elasticmagic.search import SearchQuery
from elasticmagic.types import Integer
from elasticmagic.types import Text


class Question(Document):
    __doc_type__ = 'question'
    __parent__ = None

    question = Field(Text)
    stars = Field(Integer)


class Answer(Document):
    __doc_type__ = 'answer'
    __parent__ = Question

    answer = Field(Text)


class FrenchAnswer(Document):
    __doc_type__ = 'another_answer'
    __parent__ = Question

    answer = Field(Text, analyzer='french')


compilers_no_mapping_types = [Compiler_6_0, Compiler_7_0]

compilers_no_doc_type = [Compiler_7_0]


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_document_mapping_no_mapping_types(compiler):
    assert \
        Question.to_mapping(compiler=compiler) == \
        {
            'properties': {
                '_doc_type': {
                    'type': 'object',
                    'properties': {
                        'name': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                        'parent': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                    }
                },
                '_doc_type_join': {
                    'type': 'join'
                },
                'question': {
                    'type': 'text'
                },
                'stars': {
                    'type': 'integer'
                },
            }
        }


@pytest.mark.parametrize('compiler', [Compiler_6_0])
def test_multiple_document_mappings_6x(compiler):
    compiled_mapping = compiler.compiled_put_mapping([Question, Answer])
    assert compiled_mapping.params == {
        'doc_type': '_doc',
    }
    assert \
        compiled_mapping.body == \
        {
            'properties': {
                '_doc_type': {
                    'type': 'object',
                    'properties': {
                        'name': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                        'parent': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                    }
                },
                '_doc_type_join': {
                    'type': 'join',
                    'relations': {
                        'question': ['answer']
                    }
                },
                'question': {
                    'type': 'text'
                },
                'stars': {
                    'type': 'integer'
                },
                'answer': {
                    'type': 'text'
                }
            }
        }


@pytest.mark.parametrize('compiler', [Compiler_7_0])
def test_multiple_document_mappings_7x(compiler):
    compiled_mapping = compiler.compiled_put_mapping([Question, Answer])
    assert compiled_mapping.params == {}
    assert \
        compiled_mapping.body == \
        {
            'properties': {
                '_doc_type': {
                    'type': 'object',
                    'properties': {
                        'name': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                        'parent': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                    }
                },
                '_doc_type_join': {
                    'type': 'join',
                    'relations': {
                        'question': ['answer']
                    }
                },
                'question': {
                    'type': 'text'
                },
                'stars': {
                    'type': 'integer'
                },
                'answer': {
                    'type': 'text'
                }
            }
        }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_put_multiple_mappings_with_conflicting_fields(compiler):
    with pytest.raises(ValueError):
        compiler.compiled_put_mapping([Question, Answer, FrenchAnswer])


@pytest.mark.parametrize('compiler', [Compiler_6_0])
def test_create_index_with_multiple_mappings_6x(compiler):
    compiled_create_index = compiler.compiled_create_index(
        settings={
            'index': {
                'number_of_replicas': 0,
            }
        },
        mappings=[Question, Answer]
    )
    assert compiled_create_index.body == {
        'settings': {
            'index': {
                'number_of_replicas': 0,
            }
        },
        'mappings': {
            '_doc': {
                'properties': {
                    '_doc_type': {
                        'type': 'object',
                        'properties': {
                            'name': {
                                'type': 'keyword',
                                'index': False,
                                'doc_values': False,
                                'store': True
                            },
                            'parent': {
                                'type': 'keyword',
                                'index': False,
                                'doc_values': False,
                                'store': True
                            },
                        }
                    },
                    '_doc_type_join': {
                        'type': 'join',
                        'relations': {
                            'question': ['answer']
                        }
                    },
                    'question': {
                        'type': 'text'
                    },
                    'stars': {
                        'type': 'integer'
                    },
                    'answer': {
                        'type': 'text'
                    }
                }
            }
        }
    }


@pytest.mark.parametrize('compiler', [Compiler_7_0])
def test_create_index_with_multiple_mappings_7x(compiler):
    compiled_create_index = compiler.compiled_create_index(
        settings={
            'index': {
                'number_of_replicas': 0,
            }
        },
        mappings=[Question, Answer]
    )
    assert compiled_create_index.body == {
        'settings': {
            'index': {
                'number_of_replicas': 0,
            }
        },
        'mappings': {
            'properties': {
                '_doc_type': {
                    'type': 'object',
                    'properties': {
                        'name': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                        'parent': {
                            'type': 'keyword',
                            'index': False,
                            'doc_values': False,
                            'store': True
                        },
                    }
                },
                '_doc_type_join': {
                    'type': 'join',
                    'relations': {
                        'question': ['answer']
                    }
                },
                'question': {
                    'type': 'text'
                },
                'stars': {
                    'type': 'integer'
                },
                'answer': {
                    'type': 'text'
                }
            }
        }
    }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_document_meta_and_source_no_mapping_types(compiler):
    ultimate_question = Question(
        _id=1,
        question='The Ultimate Question'
    )
    if compiler.features.requires_doc_type:
        assert \
            ultimate_question.to_meta(compiler=compiler) == \
            {
                '_id': 'question~1',
                '_type': '_doc'
            }
    else:
        assert \
            ultimate_question.to_meta(compiler=compiler) == \
            {
                '_id': 'question~1',
            }
    assert \
        ultimate_question.to_source(compiler=compiler) == \
        {
            '_doc_type': {
                'name': 'question'
            },
            '_doc_type_join': {
                'name': 'question'
            },
            'question': 'The Ultimate Question'
        }

    amazingly_accurate_answer = Answer(
        _id=1,
        _parent=1,
        _routing=1,
        answer='42'
    )
    if compiler.features.requires_doc_type:
        assert \
            amazingly_accurate_answer.to_meta(compiler=compiler) == \
            {
                '_id': 'answer~1',
                '_type': '_doc',
                'routing': 1
            }
    else:
        assert \
            amazingly_accurate_answer.to_meta(compiler=compiler) == \
            {
                '_id': 'answer~1',
                'routing': 1
            }
    assert \
        amazingly_accurate_answer.to_source(compiler) == \
        {
            '_doc_type': {
                'name': 'answer',
                'parent': 'question~1',
            },
            '_doc_type_join': {
                'name': 'answer',
                'parent': 'question~1',
            },
            'answer': '42',
        }


def test_document_from_hit_no_mapping_types():
    q = Question(_hit={
        '_id': 'question~1',
        '_type': '_doc',
        'fields': {
            '_doc_type.name': ['question'],
        },
        '_source': {
            'question': 'The Ultimate Question',
        }
    })
    assert q._id == '1'
    assert q._type == 'question'
    assert q.question == 'The Ultimate Question'

    a = Answer(_hit={
        '_id': 'answer~1',
        '_type': '_doc',
        'fields': {
            '_doc_type.name': ['answer'],
            '_doc_type.parent': ['question~1'],
        },
        '_source': {
            'answer': '42',
        }
    })
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._parent == '1'
    assert a.answer == '42'


def test_document_from_hit_no_mapping_types_via_join_field():
    q = Question(_hit={
        '_id': 'question~1',
        '_type': '_doc',
        'fields': {
            '_doc_type_join': ['question'],
            '_doc_type_join#question': ['question~1'],
        },
        '_source': {
            'question': 'The Ultimate Question',
        }
    })
    assert q._id == '1'
    assert q._type == 'question'
    assert q.question == 'The Ultimate Question'

    a = Answer(_hit={
        '_id': 'answer~1',
        '_type': '_doc',
        'fields': {
            '_doc_type_join': ['answer'],
            '_doc_type_join#question': ['question~1'],
        },
        '_source': {
            'answer': '42',
        }
    })
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._parent == '1'
    assert a.answer == '42'


def test_document_from_hit_with_mapping_types():
    q = Question(_hit={
        '_id': '1',
        '_type': 'question',
        '_source': {
            'question': 'The Ultimate Question',
        }
    })
    assert q._id == '1'
    assert q._type == 'question'
    assert q.question == 'The Ultimate Question'

    a = Answer(_hit={
        '_id': '1',
        '_type': 'answer',
        '_parent': '1',
        '_source': {
            'answer': '42',
        }
    })
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._parent == '1'
    assert a.answer == '42'


def test_ids_query_no_type(compiler):
    q = Ids([1, 2, 3])
    assert q.to_elastic(compiler) == {
        'ids': {
            'values': [1, 2, 3]
        }
    }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_ids_query_no_mapping_types(compiler):
    q = Ids([1, 2, 3], type=Question)
    assert q.to_elastic(compiler=compiler) == {
        'ids': {
            'values': ['question~1', 'question~2', 'question~3']
        }
    }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_parent_id_query_no_mapping_types(compiler):
    q = ParentId(Answer, 1)
    res = q.to_elastic(compiler=compiler)

    assert res == {
        'parent_id': {
            'type': 'answer',
            'id': 'question~1'
        }
    }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_top_hits_agg(compiler):
    agg = TopHits()
    compiled_agg = compiler.compiled_expression(
        agg, doc_classes=[Question, Answer]
    )
    assert compiled_agg.body == {
        'top_hits': {
            'docvalue_fields': [
                '_doc_type_join', '_doc_type_join#*'
            ]
        }
    }


def test_top_hits_agg_result():
    agg = TopHits()
    raw_agg_result = {
        'hits': {
            'hits': [
                {
                    '_type': '_doc',
                    '_id': 'question~1',
                    'fields': {
                        '_doc_type.name': ['question']
                    }
                },
                {
                    '_type': '_doc',
                    '_id': 'answer~1',
                    'fields': {
                        '_doc_type.name': ['answer'],
                        '_doc_type.parent': ['question~1']
                    }
                }
            ]
        }
    }
    agg_res = TopHitsResult(
        agg, raw_agg_result,
        doc_cls_map={'question': Question, 'answer': Answer},
        mapper_registry={},
        instance_mapper=None
    )
    assert len(agg_res.hits) == 2
    q = agg_res.hits[0]
    assert isinstance(q, Question)
    assert q._id == '1'
    assert q._type == 'question'
    assert q._parent is None
    a = agg_res.hits[1]
    assert isinstance(a, Answer)
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._parent == '1'


def test_top_hits_agg_result_via_join_field():
    agg = TopHits()
    raw_agg_result = {
        'hits': {
            'hits': [
                {
                    '_type': '_doc',
                    '_id': 'question~1',
                    'fields': {
                        '_doc_type_join': ['question'],
                        '_doc_type_join#question': ['question~1']
                    }
                },
                {
                    '_type': '_doc',
                    '_id': 'answer~1',
                    'fields': {
                        '_doc_type_join': ['answer'],
                        '_doc_type_join#question': ['question~1']
                    }
                }
            ]
        }
    }
    agg_res = TopHitsResult(
        agg, raw_agg_result,
        doc_cls_map={'question': Question, 'answer': Answer},
        mapper_registry={},
        instance_mapper=None
    )
    assert len(agg_res.hits) == 2
    q = agg_res.hits[0]
    assert isinstance(q, Question)
    assert q._id == '1'
    assert q._type == 'question'
    assert q._parent is None
    a = agg_res.hits[1]
    assert isinstance(a, Answer)
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._parent == '1'


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_search_query_filter_by_ids_no_mapping_types(compiler):
    sq = (
        SearchQuery(doc_cls=[Question])
        .filter(Ids([1, 2, 3]))
    )
    compiled_query = compiler.compiled_query(sq)
    assert compiled_query.body == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'ids': {
                            'values': [
                                'question~1', 'question~2', 'question~3'
                            ]
                        }
                    },
                    {
                        'terms': {
                            '_doc_type_join': ['question']
                        }
                    },
                ]
            }
        },
        'docvalue_fields': ['_doc_type_join', '_doc_type_join#*']
    }
    if compiler.features.requires_doc_type:
        assert compiled_query.params == {
            'doc_type': '_doc'
        }
    else:
        assert compiled_query.params == {}

    sq = (
        SearchQuery(doc_cls=[Question, Answer])
        .filter(Ids([1, 2, 3]))
    )
    compiled_query = compiler.compiled_query(sq)
    assert compiled_query.body == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'ids': {
                            'values': [
                                'question~1', 'question~2', 'question~3',
                                'answer~1', 'answer~2', 'answer~3'
                            ]
                        }
                    },
                    {
                        'terms': {
                            '_doc_type_join': ['question', 'answer']
                        }
                    }
                ]
            }
        },
        'docvalue_fields': [
            '_doc_type_join', '_doc_type_join#*'
        ]
    }
    if compiler.features.requires_doc_type:
        assert compiled_query.params == {
            'doc_type': '_doc'
        }
    else:
        assert compiled_query.params == {}


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_search_query_with_doc_value_fields(compiler):
    sq = (
        SearchQuery(doc_cls=[Question])
        .docvalue_fields(Question.stars)
    )
    compiled_query = compiler.compiled_query(sq)
    assert compiled_query.body == {
        'query': {
            'bool': {
                'filter': {
                    'terms': {
                        '_doc_type_join': ['question']
                    }
                }
            }
        },
        'docvalue_fields': ['stars', '_doc_type_join', '_doc_type_join#*']
    }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_process_search_result(compiler):
    sq = SearchQuery(doc_cls=[Question, Answer])
    compiled_query = compiler.compiled_query(sq)
    search_result = compiled_query.process_result(
        {
            'hits': {
                'hits': [
                    {
                        '_id': 'question~1',
                        '_type': '_doc',
                        'fields': {
                            '_doc_type.name': ['question']
                        }
                    },
                    {
                        '_id': 'answer~1',
                        '_type': '_doc',
                        'fields': {
                            '_doc_type.name': ['answer'],
                            '_doc_type.parent': ['question~1']
                        }
                    },
                ]
            }
        }
    )
    assert len(search_result.hits) == 2
    q1 = search_result.hits[0]
    assert isinstance(q1, Question)
    assert q1._id == '1'
    assert q1._type == 'question'
    assert q1._parent is None
    a1 = search_result.hits[1]
    assert isinstance(a1, Answer)
    assert a1._id == '1'
    assert a1._type == 'answer'
    assert a1._parent == '1'


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_get_no_mapping_types(compiler):
    compiled_get = compiler.compiled_get(1, doc_cls=Question)
    if compiler.features.requires_doc_type:
        assert compiled_get.params == {
            'doc_type': '_doc',
            'id': 'question~1',
            'stored_fields': '_source,_doc_type.name,_doc_type.parent'
        }
    else:
        assert compiled_get.params == {
            'id': 'question~1',
            'stored_fields': '_source,_doc_type.name,_doc_type.parent'
        }

    compiled_get = compiler.compiled_get(
        {'id': 1}, doc_cls=Question
    )
    if compiler.features.requires_doc_type:
        assert compiled_get.params == {
            'doc_type': '_doc',
            'id': 'question~1',
            'stored_fields': '_source,_doc_type.name,_doc_type.parent'
        }
    else:
        assert compiled_get.params == {
            'id': 'question~1',
            'stored_fields': '_source,_doc_type.name,_doc_type.parent'
        }

    compiled_get = compiler.compiled_get(Question(_id=1))
    if compiler.features.requires_doc_type:
        assert compiled_get.params == {
            'doc_type': '_doc',
            'id': 'question~1',
            'stored_fields': '_source,_doc_type.name,_doc_type.parent'
        }
    else:
        assert compiled_get.params == {
            'id': 'question~1',
            'stored_fields': '_source,_doc_type.name,_doc_type.parent'
        }


@pytest.mark.parametrize('compiler', compilers_no_mapping_types)
def test_multi_get_no_mapping_types(compiler):
    compiled_multi_get = compiler.compiled_multi_get(
        [Question(_id=1), Answer(_id=1)]
    )
    if compiler.features.requires_doc_type:
        assert compiled_multi_get.body == {
            'docs': [
                {
                    '_type': '_doc',
                    '_id': 'question~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                },
                {
                    '_type': '_doc',
                    '_id': 'answer~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                }
            ]
        }
    else:
        assert compiled_multi_get.body == {
            'docs': [
                {
                    '_id': 'question~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                },
                {
                    '_id': 'answer~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                }
            ]
        }

    compiled_multi_get = compiler.compiled_multi_get(
        [{'_id': 1, 'doc_cls': Question}, {'_id': 1, 'doc_cls': Answer}]
    )
    if compiler.features.requires_doc_type:
        assert compiled_multi_get.body == {
            'docs': [
                {
                    '_type': '_doc',
                    '_id': 'question~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                },
                {
                    '_type': '_doc',
                    '_id': 'answer~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                }
            ]
        }
    else:
        assert compiled_multi_get.body == {
            'docs': [
                {
                    '_id': 'question~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                },
                {
                    '_id': 'answer~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                }
            ]
        }

    compiled_multi_get = compiler.compiled_multi_get(
        [1, 2], doc_cls=Answer
    )
    if compiler.features.requires_doc_type:
        assert compiled_multi_get.body == {
            'docs': [
                {
                    '_type': '_doc',
                    '_id': 'answer~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                },
                {
                    '_type': '_doc',
                    '_id': 'answer~2',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                }
            ]
        }
    else:
        assert compiled_multi_get.body == {
            'docs': [
                {
                    '_id': 'answer~1',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                },
                {
                    '_id': 'answer~2',
                    'stored_fields': ['_source', '_doc_type.name', '_doc_type.parent']
                }
            ]
        }
