import pytest

from elasticmagic import Document, Field
from elasticmagic.agg import TopHits
from elasticmagic.agg import TopHitsResult
from elasticmagic.compiler import Compiler_1_0
from elasticmagic.compiler import Compiler_2_0
from elasticmagic.compiler import Compiler_5_0
from elasticmagic.compiler import Compiler_6_0
from elasticmagic.expression import Ids, ParentId
from elasticmagic.search import SearchQuery
from elasticmagic.types import Text


class Question(Document):
    __doc_type__ = 'question'
    __parent__ = None

    question = Field(Text)


class Answer(Document):
    __doc_type__ = 'answer'
    __parent__ = Question

    answer = Field(Text)


class FrenchAnswer(Document):
    __doc_type__ = 'another_answer'
    __parent__ = Question

    answer = Field(Text, analyzer='french')


@pytest.fixture(
    params=[Compiler_1_0, Compiler_2_0, Compiler_5_0]
)
def compiler_with_mapping_types(request):
    return request.param


@pytest.fixture(
    params=[Compiler_6_0]
)
def compiler_no_mapping_types(request):
    return request.param


def test_document_mapping_with_mapping_types(
        compiler_with_mapping_types
):
    assert \
        Question.to_mapping(compiler=compiler_with_mapping_types) == \
        {
            'question': {
                'properties': {
                    'question': {
                        'type': 'text'
                    }
                }
            }
        }


def test_document_mapping_no_mapping_types(
        compiler_no_mapping_types
):
    assert \
        Question.to_mapping(compiler=compiler_no_mapping_types) == \
        {
            'properties': {
                '_doc_type': {
                    'type': 'join'
                },
                '_doc_type_name': {
                    'type': 'keyword',
                    'index': False,
                    'doc_values': False,
                    'store': True
                },
                '_doc_type_parent': {
                    'type': 'keyword',
                    'index': False,
                    'doc_values': False,
                    'store': True
                },
                'question': {
                    'type': 'text'
                }
            }
        }


def test_multiple_document_mappings_with_mapping_types(
        compiler_with_mapping_types
):
    compiled_mapping = compiler_with_mapping_types.compiled_put_mapping(
        [Question, Answer]
    )
    assert compiled_mapping.params == {
        'doc_type': None,
    }
    assert \
        compiled_mapping.body == \
        {
            'question': {
                'properties': {
                    'question': {
                        'type': 'text'
                    }
                }
            },
            'answer': {
                'properties': {
                    'answer': {
                        'type': 'text'
                    }
                }
            }
        }


def test_multiple_document_mappings_no_mapping_types(
        compiler_no_mapping_types
):
    compiled_mapping = compiler_no_mapping_types.compiled_put_mapping(
        [Question, Answer]
    )
    assert compiled_mapping.params == {
        'doc_type': '_doc',
    }
    assert \
        compiled_mapping.body == \
        {
            'properties': {
                '_doc_type': {
                    'type': 'join',
                    'relations': {
                        'question': ['answer']
                    }
                },
                '_doc_type_name': {
                    'type': 'keyword',
                    'index': False,
                    'doc_values': False,
                    'store': True
                },
                '_doc_type_parent': {
                    'type': 'keyword',
                    'index': False,
                    'doc_values': False,
                    'store': True
                },
                'question': {
                    'type': 'text'
                },
                'answer': {
                    'type': 'text'
                }
            }
        }


def test_put_multiple_mappings_with_conflicting_fields(
        compiler_no_mapping_types
):
    with pytest.raises(ValueError):
        compiler_no_mapping_types.compiled_put_mapping(
            [Question, Answer, FrenchAnswer]
        )


def test_document_meta_and_source_no_mapping_types(
        compiler_no_mapping_types
):
    ultimate_question = Question(
        _id=1,
        question='The Ultimate Question'
    )
    assert \
        ultimate_question.to_meta(compiler_no_mapping_types) == \
        {
            '_id': 'question~1',
            '_type': '_doc'
        }
    assert \
        ultimate_question.to_source(compiler_no_mapping_types) == \
        {
            '_doc_type': {
                'name': 'question'
            },
            '_doc_type_name': 'question',
            'question': 'The Ultimate Question'
        }

    amazingly_accurate_answer = Answer(
        _id=1,
        _parent=1,
        _routing=1,
        answer='42'
    )
    assert \
        amazingly_accurate_answer.to_meta(compiler=compiler_no_mapping_types) == \
        {
            '_id': 'answer~1',
            '_type': '_doc',
            '_routing': 1
        }
    assert \
        amazingly_accurate_answer.to_source(compiler_no_mapping_types) == \
        {
            '_doc_type': {
                'name': 'answer',
                'parent': 'question~1',
            },
            '_doc_type_name': 'answer',
            '_doc_type_parent': 'question~1',
            'answer': '42',
        }


def test_document_meta_and_source_with_mapping_types(
        compiler_with_mapping_types
):
    ultimate_question = Question(
        _id=1,
        question='The Ultimate Question'
    )
    assert \
        ultimate_question.to_meta(compiler_with_mapping_types) == \
        {
            '_id': 1,
            '_type': 'question'
        }
    assert \
        ultimate_question.to_source(compiler_with_mapping_types) == \
        {
            'question': 'The Ultimate Question'
        }

    amazingly_accurate_answer = Answer(
        _id=1,
        _parent=1,
        _routing=1,
        answer='42'
    )
    assert \
        amazingly_accurate_answer.to_meta(compiler=compiler_with_mapping_types) == \
        {
            '_id': 1,
            '_type': 'answer',
            '_routing': 1,
            '_parent': 1
        }
    assert \
        amazingly_accurate_answer.to_source(compiler_with_mapping_types) == \
        {
            "answer": "42"
        }


def test_document_from_hit_no_mapping_types():
    q = Question(_hit={
        '_id': 'question~1',
        '_type': '_doc',
        'fields': {
            '_doc_type_name': ['question'],
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
            '_doc_type_name': ['answer'],
            '_doc_type_parent': ['question~1'],
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


def test_ids_query_no_mapping_types(compiler_no_mapping_types):
    q = Ids([1, 2, 3], type=Question)
    assert q.to_elastic(compiler_no_mapping_types) == {
        'ids': {
            'values': ['question~1', 'question~2', 'question~3']
        }
    }


def test_ids_query_with_mapping_types(compiler_with_mapping_types):
    q = Ids([1, 2, 3], type=Question)
    assert q.to_elastic(compiler_with_mapping_types) == {
        'ids': {
            'type': 'question',
            'values': [1, 2, 3]
        }
    }


def test_parent_id_query_no_mapping_types(compiler_no_mapping_types):
    q = ParentId(Answer, 1)
    res = q.to_elastic(compiler_no_mapping_types)

    assert res == {
        'parent_id': {
            'type': 'answer',
            'id': 'question~1'
        }
    }


def test_parent_id_query_with_mapping_types():
    q = ParentId(Answer, 1)
    res = q.to_elastic(Compiler_5_0)

    assert res == {
        'parent_id': {
            'type': 'answer',
            'id': 1
        }
    }


def test_top_hits_agg(
        compiler_no_mapping_types
):
    agg = TopHits()
    compiled_agg = compiler_no_mapping_types.compiled_expression(
        agg, doc_classes=[Question, Answer]
    )
    assert compiled_agg.body == {
        'top_hits': {
            'stored_fields': [
                '_source', '_doc_type_name', '_doc_type_parent'
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
                        '_doc_type_name': ['question']
                    }
                },
                {
                    '_type': '_doc',
                    '_id': 'answer~1',
                    'fields': {
                        '_doc_type_name': ['answer'],
                        '_doc_type_parent': ['question~1']
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


def test_search_query_filter_by_ids_no_mapping_types(
        compiler_no_mapping_types
):
    sq = (
        SearchQuery(doc_cls=[Question])
        .filter(Ids([1, 2, 3]))
    )
    compiled_query = compiler_no_mapping_types.compiled_query(sq)
    assert compiled_query.body == {
        'query': {
            'bool': {
                'filter': {
                    'ids': {
                        'values': [
                            'question~1', 'question~2', 'question~3'
                        ]
                    }
                }
            }
        },
        'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
    }
    assert compiled_query.params == {
        'doc_type': '_doc'
    }

    sq = (
        SearchQuery(doc_cls=[Question, Answer])
        .filter(Ids([1, 2, 3]))
    )
    compiled_query = compiler_no_mapping_types.compiled_query(sq)
    assert compiled_query.body == {
        'query': {
            'bool': {
                'filter': {
                    'ids': {
                        'values': [
                            'question~1', 'question~2', 'question~3',
                            'answer~1', 'answer~2', 'answer~3'
                        ]
                    }
                }
            }
        },
        'stored_fields': [
            '_source', '_doc_type_name', '_doc_type_parent'
        ]
    }
    assert compiled_query.params == {
        'doc_type': '_doc'
    }


def test_search_query_filter_by_ids_with_mapping_types():
    sq = (
        SearchQuery(doc_cls=[Question, Answer])
        .filter(Ids([1, 2, 3]))
    )
    compiled_query = Compiler_5_0.compiled_query(sq)
    assert compiled_query.body == {
        'query': {
            'bool': {
                'filter': {
                    'ids': {
                        'values': [1, 2, 3]
                    }
                }
            }
        }
    }
    assert compiled_query.params == {
        'doc_type': 'answer,question'
    }


def test_process_search_result(
        compiler_no_mapping_types
):
    sq = SearchQuery(doc_cls=[Question, Answer])
    compiled_query = compiler_no_mapping_types.compiled_query(sq)
    search_result = compiled_query.process_result(
        {
            'hits': {
                'hits': [
                    {
                        '_id': 'question~1',
                        '_type': '_doc',
                        'fields': {
                            '_doc_type_name': ['question']
                        }
                    },
                    {
                        '_id': 'answer~1',
                        '_type': '_doc',
                        'fields': {
                            '_doc_type_name': ['answer'],
                            '_doc_type_parent': ['question~1']
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


def test_get_no_mapping_types(compiler_no_mapping_types):
    compiled_get = compiler_no_mapping_types.compiled_get(1, doc_cls=Question)
    assert compiled_get.params == {
        'doc_type': '_doc',
        'id': 'question~1',
        'stored_fields': '_source,_doc_type_name,_doc_type_parent'
    }
    compiled_get = compiler_no_mapping_types.compiled_get(
        {'id': 1}, doc_cls=Question
    )
    assert compiled_get.params == {
        'doc_type': '_doc',
        'id': 'question~1',
        'stored_fields': '_source,_doc_type_name,_doc_type_parent'
    }
    compiled_get = compiler_no_mapping_types.compiled_get(Question(_id=1))
    assert compiled_get.params == {
        'doc_type': '_doc',
        'id': 'question~1',
        'stored_fields': '_source,_doc_type_name,_doc_type_parent'
    }


def test_multi_get_no_mapping_types(compiler_no_mapping_types):
    compiled_multi_get = compiler_no_mapping_types.compiled_multi_get(
        [Question(_id=1), Answer(_id=1)]
    )
    assert compiled_multi_get.body == {
        'docs': [
            {
                '_type': '_doc',
                '_id': 'question~1',
                'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
            },
            {
                '_type': '_doc',
                '_id': 'answer~1',
                'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
            }
        ]
    }
    compiled_multi_get = compiler_no_mapping_types.compiled_multi_get(
        [{'_id': 1, 'doc_cls': Question}, {'_id': 1, 'doc_cls': Answer}]
    )
    assert compiled_multi_get.body == {
        'docs': [
            {
                '_type': '_doc',
                '_id': 'question~1',
                'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
            },
            {
                '_type': '_doc',
                '_id': 'answer~1',
                'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
            }
        ]
    }
    compiled_multi_get = compiler_no_mapping_types.compiled_multi_get(
        [1, 2], doc_cls=Answer
    )
    assert compiled_multi_get.body == {
        'docs': [
            {
                '_type': '_doc',
                '_id': 'answer~1',
                'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
            },
            {
                '_type': '_doc',
                '_id': 'answer~2',
                'stored_fields': ['_source', '_doc_type_name', '_doc_type_parent']
            }
        ]
    }
