from elasticmagic import Document, Field
from elasticmagic.compiler import Compiler60
from elasticmagic.expression import Ids, ParentId
from elasticmagic.types import Text


class Question(Document):
    __doc_type__ = 'question'
    __parent__ = None

    question = Field(Text)

class Answer(Document):
    __doc_type__ = 'answer'
    __parent__ = Question

    answer = Field(Text)


def test_document_mapping():
    assert \
        Question.to_mapping(compiler=Compiler60) == \
        {
            'properties': {
                '_doc_type': {
                    'type': 'join'
                },
                'question': {
                    'type': 'text'
                }
            }
        }


def test_document_meta_and_source():
    ultimate_question = Question(
        _id=1,
        question='The Ultimate Question'
    )
    assert \
        ultimate_question.to_meta(compiler=Compiler60) == \
        {
            '_id': 'question~1',
            '_type': '_doc'
        }
    assert \
        ultimate_question.to_source(compiler=Compiler60) == \
        {
            '_doc_type': {
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
    assert \
        amazingly_accurate_answer.to_meta(compiler=Compiler60) == \
        {
            '_id': 'answer~1',
            '_type': '_doc',
            '_routing': 1
        }
    assert \
        amazingly_accurate_answer.to_source(compiler=Compiler60) == \
        {
            "_doc_type": {
                "name": "answer",
                "parent": "question~1",
            },
            "answer": "42",
        }


def test_document_from_hit():
    q = Question(_hit={
        '_id': 'question~1',
        '_type': '_doc',
        'fields': {
            '_doc_type': ['question'],
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
            '_doc_type': ['answer'],
            '_doc_type#question': ['question~1'],
        },
        '_source': {
            'answer': '42',
        }
    })
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._parent == '1'
    assert a.answer == '42'


def test_ids_query_no_type():
    q = Ids([1, 2, 3])
    res = q.to_elastic(compiler=Compiler60)
    assert res == {
        'ids': {
            'values': [1, 2, 3]
        }
    }


def test_ids_query():
    q = Ids([1, 2, 3], type=Question)
    res = q.to_elastic(compiler=Compiler60)

    assert res == {
        'ids': {
            'values': ['question~1', 'question~2', 'question~3']
        }
    }


def test_ids_query_with_multiple_types():
    # TODO
    # q = Ids([1, 2, 3], type=[Question, Answer])
    # res = q.to_elastic(compiler=Compiler60)

    # assert res == {
    #     'ids': {
    #         'values': [
    #             'question~1', 'question~2', 'question~3',
    #             'answer~1', 'answer~2', 'answer~3',
    #         ]
    #     }
    # }
    pass


def test_parent_id_query():
    q = ParentId(Answer, 1)
    res = q.to_elastic(compiler=Compiler60)

    assert res == {
        'parent_id': {
            'type': 'answer',
            'id': 'question~1'
        }
    }
