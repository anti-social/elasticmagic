import pytest

from elasticmagic import Document, Field
from elasticmagic.compiler import Compiler_1_0
from elasticmagic.compiler import Compiler_2_0
from elasticmagic.compiler import Compiler_5_0
from elasticmagic.compiler import Compiler_6_0
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
    params=[Compiler_1_0, Compiler_2_0, Compiler_5_0, Compiler_6_0]
)
def compiler(request):
    return request.param


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
