import pytest

from elasticmagic import (
    agg,
    Document,
    Field,
    Ids,
    HasParent, HasChild)
from elasticmagic.expression import ParentId
from elasticmagic.types import (
    Float,
    Text,
)


class Question(Document):
    __doc_type__ = 'question'
    __parent__ = None

    title = Field(Text)
    text = Field(Text)
    rank = Field(Float, store=True)


class Answer(Document):
    __doc_type__ = 'answer'
    __parent__ = Question

    text = Field(Text)


@pytest.fixture
async def es_index(es_cluster, es_client, index_name):
    await es_client.indices.create(
        index=index_name,
        body={'index': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }}
    )
    es_index = es_cluster[index_name]
    await es_index.put_mapping([Question, Answer])
    yield es_index
    await es_client.indices.delete(index=index_name)


@pytest.fixture
async def es_index_empty(es_cluster, es_client, index_name):
    await es_client.indices.create(
        index=index_name,
        body={'index': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }}
    )
    es_index = es_cluster[index_name]
    yield es_index
    await es_client.indices.delete(index=index_name)


@pytest.fixture
async def docs(es_index):
    docs = [
        Question(
            _id=1,
            title='The Ultimate Question',
            text='The Ultimate Answer to Life, The Universe, and Everything',
            rank=4.2,
        ),
        Answer(
            _id=1,
            _parent=1,
            _routing=1,
            text='42'
        )
    ]
    res = await es_index.add(docs, refresh=True)
    if res.errors:
        error_msg = ''
        for item in res.items:
            if item.error:
                error_msg += str(item.error.caused_by) + '\n'
        raise ValueError(
            'Errors when indexing fixtures:\n{}'.format(error_msg)
        )
    yield docs


def check_question_meta(q):
    assert isinstance(q, Question)
    assert q._id == '1'
    assert q._type == 'question'
    assert q._routing is None
    assert q._parent is None
    fields = q.get_hit_fields()
    assert fields['_doc_type_name'] == ['question']


def check_standard_question_doc(q):
    check_question_meta(q)
    assert q.title == 'The Ultimate Question'
    assert q.text == \
        'The Ultimate Answer to Life, The Universe, and Everything'
    assert q.rank == 4.2


def check_answer_meta(a):
    assert isinstance(a, Answer)
    assert a._id == '1'
    assert a._type == 'answer'
    assert a._routing == '1'
    assert a._parent == '1'
    fields = a.get_hit_fields()
    assert fields['_doc_type_name'] == ['answer']
    assert fields['_doc_type_parent'] == ['question~1']


def check_standard_answer_doc(a):
    check_answer_meta(a)
    assert a.text == '42'


@pytest.mark.asyncio
async def test_update_mapping(es_client, index_name, es_index_empty):
    await es_index_empty.put_mapping(Answer)
    assert await es_client.indices.get_mapping(index_name) == {
        index_name: {
            'mappings': {
                '_doc': {
                    'properties': {
                        '_doc_type': {
                            'type': 'join',
                            'relations': {},
                            'eager_global_ordinals': True
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
                        'text': {
                            'type': 'text'
                        }
                    }
                }
            }
        }
    }

    await es_index_empty.put_mapping([Question, Answer])
    assert await es_client.indices.get_mapping(index_name) == {
        index_name: {
            'mappings': {
                '_doc': {
                    'properties': {
                        '_doc_type': {
                            'type': 'join',
                            'relations': {
                                'question': 'answer'
                            },
                            'eager_global_ordinals': True
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
                        'title': {
                            'type': 'text'
                        },
                        'text': {
                            'type': 'text'
                        },
                        'rank': {
                            'type': 'float',
                            'store': True
                        }
                    }
                }
            }
        }
    }


@pytest.mark.asyncio
async def test_get_document(es_index, docs):
    q = await es_index.get(Question(_id=1))
    check_standard_question_doc(q)

    a = await es_index.get(Answer(_id=1, _routing=1))
    check_standard_answer_doc(a)


@pytest.mark.asyncio
async def test_get_document_with_stored_fields(es_index, docs):
    q = await es_index.get(Question(_id=1), stored_fields='rank')
    check_standard_question_doc(q)
    assert q.get_hit_fields()['rank'] == [4.2]


@pytest.mark.asyncio
async def test_multi_get(es_index, docs):
    docs = await es_index.multi_get(
        [Question(_id=1), Answer(_id=1, _routing=1)]
    )
    check_standard_question_doc(docs[0])
    check_standard_answer_doc(docs[1])


@pytest.mark.asyncio
async def test_search(es_index, docs):
    sq = es_index.search_query(doc_cls=[Answer, Question])
    res = await sq.get_result()

    assert res.total == 2
    assert len(res.hits) == 2
    assert res.error is None

    check_standard_question_doc(res.hits[0])

    check_standard_answer_doc(res.hits[1])

    cached_res = await sq.get_result()
    assert cached_res is res


@pytest.mark.asyncio
async def test_search_no_source(es_index, docs):
    sq = es_index.search_query(doc_cls=[Answer, Question]).source(False)
    res = await sq.get_result()

    assert res.total == 2
    assert len(res.hits) == 2
    assert res.error is None

    d = res.hits[0]
    check_question_meta(d)
    assert d.title is None
    assert d.text is None
    assert d.rank is None

    d = res.hits[1]
    check_answer_meta(d)
    assert d.text is None

    cached_res = await sq.get_result()
    assert cached_res is res


@pytest.mark.asyncio
async def test_search_partial_source_and_stored_fields(es_index, docs):
    sq = (
        es_index.search_query(doc_cls=[Answer, Question])
        .stored_fields(Question.rank)
        .source(include=[Question.title])
    )
    res = await sq.get_result()

    assert res.total == 2
    assert len(res.hits) == 2
    assert res.error is None

    d = res.hits[0]
    assert isinstance(d, Question)
    assert d._id == '1'
    assert d._type == 'question'
    assert d._routing is None
    assert d._parent is None
    assert d.title == 'The Ultimate Question'
    assert d.text is None
    assert d.rank is None
    assert d.get_hit_fields()['rank'] == [4.2]

    d = res.hits[1]
    assert isinstance(d, Answer)
    assert d._id == '1'
    assert d._type == 'answer'
    assert d._routing == '1'
    assert d._parent == '1'
    assert d.text is None

    cached_res = await sq.get_result()
    assert cached_res is res


@pytest.mark.asyncio
async def test_top_hits_agg(es_index, docs):
    sq = (
        es_index.search_query()
        .aggs({
            'ranks': agg.Histogram(
                Question.rank,
                interval=1.0,
                min_doc_count=1,
                aggs={
                    'hits': agg.TopHits()
                }
            )
        })
    )
    res = await sq.get_result()

    ranks_agg = res.get_aggregation('ranks')
    b = ranks_agg.buckets[0]
    assert b.key == 4.0
    assert b.doc_count == 1
    hits = b.get_aggregation('hits').hits
    assert len(hits) == 1

    check_standard_question_doc(hits[0])


@pytest.mark.asyncio
async def test_ids_query(es_index, docs):
    sq = (
        es_index.search_query(doc_cls=[Question, Answer])
        .filter(Ids([1]))
    )
    res = await sq.get_result()

    check_standard_question_doc(res.hits[0])
    check_standard_answer_doc(res.hits[1])


@pytest.mark.asyncio
async def test_parent_id_query(es_index, docs):
    sq = es_index.search_query(ParentId(Answer, 1))
    res = await sq.get_result()

    check_standard_answer_doc(res.hits[0])


@pytest.mark.asyncio
async def test_has_parent_query(es_index, docs):
    sq = (
        es_index.search_query(
            HasParent(
                Question.title.match('ultimate')
            ),
            # TODO: document class can be detected automatically
            doc_cls=Answer
        )
    )
    res = await sq.get_result()

    check_standard_answer_doc(res.hits[0])


@pytest.mark.asyncio
async def test_has_child_query(es_index, docs):
    sq = (
        es_index.search_query(
            HasChild(
                Answer.text.match('42')
            ),
            # TODO: document class can be detected automatically
            doc_cls=Question
        )
    )
    res = await sq.get_result()

    check_standard_question_doc(res.hits[0])
