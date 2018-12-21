from elasticsearch import ElasticsearchException

from .document import DynamicDocument, Document
from .expression import Params
from .result import (
    BulkResult,
    ClearScrollResult,
    CountResult,
    DeleteByQueryResult,
    DeleteResult,
    ExistsResult,
    FlushResult,
    RefreshResult,
    SearchResult,
)
from .search import BaseSearchQuery
from .util import clean_params


class MultiSearchError(ElasticsearchException):
    pass


def get_params(params):
    params.pop('self', None)
    kwargs = params.pop('kwargs') or {}
    doc_cls = params.pop('doc_cls', None) or DynamicDocument
    if params.get('doc_type') is None:
        params['doc_type'] = getattr(doc_cls, '__doc_type__', None)
    return doc_cls, clean_params(params, **kwargs)


def get_result(doc_cls, raw_result):
    return doc_cls(_hit=raw_result)


def multi_get_params(params):
    # TODO: support ids
    # need a way to know document class for id
    params.pop('self', None)
    kwargs = params.pop('kwargs') or {}
    docs = params.pop('docs')
    params = clean_params(params, **kwargs)
    body = {}
    body['docs'] = []
    doc_classes = []
    for doc in docs:
        body['docs'].append(doc.to_meta())
        doc_classes.append(doc.__class__)
    params['body'] = body
    return doc_classes, params


def multi_get_result(doc_classes, raw_result):
    result_docs = []
    for doc_cls, raw_doc in zip(doc_classes, raw_result['docs']):
        if raw_doc['found']:
            result_docs.append(doc_cls(_hit=raw_doc))
        else:
            result_docs.append(None)
    return result_docs


def _prepare_query(cluster, q):
    query_compiler = cluster._get_compiler().compiled_query
    if isinstance(q, BaseSearchQuery):
        query = q.get_context().get_filtered_query(wrap_function_score=False)
    else:
        query = q

    if query is None:
        return None

    query = Params(query=query)
    return query.to_elastic(compiler=query_compiler)


def search_params(params):
    cluster = params.pop('self')
    kwargs = params.pop('kwargs') or {}

    q = params.pop('q')
    query_compiler = cluster._get_compiler().compiled_query
    body = q.to_dict(compiler=query_compiler)

    params = clean_params(params, **kwargs)
    return body, params


def search_result(q, raw_result):
    return SearchResult(
        raw_result, q._aggregations,
        doc_cls=q._get_doc_cls(),
        instance_mapper=q._instance_mapper,
    )


def count_params(params):
    cluster = params.pop('self')
    kwargs = params.pop('kwargs') or {}
    q = params.pop('q')

    body = _prepare_query(cluster, q)
    return body, clean_params(params, **kwargs)


def count_result(raw_result):
    return CountResult(raw_result)


def exists_params(params):
    cluster = params.pop('self')
    kwargs = params.pop('kwargs') or {}
    q = params.pop('q')

    body = _prepare_query(cluster, q)
    return body, clean_params(params, **kwargs)


def exists_result(raw_result):
    return ExistsResult(raw_result)


def scroll_params(params):
    params.pop('self', None)
    kwargs = params.pop('kwargs') or {}
    doc_cls = params.pop('doc_cls', None)
    instance_mapper = params.pop('instance_mapper', None)
    return doc_cls, instance_mapper, clean_params(params, **kwargs)


def scroll_result(doc_cls, instance_mapper, raw_result):
    return SearchResult(
        raw_result,
        doc_cls=doc_cls,
        instance_mapper=instance_mapper,
    )


def clear_scroll_params(params):
    params.pop('self', None)
    kwargs = params.pop('kwargs') or {}
    return clean_params(params, **kwargs)


def clear_scroll_result(raw_result):
    return ClearScrollResult(raw_result)


def multi_search_params(params):
    cluster = params.pop('self')
    kwargs = params.pop('kwargs') or {}
    raise_on_error = params.pop('raise_on_error', None)
    raise_on_error = (
        raise_on_error
        if raise_on_error is not None
        else cluster._multi_search_raise_on_error
    )
    queries = params.pop('queries')

    params = clean_params(params, **kwargs)
    query_compiler = cluster._get_compiler().compiled_query
    body = []
    for q in queries:
        query_header = {}
        if q._index:
            query_header['index'] = q._index._name
        doc_type = q._get_doc_type()
        if doc_type:
            query_header['type'] = doc_type
        query_header.update(q._search_params)
        body += [query_header, q.to_dict(compiler=query_compiler)]
    return body, raise_on_error, params


def multi_search_result(queries, raise_on_error, raw_results):
    errors = []
    for raw, q in zip(raw_results, queries):
        result = SearchResult(
            raw, q._aggregations,
            doc_cls=q._get_doc_cls(),
            instance_mapper=q._instance_mapper,
        )
        q._cached_result = result
        print(q, q._cached_result)
        if result.error:
            errors.append(result.error)

    if raise_on_error and errors:
        if len(errors) == 1:
            error_msg = '1 query was failed'
        else:
            error_msg = '{} queries were failed'.format(len(errors))
        raise MultiSearchError(error_msg, errors)

    return [q.get_result() for q in queries]


def put_mapping_params(params):
    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    doc_cls_or_mapping = params.pop('doc_cls_or_mapping')
    if issubclass(doc_cls_or_mapping, Document):
        body = doc_cls_or_mapping.to_mapping()
    else:
        body = doc_cls_or_mapping
    if params.get('doc_type', None) is None:
        params['doc_type'] = getattr(doc_cls_or_mapping, '__doc_type__', None)
    return body, clean_params(params, **kwargs)


def put_mapping_result(raw_result):
    # TODO Convert to nice result object
    return raw_result


def add_params(params):
    from . import actions

    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    docs = params.pop('docs')

    # TODO: Override an index for action if there is index in params
    return [actions.Index(d) for d in docs], clean_params(params, **kwargs)


def delete_params(params):
    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    doc_or_id = params.pop('doc_or_id')
    doc_cls = params.pop('doc_cls', None)
    doc_type = params.pop('doc_type', None)
    if isinstance(doc_or_id, Document):
        doc_id = doc_or_id._id
        doc_cls = doc_cls or doc_or_id.__class__
    else:
        doc_id = doc_or_id
    assert doc_type or (doc_cls and doc_cls.__doc_type__), \
        'Cannot evaluate doc_type: you must specify doc_type or doc_cls'
    params['doc_type'] = doc_type or doc_cls.__doc_type__
    params['id'] = doc_id
    return clean_params(params, **kwargs)


def delete_result(raw_result):
    return DeleteResult(raw_result)


def delete_by_query_params(params):
    cluster = params.pop('self')
    kwargs = params.pop('kwargs') or {}
    q = params.pop('q')

    params['body'] = _prepare_query(cluster, q)
    return clean_params(params, **kwargs)


def delete_by_query_result(raw_result):
    return DeleteByQueryResult(raw_result)


def bulk_params(params):
    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    actions = params.pop('actions')
    body = []
    for act in actions:
        body.append({act.__action_name__: act.get_meta()})
        source = act.get_source()
        if source is not None:
            body.append(source)
    return clean_params(params, body=body, **kwargs)


def bulk_result(raw_result):
    return BulkResult(raw_result)


def refresh_params(params):
    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    index = params.pop('index')
    return clean_params(params, index=index, **kwargs)


def refresh_result(raw_result):
    return RefreshResult(raw_result)


def flush_params(params):
    params.pop('self')
    kwargs = params.pop('kwargs') or {}
    index = params.pop('index')
    return clean_params(params, index=index, **kwargs)


def flush_result(raw_result):
    return FlushResult(raw_result)
