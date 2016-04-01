import collections

from elasticsearch import ElasticsearchException

from .compiler import DefaultCompiler
from .util import clean_params
from .index import Index
from .search import SearchQuery
from .result import (
    BulkResult, CountResult, DeleteByQueryResult, DeleteResult, ExistsResult,
    FlushResult, RefreshResult, SearchResult,
)
from .document import Document, DynamicDocument
from .expression import Params


MAX_RESULT_WINDOW = 10000


class MultiSearchError(ElasticsearchException):
    pass


class Cluster(object):
    def __init__(
            self, client,
            multi_search_raise_on_error=True, compiler=None,
            index_cls=None
    ):
        self._client = client
        self._multi_search_raise_on_error = multi_search_raise_on_error
        self._compiler = compiler or DefaultCompiler()
        self._index_cls = index_cls or Index

        self._index_cache = {}

    def __getitem__(self, index_name):
        return self.get_index(index_name)

    def get_index(self, name):
        if isinstance(name, tuple):
            name = ','.join(name)

        if name not in self._index_cache:
            self._index_cache[name] = self._index_cls(self, name)
        return self._index_cache[name]

    def search_query(self, *args, **kwargs):
        kwargs['cluster'] = self
        kwargs.setdefault('_compiler', self._compiler.get_query_compiler())
        return SearchQuery(*args, **kwargs)

    query = search_query

    def get(self, index, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, parent=None, preference=None,
            refresh=None, version=None, version_type=None, **kwargs):
        doc_cls = doc_cls or DynamicDocument
        doc_type = doc_type or getattr(doc_cls, '__doc_type__', None)
        params = clean_params({
            'doc_type': doc_type,
            '_source': source,
            'parent': parent,
            'routing': routing,
            'preference': preference,
            'realtime': realtime,
            'refresh': refresh,
            'version': version,
            'version_type': version_type,
        }, **kwargs)
        raw_doc = self._client.get(index=index, id=id, **params)
        return doc_cls(_hit=raw_doc)

    # TODO: support ids
    # need way to know document class for id
    def multi_get(self, docs, index=None, doc_type=None, source=None,
                  parent=None, routing=None, preference=None, realtime=None,
                  refresh=None, **kwargs):
        params = clean_params({
            'index': index,
            'doc_type': doc_type,
            '_source': source,
            'parent': parent,
            'routing': routing,
            'preference': preference,
            'realtime': realtime,
            'refresh': refresh,
        }, **kwargs)
        body = {}
        body['docs'] = []
        doc_classes = []
        for doc in docs:
            body['docs'].append(doc.to_meta())
            doc_classes.append(doc.__class__)
        raw_result = self._client.mget(body=body, **params)
        result_docs = []
        for doc_cls, raw_doc in zip(doc_classes, raw_result['docs']):
            if raw_doc['found']:
                result_docs.append(doc_cls(_hit=raw_doc))
            else:
                result_docs.append(None)
        return result_docs

    mget = multi_get

    def search(
            self, q, index=None, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        params = clean_params(
            dict({
                'index': index, 'doc_type': doc_type, 
                'routing': routing, 'preference': preference,
                'timeout': timeout, 'search_type': search_type,
                'query_cache': query_cache, 'terminate_after': terminate_after,
                'scroll': scroll,
            }, **kwargs)
        )
        raw_result = self._client.search(body=q.to_dict(), **params)
        return SearchResult(
            raw_result, q._aggregations,
            doc_cls=q._get_doc_cls(), instance_mapper=q._instance_mapper,
        )

    def count(self, q, index=None, doc_type=None, routing=None, preference=None, **kwargs):
        body = {'query': q.to_dict()} if q else None
        params = clean_params({
            'index': index,
            'doc_type': doc_type,
            'routing': routing, 
            'preference': preference,
        }, **kwargs)
        return CountResult(
            self._client.count(body=body, **params)
        )

    def exists(self, q, index=None, doc_type=None, refresh=None, routing=None, **kwargs):
        body = {'query': q.to_dict()} if q else None
        params = clean_params({
            'index': index, 
            'doc_type': doc_type,
            'refresh': refresh,
            'routing': routing,
        }, **kwargs)
        return ExistsResult(
            self._client.search_exists(body=body, **params)
        )

    def scroll(self, scroll_id, scroll, doc_cls=None, instance_mapper=None, **kwargs):
        return SearchResult(
            self._client.scroll(scroll_id=scroll_id, scroll=scroll, **clean_params(kwargs)),
            doc_cls=doc_cls,
            instance_mapper=instance_mapper,
        )
    
    def multi_search(self, queries, index=None, doc_type=None, 
                     routing=None, preference=None, search_type=None,
                     raise_on_error=None, **kwargs):
        params = clean_params({
            'index': index,
            'doc_type': doc_type,
            'routing': routing,
            'preference': preference,
            'search_type': search_type
        }, **kwargs)
        body = []
        for q in queries:
            query_header = {}
            if q._index:
                query_header['index'] = q._index._name
            doc_type = q._get_doc_type()
            if doc_type:
                query_header['type'] = doc_type
            query_header.update(q._search_params)
            body += [query_header, q.to_dict()]

        raw_results = self._client.msearch(body=body, **params)['responses']
        errors = []
        for raw, q in zip(raw_results, queries):
            result = SearchResult(
                raw, q._aggregations,
                doc_cls=q._get_doc_cls(),
                instance_mapper=q._instance_mapper
            )
            q.__dict__['result'] = result
            if result.error:
                errors.append(result.error)

        raise_on_error = (
            raise_on_error
            if raise_on_error is not None
            else self._multi_search_raise_on_error
        )
        if raise_on_error and errors:
            if len(errors) == 1:
                error_msg = '1 query was failed'
            else:
                error_msg = '{} queries were failed'.format(len(errors))
            raise MultiSearchError(error_msg, errors)

        return [q.result for q in queries]

    msearch = multi_search

    def put_mapping(self, doc_cls_or_mapping, index, doc_type=None, allow_no_indices=None,
                    expand_wildcards=None, ignore_conflicts=None, ignore_unavailable=None,
                    master_timeout=None, timeout=None, **kwargs):
        if issubclass(doc_cls_or_mapping, Document):
            mapping = doc_cls_or_mapping.to_mapping()
        else:
            mapping = doc_cls_or_mapping
        doc_type = doc_type or doc_cls_or_mapping.__doc_type__
        params = clean_params({
            'allow_no_indices': allow_no_indices,
            'expand_wildcards': expand_wildcards,
            'ignore_conflicts': ignore_conflicts,
            'ignore_unavailable': ignore_unavailable,
            'master_timeout': master_timeout,
            'timeout': timeout,
        }, **kwargs)
        return self._client.indices.put_mapping(
            doc_type=doc_type, index=index, body=mapping, **params
        )

    def delete(self, doc, index, doc_type=None,
               timeout=None, consistency=None, replication=None,
               parent=None, routing=None, refresh=None, version=None,
               version_type=None, **kwargs):
        doc_type = doc_type or doc.__doc_type__
        params = clean_params({
            'timeout': timeout,
            'consistency': consistency,
            'replication': replication,
            'parent': parent,
            'routing': routing,
            'refresh': refresh,
            'version': version,
            'version_type': version_type,
        }, **kwargs)
        return DeleteResult(
            self._client.delete(id=doc._id, index=index, doc_type=doc_type, **params)
        )

    def delete_by_query(self, q, index=None, doc_type=None,
                        timeout=None, consistency=None, replication=None,
                        routing=None, **kwargs):
        params = clean_params({
            'index': index,
            'doc_type': doc_type,
            'timeout': timeout,
            'consistency': consistency,
            'replication': replication,
            'routing': routing,
        }, **kwargs)
        return DeleteByQueryResult(
            self._client.delete_by_query(body=Params(query=q).to_dict(), **params)
        )

    def bulk(self, actions, index=None, doc_type=None, refresh=None, 
             timeout=None, consistency=None, replication=None, **kwargs):
        params = clean_params({
            'index': index,
            'doc_type': doc_type,
            'refresh': refresh,
            'timeout': timeout,
            'consistency': consistency,
            'replication': replication,
        }, **kwargs)
        body = []
        for act in actions:
            body.append({act.__action_name__: act.get_meta()})
            source = act.get_source()
            if source is not None:
                body.append(source)
        return BulkResult(self._client.bulk(body=body, **params))

    def refresh(self, index=None, **kwargs):
        params = clean_params({'index': index}, **kwargs)
        return RefreshResult(self._client.indices.refresh(**params))

    def flush(self, index=None, **kwargs):
        params = clean_params({'index': index}, **kwargs)
        return FlushResult(self._client.indices.flush(**params))
