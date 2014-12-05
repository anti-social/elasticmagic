from collections import defaultdict

from .util import to_camel_case, clean_params
from .search import SearchQuery
from .result import Result
from .helpers import multi_search as _multi_search, bulk as _bulk
from .document import DynamicDocument
from .expression import Params


class Index(object):
    def __init__(self, client, name):
        self._client = client
        self._name = name

        self._doc_cls_cache = {}

    def __getattr__(self, name):
        return self.get_doc_cls(name)

    def get_doc_cls(self, name):
        if name not in self._doc_cls_cache:
            self._doc_cls_cache[name] = type(
                '{}{}'.format(to_camel_case(name), 'Document'),
                (DynamicDocument,),
                {'__doc_type__': name}
            )
        return self._doc_cls_cache[name]

    def query(self, *args, **kwargs):
        kwargs['index'] = self
        return SearchQuery(*args, **kwargs)

    # Methods that do requests to elasticsearch

    def search(self, q, doc_type, doc_cls=None, aggregations=None, instance_mapper=None,
               routing=None, search_type=None):
        params = clean_params({'routing': routing, 'search_type': search_type})
        raw_result = self._client.search(
            index=self._name, doc_type=doc_type, body=q.to_dict(), **params
        )
        return Result(raw_result, aggregations,
                      doc_cls=doc_cls,
                      instance_mapper=instance_mapper)

    def multi_search(self, *queries, **params):
        params['index'] = self._name
        return _multi_search(self._client, queries, params)

    msearch = multi_search

    def count(self, q, doc_type, routing=None):
        body = {'query': q.to_dict()} if q else None
        params = clean_params({'routing': routing})
        return self._client.count(
            index=self._name, doc_type=doc_type, body=body, **params
        )['count']

    def exists(self, q, doc_type, refresh=None, routing=None):
        body = {'query': q.to_dict()} if q else None
        params = clean_params({'refresh': refresh, 'routing': routing})
        return self._client.exists(
            index=self._name, doc_type=doc_type, body=body, **params
        )['exists']

    def add(self, docs, timeout=None, consistency=None, replication=None):
        actions = []
        for doc in docs:
            if isinstance(doc, dict):
                raw_doc = doc.copy()
                doc_meta = {
                    '_type': raw_doc.pop('_type'),
                }
                if '_id' in raw_doc:
                    doc_meta['_id'] = raw_doc.pop('_id')
                if '_routing' in raw_doc:
                    doc_meta['_routing'] = raw_doc.pop('_routing')
                if '_parent' in raw_doc:
                    doc_meta['_parent'] = raw_doc.pop('_parent')
            else:
                doc_type = doc.__doc_type__
                doc_meta = {'_type': doc_type}
                if doc._id:
                    doc_meta['_id'] = doc._id
                if doc._routing:
                    doc_meta['_routing'] = doc._routing
                if doc._parent:
                    doc_meta['_parent'] = doc._parent
                raw_doc = doc.to_dict()
            actions.extend([
                {'index': doc_meta},
                raw_doc
            ])
        params = clean_params({'timeout': timeout,
                               'consistency': consistency,
                               'replication': replication})
        self._client.bulk(index=self._name, body=actions, **params)

    def delete(self, id, doc_type,
               timeout=None, consistency=None, replication=None,
               parent=None, routing=None, refresh=None, version=None, version_type=None):
        params = clean_params({'timeout': timeout,
                               'consistency': consistency,
                               'replication': replication,
                               'parent': parent,
                               'routing': routing,
                               'refresh': refresh,
                               'version': version,
                               'version_type': version_type})
        return self._client.delete(
            index=self._name, doc_type=doc_type, id=id, **params
        )

    def delete_by_query(self, q, doc_type, timeout=None, consistency=None, replication=None, routing=None):
        params = clean_params({'timeout': timeout,
                               'consistency': consistency,
                               'replication': replication,
                               'routing': routing})
        return self._client.delete_by_query(
            index=self._name, doc_type=doc_type, body=Params(query=q).to_dict(), **params
        )

    def bulk(self, *actions, **params):
        params['index'] = self._name
        return _bulk(self._client, queries, params)
        
        
    def refresh(self):
        return self._client.indices.refresh(index=self._name)
        
    def flush(self):
        return self._client.indices.flush(index=self._name)
