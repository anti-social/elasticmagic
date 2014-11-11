from collections import defaultdict

from elasticsearch.client import IndicesClient

from .util import to_camel_case
from .search import SearchQuery
from .result import Result
from .document import DynamicDocument
from .expression import Params


class Index(object):
    def __init__(self, client, name):
        self._client = client
        self._indices_client = IndicesClient(client)
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

    def _clean_params(self, params):
        return {p: v for p, v in params.items() if v is not None}

    # Methods that do requests to elasticsearch

    def search(self, q, doc_type, routing=None, doc_cls=None, aggregations=None, instance_mapper=None):
        params = self._clean_params({'routing': routing})
        raw_result = self._client.search(
            index=self._name, doc_type=doc_type, body=q.to_dict(), **params
        )
        return Result(raw_result, aggregations,
                      doc_cls=doc_cls,
                      instance_mapper=instance_mapper)

    def count(self, q, doc_type, routing=None):
        body = {'query': q.to_dict()} if q else None
        params = self._clean_params({'routing': routing})
        return self._client.count(
            index=self._name, doc_type=doc_type, body=body, **params
        )['count']

    def add(self, docs, timeout=None, consistency=None, replication=None):
        actions = []
        for doc in docs:
            if isinstance(doc, dict):
                raw_doc = doc.copy()
                doc_meta = {
                    '_type': raw_doc.pop('_type'),
                    '_id': raw_doc.pop('_id'),
                }
                if '_routing' in raw_doc:
                    doc_meta['_routing'] = raw_doc.pop('_routing')
                if '_parent' in raw_doc:
                    doc_meta['_parent'] = raw_doc.pop('_parent')
            else:
                doc_type = doc.__doc_type__
                doc_meta = {'_type': doc_type, '_id': doc._id}
                if doc._routing:
                    doc_meta['_routing'] = doc._routing
                raw_doc = doc.to_dict()
            actions.extend([
                {'index': doc_meta},
                raw_doc
            ])
        params = self._clean_params({'timeout': timeout,
                                     'consistency': consistency,
                                     'replication': replication})
        self._client.bulk(index=self._name, body=actions, **params)

    def delete(self, q, doc_type, timeout=None, consistency=None, replication=None):
        params = self._clean_params({'timeout': timeout,
                                     'consistency': consistency,
                                     'replication': replication})
        return self._client.delete_by_query(
            index=self._name, doc_type=doc_type, body=Params(query=q).to_dict(), **params
        )

    def refresh(self):
        return self._indices_client.refresh(index=self.name)
        
    def flush(self):
        return self._indices_client.flush(index=self.name)
