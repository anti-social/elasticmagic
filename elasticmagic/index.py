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

    # Methods that do requests to elasticsearch

    def search(self, q, doc_type, doc_cls=None, aggregations=None, instance_mapper=None):
        raw_result = self._client.search(
            index=self._name, doc_type=doc_type, body=q.to_dict()
        )
        return Result(raw_result, aggregations,
                      doc_cls=doc_cls,
                      instance_mapper=instance_mapper)

    def add(self, docs):
        actions = []
        for doc in docs:
            doc_type = doc.__doc_type__
            doc_meta = {'_type': doc_type, '_id': doc._id}
            if doc._routing:
                doc_meta['_routing'] = doc._routing
            actions.extend([
                {'index': doc_meta},
                doc.to_dict()
            ])
        self._client.bulk(index=self._name, body=actions)

    def delete(self, q, doc_type):
        return self._client.delete_by_query(
            index=self._name, doc_type=doc_type, body=Params(query=q).to_dict()
        )

    def refresh(self):
        return self._indices_client.refresh(index=self.name)
        
    def flush(self):
        return self._indices_client.flush(index=self.name)
