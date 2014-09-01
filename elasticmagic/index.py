from collections import defaultdict

from .util import to_camel_case
from .search import SearchQuery
from .document import DynamicDocument


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

    def search(self, *args, **kwargs):
        kwargs['index'] = self
        return SearchQuery(*args, **kwargs)

    def add(self, docs):
        actions = []
        for doc in docs:
            doc_type = doc.__doc_type__
            actions.extend([
                {'index': {'_type': doc_type, '_id': doc._id}},
                doc.to_dict()
            ])
        self._client.bulk(index=self._name, body=actions)

    def flush(self):
        # TODO: flush modified documents
        pass
