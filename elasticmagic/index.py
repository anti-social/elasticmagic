from __future__ import absolute_import
from collections import defaultdict

from .util import to_camel_case
from .search import SearchQuery
from .result import Result
from .document import DynamicDocument
from .expression import Params


class Index(object):
    def __init__(self, cluster, name):
        self._cluster = cluster
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

    def get_name(self):
        return self._name

    def get_cluster(self):
        return self._cluster

    def get_settings(self):
        return self._cluster._client.indices.get_settings(index=self._name)

    def search_query(self, *args, **kwargs):
        kwargs['index'] = self
        kwargs.setdefault('_compiler', self._cluster._compiler.get_query_compiler())
        return SearchQuery(*args, **kwargs)

    query = search_query

    # Methods that do requests to elasticsearch

    def get(self, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, preference=None, refresh=None,
            version=None, version_type=None, **kwargs):
        return self._cluster.get(
            self._name, id, doc_cls=doc_cls, doc_type=doc_type, source=source,
            realtime=realtime, routing=routing, preference=preference, refresh=refresh,
            version=version, version_type=version_type,
            **kwargs
        )

    def multi_get(self, docs, doc_type=None, source=None,
            realtime=None, routing=None, preference=None, refresh=None):
        return self._cluster.multi_get(
            docs, index=self._name, doc_type=doc_type, source=source,
            realtime=realtime, routing=routing, preference=preference, refresh=refresh,
        )

    mget = multi_get

    def search(
            self, q, doc_type=None, routing=None, preference=None, timeout=None,
            search_type=None, query_cache=None, terminate_after=None, scroll=None,
            **kwargs
    ):
        return self._cluster.search(
            q, index=self._name, doc_type=doc_type, 
            routing=routing, preference=preference, timeout=timeout,
            search_type=search_type, query_cache=query_cache,
            terminate_after=terminate_after, scroll=scroll,
            **kwargs
        )

    def multi_search(self, queries, doc_type=None, routing=None, preference=None,
                     search_type=None, **kwargs):
        return self._cluster.multi_search(
            queries, index=self._name, doc_type=doc_type,
            routing=routing, preference=preference, search_type=search_type,
            **kwargs
        )

    msearch = multi_search

    def count(self, q, doc_type=None, routing=None, preference=None, **kwargs):
        return self._cluster.count(
            q, index=self._name, doc_type=doc_type, routing=routing,
            preference=preference, **kwargs
        )

    def exists(self, q, doc_type=None, refresh=None, routing=None, **kwargs):
        return self._cluster.exists(
            q, index=self._name, doc_type=doc_type, refresh=refresh,
            routing=routing, **kwargs
        )

    def scroll(self, scroll_id, scroll, doc_cls=None, instance_mapper=None, **kwargs):
        return self._cluster.scroll(
            scroll_id, scroll, doc_cls=doc_cls, instance_mapper=instance_mapper,
            **kwargs
        )

    def put_mapping(self, doc_cls_or_mapping, doc_type=None, allow_no_indices=None,
                    expand_wildcards=None, ignore_conflicts=None, ignore_unavailable=None,
                    master_timeout=None, timeout=None, **kwargs):
        return self._cluster.put_mapping(
            doc_cls_or_mapping, index=self._name, doc_type=doc_type,
            allow_no_indices=allow_no_indices, expand_wildcards=expand_wildcards,
            ignore_conflicts=ignore_conflicts, ignore_unavailable=ignore_unavailable,
            master_timeout=master_timeout, timeout=timeout,
            **kwargs
        )

    def add(self, docs, doc_type=None, timeout=None, consistency=None, replication=None, **kwargs):
        from . import actions

        acts = []
        for doc in docs:
            acts.append(actions.Index(doc))
        return self._cluster.bulk(
            acts, index=self._name, doc_type=doc_type,
            timeout=timeout, consistency=consistency, replication=replication
        )

    def delete(
            self, doc_or_id, doc_cls=None, doc_type=None,
            timeout=None, consistency=None, replication=None,
            parent=None, routing=None, refresh=None, version=None,
            version_type=None,
            **kwargs
    ):
        return self._cluster.delete(
            doc_or_id, index=self._name, doc_cls=doc_cls, doc_type=doc_type, 
            timeout=timeout, consistency=consistency, replication=replication,
            parent=parent, routing=routing, refresh=refresh,
            version=version, version_type=version_type,
            **kwargs
        )

    def delete_by_query(self, q, doc_type=None, timeout=None, consistency=None,
                        replication=None, routing=None, **kwargs):
        return self._cluster.delete_by_query(
            q, index=self._name, doc_type=doc_type,
            timeout=timeout, consistency=consistency,
            replication=replication, routing=routing,
            **kwargs
        )

    def bulk(self, actions, doc_type=None, refresh=None, **kwargs):
        return self._cluster.bulk(
            actions, index=self._name, doc_type=doc_type, refresh=refresh, **kwargs
        )
        
    def refresh(self, **kwargs):
        return self._cluster.refresh(index=self._name, **kwargs)
        
    def flush(self, **kwargs):
        return self._cluster.flush(index=self._name, **kwargs)
