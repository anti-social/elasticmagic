from __future__ import absolute_import

from abc import ABCMeta

from .compat import with_metaclass
from .document import DynamicDocument
from .util import to_camel_case


class BaseIndex(with_metaclass(ABCMeta)):
    def __init__(self, cluster, name):
        self._cluster = cluster
        self._name = name

        self._doc_cls_cache = {}

    def __getitem__(self, doc_type):
        return self.get_doc_cls(doc_type)

    def get_doc_cls(self, doc_type):
        if doc_type not in self._doc_cls_cache:
            self._doc_cls_cache[doc_type] = type(
                '{}{}'.format(to_camel_case(doc_type), 'Document'),
                (DynamicDocument,),
                {'__doc_type__': doc_type}
            )
        return self._doc_cls_cache[doc_type]

    def get_name(self):
        return self._name

    def get_cluster(self):
        return self._cluster

    def get_settings(self):
        return self._cluster._client.indices.get_settings(index=self._name)

    def search_query(self, *args, **kwargs):
        """Returns a :class:`search.SearchQuery` instance that is bound to this
        index.
        """
        kwargs['index'] = self
        return self._cluster._search_query_cls(*args, **kwargs)

    def query(self, *args, **kwargs):
        return self.search_query(*args, **kwargs)


class Index(BaseIndex):
    # Methods that do requests to elasticsearch

    def get_compiler(self):
        return self._cluster.get_compiler()

    def get(
            self, id, doc_cls=None, doc_type=None, source=None,
            realtime=None, routing=None, preference=None, refresh=None,
            version=None, version_type=None, **kwargs
    ):
        return self._cluster.get(
            self._name, id, doc_cls=doc_cls, doc_type=doc_type, source=source,
            realtime=realtime, routing=routing, preference=preference,
            refresh=refresh, version=version, version_type=version_type,
            **kwargs
        )

    def multi_get(
            self, docs, doc_type=None, source=None, realtime=None,
            routing=None, preference=None, refresh=None, **kwargs
    ):
        return self._cluster.multi_get(
            docs, index=self._name, doc_type=doc_type, source=source,
            realtime=realtime, routing=routing, preference=preference,
            refresh=refresh, **kwargs
        )

    mget = multi_get

    def search(
            self, q, doc_type=None, routing=None, preference=None,
            timeout=None, search_type=None, query_cache=None,
            terminate_after=None, scroll=None, **kwargs
    ):
        return self._cluster.search(
            q, index=self._name, doc_type=doc_type,
            routing=routing, preference=preference, timeout=timeout,
            search_type=search_type, query_cache=query_cache,
            terminate_after=terminate_after, scroll=scroll,
            **kwargs
        )

    def multi_search(
            self, queries, doc_type=None, routing=None, preference=None,
            search_type=None, **kwargs
    ):
        return self._cluster.multi_search(
            queries, index=self._name, doc_type=doc_type,
            routing=routing, preference=preference, search_type=search_type,
            **kwargs
        )

    msearch = multi_search

    def count(
            self, q=None, doc_type=None, routing=None, preference=None,
            **kwargs
    ):
        return self._cluster.count(
            q, index=self._name, doc_type=doc_type, routing=routing,
            preference=preference, **kwargs
        )

    def exists(
            self, q, doc_type=None, refresh=None, routing=None, **kwargs
    ):
        return self._cluster.exists(
            q, index=self._name, doc_type=doc_type, refresh=refresh,
            routing=routing, **kwargs
        )

    def scroll(
            self, scroll_id, scroll, doc_cls=None, instance_mapper=None,
            **kwargs
    ):
        return self._cluster.scroll(
            scroll_id, scroll,
            doc_cls=doc_cls,
            instance_mapper=instance_mapper,
            **kwargs
        )

    def clear_scroll(self, scroll_id, **kwargs):
        return self._cluster.clear_scroll(scroll_id, **kwargs)

    def put_mapping(
            self, doc_cls_or_mapping, doc_type=None, allow_no_indices=None,
            expand_wildcards=None, ignore_conflicts=None,
            ignore_unavailable=None, master_timeout=None, timeout=None,
            **kwargs
    ):
        return self._cluster.put_mapping(
            doc_cls_or_mapping, index=self._name, doc_type=doc_type,
            allow_no_indices=allow_no_indices,
            expand_wildcards=expand_wildcards,
            ignore_conflicts=ignore_conflicts,
            ignore_unavailable=ignore_unavailable,
            master_timeout=master_timeout, timeout=timeout,
            **kwargs
        )

    def add(
            self, docs, doc_type=None, refresh=None, timeout=None,
            consistency=None, replication=None, **kwargs
    ):
        return self._cluster.add(
            docs, index=self._name, doc_type=doc_type, refresh=refresh,
            timeout=timeout, consistency=consistency, replication=replication,
            **kwargs
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

    def delete_by_query(
            self, q, doc_type=None, routing=None,
            conflicts=None, refresh=None, timeout=None,
            scroll=None, scroll_size=None,
            wait_for_completion=None, requests_per_second=None,
            **kwargs
    ):
        return self._cluster.delete_by_query(
            q, index=self._name, doc_type=doc_type, routing=routing,
            conflicts=conflicts, refresh=refresh, timeout=timeout,
            scroll=scroll, scroll_size=scroll_size,
            wait_for_completion=wait_for_completion,
            requests_per_second=requests_per_second,
            **kwargs
        )

    def bulk(self, actions, doc_type=None, refresh=None, **kwargs):
        return self._cluster.bulk(
            actions, index=self._name, doc_type=doc_type, refresh=refresh,
            **kwargs
        )

    def refresh(self, **kwargs):
        return self._cluster.refresh(index=self._name, **kwargs)

    def flush(self, **kwargs):
        return self._cluster.flush(index=self._name, **kwargs)

    def flush_synced(self, **kwargs):
        return self._cluster.flush_synced(index=self._name, **kwargs)
