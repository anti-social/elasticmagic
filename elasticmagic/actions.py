from .util import clean_params
from .document import Document, META_FIELD_NAMES


class Action(object):
    __action_name__ = None

    def __init__(self, doc, index=None, doc_type=None,
                 consistency=None, refresh=None,
                 routing=None, parent=None, timestamp=None, ttl=None,
                 version=None, version_type=None, **kwargs):
        from .index import Index as ESIndex
        index = index._name if isinstance(index, ESIndex) else index

        self.doc = doc

        if isinstance(self.doc, Document):
            self.meta = self.doc.to_meta()
            self.source = self.doc.to_source()
        else:
            self.meta = {}
            for field_name in META_FIELD_NAMES:
                value = self.doc.get(field_name)
                if value:
                    self.meta[field_name] = value

            self.source = self.doc.copy()
            for exclude_field in Document.mapping_fields:
                self.source.pop(exclude_field.get_field().get_name(), None)

        self.meta.update(clean_params({
            '_index': index,
            '_type': doc_type,
            '_routing': routing,
            '_parent': parent,
            '_timestamp': timestamp,
            '_ttl': ttl,
            '_version': version,
            '_version_type': version_type,
            'refresh': refresh,
            'consistency': consistency,
        }))
        self.meta.update(clean_params(kwargs))

    def get_meta(self):
        return self.meta

    def get_source(self):
        return self.source


class Index(Action):
    __action_name__ = 'index'


class Delete(Action):
    __action_name__ = 'delete'

    def get_source(self):
        pass


class Create(Action):
    __action_name__ = 'create'


class Update(Action):
    __action_name__ = 'update'

    def __init__(self, doc=None, script=None, script_id=None,
                 index=None, doc_type=None,
                 consistency=None, refresh=None,
                 routing=None, parent=None,
                 timestamp=None, ttl=None,
                 version=None, version_type=None,
                 detect_noop=None, retry_on_conflict=None,
                 upsert=None, doc_as_upsert=None,
                 scripted_upsert=None, params=None,
                 **kwargs):
        super(Update, self).__init__(
            doc or {},
            index=index, doc_type=doc_type,
            consistency=consistency, refresh=refresh,
            routing=routing, parent=parent,
            timestamp=timestamp, ttl=ttl,
            version=version, version_type=version_type,
            _retry_on_conflict=retry_on_conflict,
            **kwargs
        )
        self.source_params = clean_params({
            'detect_noop': detect_noop,
            'upsert': upsert,
            'doc_as_upsert': doc_as_upsert,
            'scripted_upsert': scripted_upsert,
            'params': params,
            'script': script,
            'script_id': script_id,
        })
        if self.source:
            self.source = {'doc': self.source}
        self.source.update(self.source_params)
