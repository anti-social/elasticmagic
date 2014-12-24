from .util import clean_params
from .document import Document, META_FIELD_NAMES


class Action(object):
    __action_name__ = None

    def __init__(self, doc, index=None, doc_type=None,
                 consistency=None, refresh=None,
                 routing=None, parent=None, timestamp=None, ttl=None,
                 version=None, version_type=None):
        from .index import Index as ESIndex
        index = index._name if isinstance(index, ESIndex) else index

        self.doc = doc
        self.meta = clean_params({
            '_index': index,
            '_type': doc_type,
            '_routing': routing,
            '_parent': parent,
            '_timestamp': timestamp,
            '_ttl': ttl,
            '_version': version,
            '_version_type': version_type,
        })

    def get_meta(self):
        if isinstance(self.doc, dict):
            doc_meta = {}
            for field_name in META_FIELD_NAMES:
                value = self.doc.get(field_name)
                if value:
                    doc_meta[field_name] = value
        else:
            doc_meta = self.doc.to_meta()

        meta = dict(self.meta, **clean_params(doc_meta))
        return {self.__action_name__: meta}

    def get_source(self):
        if isinstance(self.doc, dict):
            raw_doc = self.doc.copy()
            for exclude_field in Document.mapping_fields:
                raw_doc.pop(exclude_field.get_field().get_name(), None)
            return raw_doc
        else:
            return self.doc.to_source()


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

    def __init__(self, doc=None, script=None, script_id=None, retry_on_conflict=None, **kwargs):
        super(Update, self).__init__(doc, **kwargs)
        self.script = script
        self.script_id = script_id
        self.retry_on_conflict = retry_on_conflict

    def get_meta(self):
        meta = super(Update, self).get_meta()
        if self.retry_on_conflict is not None:
            meta[self.__action_name__]['_retry_on_conflict'] = self.retry_on_conflict
        return meta

    def get_source(self):
        source =  super(Update, self).get_source()
        return {'doc': source}
