from .util import clean_params
from .document import SPECIAL_FIELD_TYPES


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
            if '_index' in self.doc:
                doc_meta['_index'] = self.doc['_index']
            if '_type' in self.doc:
                doc_meta['_type'] = self.doc['_type']
            if '_id' in self.doc:
                doc_meta['_id'] = self.doc.pop('_id')
            if '_routing' in self.doc:
                doc_meta['_routing'] = self.doc['_routing']
            if '_parent' in self.doc:
                doc_meta['_parent'] = self.doc['_parent']
        else:
            doc_meta = {}
            if self.doc._index:
                doc_meta['_index'] = self.doc._index
            if self.doc._type:
                doc_meta['_type'] = self.doc._type
            if self.doc._id:
                doc_meta['_id'] = self.doc._id
            if self.doc._routing:
                doc_meta['_routing'] = self.doc._routing
            if self.doc._parent:
                doc_meta['_parent'] = self.doc._parent

            if '_type' not in doc_meta:
                doc_meta['_type'] = self.doc.__doc_type__

        meta = dict(self.meta, **clean_params(doc_meta))
        return {self.__action_name__: meta}

    def get_source(self):
        if isinstance(self.doc, dict):
            raw_doc = self.doc.copy()
            for special_field in SPECIAL_FIELD_TYPES.keys():
                raw_doc.pop(special_field, None)
            return raw_doc
        else:
            return self.doc.to_dict()


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
