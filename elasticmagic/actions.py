from .util import clean_params
from .index import Index as ESIndex


class Action(object):
    __action_name__ = None

    def __init__(self, doc, index=None, doc_type=None,
                 consistency=None, refresh=None,
                 routing=None, parent=None, timestamp=None, ttl=None,
                 version=None, version_type=None):
        self.doc = doc
        index = index._name if isinstance(index, ESIndex) else index
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
        doc_type = self.doc.__class__.__doc_type__
        doc_id = self.doc._id
        meta = dict(self.meta, **clean_params({'_type': doc_type, '_id': doc_id}))
        return {self.__action_name__: meta}

    def get_source(self):
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
        doc_dict =  self.doc.to_dict()
        return {'doc': doc_dict}
