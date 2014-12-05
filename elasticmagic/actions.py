from .expressions import Expression


class Action(object):
    def get_header(self):
        raise NotImplementedError()

    def get_source(self):
        raise NotImplementedError()


class Index(Action):
    def __init__(self, doc, index=None):
        self.doc = doc
        self.index = index

    def get_header(self):
        doc_type = self.doc.__class__.__doc_type__
        doc_id = self.doc._id
        return {'index': {'_index': index, '_type': doc_type, '_id': doc_id}}

    def get_source(self):
        return self.doc.to_dict()
