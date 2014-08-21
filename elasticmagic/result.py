from .document import Document


class Result(object):
    def __init__(self, raw_result, doc_cls=None):
        self.raw = raw_result
        self.doc_cls = doc_cls or Document

        self.total = raw_result['hits']['total']
        self.hits = []
        for hit in raw_result['hits']['hits']:
            self.hits.append(self.doc_cls(_hit=hit))

    def __iter__(self):
        return iter(self.hits)
