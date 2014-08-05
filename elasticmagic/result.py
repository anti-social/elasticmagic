class Result(object):
    def __init__(self, raw_result, doc_cls=None):
        self.raw = raw_result
        self.doc_cls = doc_cls

        self.total = raw_result['hits']['total']
