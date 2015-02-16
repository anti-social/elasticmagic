from collections import namedtuple


def tuple_doc_processor(*fields):
    doc_type_fields = {}
    for f in fields:
        doc_cls = f._collect_doc_classes()[0]
        doc_type_fields.setdefault(doc_cls.__doc_type__, []).append(f)
    doc_type_tuples = {}
    for doc_type, doc_fields in doc_type_fields.items():
        doc_type_tuples[doc_type] = namedtuple(doc_type, [f.get_attr() for f in doc_fields])

    def _processor(hit):
        doc_type = hit['_type']
        tuple_cls = doc_type_tuples[doc_type]
        
        args = ()
        source = hit.get('_source')
        if source:
            args = args + tuple(source.get(f.get_field()) for f in doc_fields)
        return tuple_cls(*args)

    return _processor
