from .util import clean_params


class Action(object):
    __visit_name__ = 'action'
    __action_name__ = None

    def __init__(self, doc, index=None, doc_type=None,
                 consistency=None, refresh=None,
                 routing=None, parent=None, timestamp=None, ttl=None,
                 version=None, version_type=None, **kwargs):
        from .index import Index as ESIndex

        index = index._name if isinstance(index, ESIndex) else index

        self.doc = doc
        self.params = clean_params({
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
        })
        self.params.update(clean_params(kwargs))

    def get_meta(self, compiler=None):
        from .compiler import DefaultCompiler

        meta_compiler = (compiler or DefaultCompiler).compiled_meta
        meta = meta_compiler(self).params
        return next(iter(meta.values()))

    def get_source(self, compiler=None):
        from .compiler import DefaultCompiler

        source_compiler = (compiler or DefaultCompiler).compiled_source
        return source_compiler(self).params


class Index(Action):
    __action_name__ = 'index'


class Delete(Action):
    __action_name__ = 'delete'


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
