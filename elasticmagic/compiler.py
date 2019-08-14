import collections
import operator

from elasticsearch import ElasticsearchException

from .document import Document
from .document import DynamicDocument
from .expression import Bool
from .expression import Exists
from .expression import Filtered
from .expression import FunctionScore
from .expression import HighlightedField
from .result import BulkResult
from .result import CountResult
from .result import DeleteByQueryResult
from .result import DeleteResult
from .result import ExistsResult
from .result import PutMappingResult
from .result import SearchResult
from .search import BaseSearchQuery
from .search import SearchQueryContext
from .types import ValidationError

OPERATORS = {
    operator.and_: 'and',
    operator.or_: 'or',
}


ESVersion = collections.namedtuple('ESVersion', ['major', 'minor', 'patch'])

ElasticsearchFeatures = collections.namedtuple(
    'ExpressionFeatures',
    [
        'supports_missing_query',
        'supports_parent_id_query',
        'supports_bool_filter',
        'supports_search_exists_api',
        'stored_fields_param',
    ]
)


class CompilationError(Exception):
    pass


class MultiSearchError(ElasticsearchException):
    pass


class Compiled(object):
    features = None

    def __init__(self, expression, **kwargs):
        self.expression = expression
        self.body = self.visit(expression)
        self.params = self.prepare_params(kwargs)

    def prepare_params(self, params):
        return params

    def process_result(self, raw_result):
        raise NotImplementedError

    def visit(self, expr, **kwargs):
        visit_name = None
        if hasattr(expr, '__visit_name__'):
            visit_name = expr.__visit_name__

        if visit_name:
            visit_func = getattr(self, 'visit_{}'.format(visit_name))
            return visit_func(expr, **kwargs)

        if isinstance(expr, dict):
            return self.visit_dict(expr)

        if isinstance(expr, (list, tuple)):
            return self.visit_list(expr)

        return expr

    def visit_params(self, params):
        res = {}
        for k, v in params.items():
            res[self.visit(k)] = self.visit(v)
        return res

    def visit_dict(self, dct):
        return {self.visit(k): self.visit(v) for k, v in dct.items()}

    def visit_list(self, lst):
        return [self.visit(v) for v in lst]


class CompiledExpression(Compiled):
    def visit_literal(self, expr):
        return expr.obj

    def visit_field(self, field):
        return field._name

    def visit_mapping_field(self, field):
        return field._name

    def visit_attributed_field(self, field):
        return field._field._name

    def visit_boost_expression(self, expr):
        return '{}^{}'.format(self.visit(expr.expr), self.visit(expr.weight))

    def visit_query_expression(self, expr):
        return {
            expr.__query_name__: self.visit(expr.params)
        }

    def visit_field_query(self, expr):
        if expr.params:
            params = {expr.__query_key__: self.visit(expr.query)}
            params.update(expr.params)
            return {
                expr.__query_name__: {
                    self.visit(expr.field): params
                }
            }
        else:
            return {
                expr.__query_name__: {
                    self.visit(expr.field): self.visit(expr.query)
                }
            }

    def visit_range(self, expr):
        field_params = {
            self.visit(expr.field): self.visit(expr.params)
        }
        return {
            'range': dict(self.visit(expr.range_params), **field_params)
        }

    def visit_terms(self, expr):
        params = {self.visit(expr.field): self.visit(expr.terms)}
        params.update(self.visit(expr.params))
        return {
            'terms': params
        }

    def visit_missing(self, expr):
        if self.features.supports_missing_query:
            return {
                'missing': self.visit(expr.params)
            }
        return self.visit(
            Bool.must_not(Exists(**expr.params))
        )

    def visit_multi_match(self, expr):
        params = {
            'query': self.visit(expr.query),
            'fields': [self.visit(f) for f in expr.fields],
        }
        params.update(self.visit(expr.params))
        return {
            'multi_match': params
        }

    def visit_match_all(self, expr):
        return {'match_all': self.visit(expr.params)}

    def visit_query(self, expr):
        params = {
            'query': self.visit(expr.query)
        }
        if expr.params:
            params.update(self.visit(expr.params))
            return {
                'fquery': params
            }
        return params

    def visit_boolean_expression(self, expr):
        if expr.params:
            params = {
                'filters': [self.visit(e) for e in expr.expressions]
            }
            params.update(self.visit(expr.params))
        else:
            params = [self.visit(e) for e in expr.expressions]
        return {
            OPERATORS[expr.operator]: params
        }

    def visit_not(self, expr):
        if expr.params:
            params = {
                'filter': self.visit(expr.expr)
            }
            params.update(self.visit(expr.params))
        else:
            params = self.visit(expr.expr)
        return {
            'not': params
        }

    def visit_sort(self, expr):
        if expr.params:
            params = {'order': self.visit(expr.order)}
            params.update(self.visit(expr.params))
            return {
                self.visit(expr.expr): params
            }
        elif expr.order:
            return {
                self.visit(expr.expr): self.visit(expr.order)
            }
        else:
            return self.visit(expr.expr)

    def visit_agg(self, agg):
        return {
            agg.__agg_name__: self.visit(agg.params)
        }

    def visit_bucket_agg(self, agg):
        params = {
            agg.__agg_name__: self.visit(agg.params)
        }
        if agg._aggregations:
            params['aggregations'] = self.visit(agg._aggregations)
        return params

    def visit_filter_agg(self, agg):
        params = self.visit_bucket_agg(agg)
        params[agg.__agg_name__] = self.visit(agg.filter)
        return params

    def visit_source(self, expr):
        if expr.include or expr.exclude:
            params = {}
            if expr.include:
                params['include'] = self.visit(expr.include)
            if expr.exclude:
                params['exclude'] = self.visit(expr.exclude)
            return params
        if isinstance(expr.fields, bool):
            return expr.fields
        return [self.visit(f) for f in expr.fields]

    def visit_query_rescorer(self, rescorer):
        return {'query': self.visit(rescorer.params)}

    def visit_rescore(self, rescore):
        params = self.visit(rescore.rescorer)
        if rescore.window_size is not None:
            params['window_size'] = rescore.window_size
        return params

    def visit_highlighted_field(self, hf):
        return {
            self.visit(hf.field): self.visit(hf.params)
        }

    def visit_highlight(self, highlight):
        params = self.visit(highlight.params)
        if highlight.fields:
            if isinstance(highlight.fields, collections.Mapping):
                compiled_fields = {}
                for f, options in highlight.fields.items():
                    compiled_fields[self.visit(f)] = self.visit(options)
                params['fields'] = compiled_fields
            elif isinstance(highlight.fields, collections.Iterable):
                compiled_fields = []
                for f in highlight.fields:
                    if isinstance(f, (HighlightedField, collections.Mapping)):
                        compiled_fields.append(self.visit(f))
                    else:
                        compiled_fields.append({self.visit(f): {}})
                params['fields'] = compiled_fields
        return params

    def visit_parent_id(self, expr):
        if not self.features.supports_parent_id_query:
            raise CompilationError(
                'Elasticsearch before 5.x does not have support for '
                'parent_id query'
            )
        child_type = expr.child_type
        if hasattr(child_type, '__doc_type__'):
            child_type = child_type.__doc_type__
        if not child_type:
            raise CompilationError(
                "Cannot detect child type, specify 'child_type' argument"
            )

        return {'parent_id': {'type': child_type, 'id': expr.parent_id}}

    def visit_has_parent(self, expr):
        params = self.visit(expr.params)
        parent_type = expr.parent_type
        if hasattr(parent_type, '__doc_type__'):
            parent_type = parent_type.__doc_type__
        if not parent_type:
            parent_doc_classes = expr.params._collect_doc_classes()
            if len(parent_doc_classes) == 1:
                parent_type = next(iter(parent_doc_classes)).__doc_type__
            elif len(parent_doc_classes) > 1:
                raise CompilationError(
                    'Too many candidates for parent type, '
                    'should be only one'
                )
            else:
                raise CompilationError(
                    'Cannot detect parent type, '
                    'specify \'parent_type\' argument'
                )
        params['parent_type'] = parent_type
        return {'has_parent': params}

    def visit_has_child(self, expr):
        params = self.visit(expr.params)
        child_type = expr.type
        if hasattr(child_type, '__doc_type__'):
            child_type = child_type.__doc_type__
        if not child_type:
            child_doc_classes = expr.params._collect_doc_classes()
            if len(child_doc_classes) == 1:
                child_type = next(iter(child_doc_classes)).__doc_type__
            elif len(child_doc_classes) > 1:
                raise CompilationError(
                    'Too many candidates for child type, '
                    'should be only one'
                )
            else:
                raise CompilationError(
                    'Cannot detect child type, '
                    'specify \'type\' argument'
                )
        params['type'] = child_type
        return {'has_child': params}

    def visit_script(self, script):
        # TODO Wrap into a dictionary with 'script' key
        return self.visit(script.params)

    def visit_function(self, func):
        params = {func.__func_name__: self.visit(func.params)}
        if func.filter:
            params['filter'] = self.visit(func.filter)
        if func.weight is not None:
            params['weight'] = self.visit(func.weight)
        return params

    def visit_weight_function(self, func):
        params = {func.__func_name__: func.weight}
        if func.filter:
            params['filter'] = self.visit(func.filter)
        return params

    def visit_decay_function(self, func):
        params = {func.__func_name__: {
            self.visit(func.field): self.visit(func.decay_params)
        }}
        if func.params:
            params[func.__func_name__].update(self.visit(func.params))
        if func.filter:
            params['filter'] = self.visit(func.filter)
        return params


class CompiledSearchQuery(CompiledExpression):
    features = None

    def __init__(self, query, **kwargs):
        if isinstance(query, BaseSearchQuery):
            expression = query.get_context()
        elif query is None:
            expression = None
        else:
            expression = {
                'query': query,
            }
        self.query = query
        super(CompiledSearchQuery, self).__init__(
            expression, **kwargs
        )

    def api_method(self, client):
        return client.search

    def prepare_params(self, params):
        if isinstance(self.expression, SearchQueryContext):
            search_params = dict(self.expression.search_params)
            search_params.update(params)
            if self.expression.doc_type:
                search_params['doc_type'] = self.expression.doc_type
        else:
            search_params = params
        return search_params

    def process_result(self, raw_result):
        return SearchResult(
            raw_result,
            aggregations=self.expression.aggregations,
            doc_cls=self.expression.doc_classes,
            instance_mapper=self.expression.instance_mapper,
        )

    @classmethod
    def get_query(cls, query_context, wrap_function_score=True):
        q = query_context.q
        if wrap_function_score:
            for (functions, params) in reversed(
                    # Without wrapping in list it fails on Python 3.4
                    list(query_context.function_scores.values())
            ):
                if not functions:
                    continue
                q = FunctionScore(
                    query=q,
                    functions=functions,
                    **params
                )
        return q

    @classmethod
    def get_filtered_query(cls, query_context, wrap_function_score=True):
        q = cls.get_query(
            query_context, wrap_function_score=wrap_function_score
        )
        if query_context.filters:
            filter_clauses = list(query_context.iter_filters())
            if cls.features.supports_bool_filter:
                return Bool(must=q, filter=Bool.must(*filter_clauses))
            return Filtered(
                query=q, filter=Bool.must(*filter_clauses)
            )
        return q

    @classmethod
    def get_post_filter(cls, query_context):
        post_filters = list(query_context.iter_post_filters())
        if post_filters:
            return Bool.must(*post_filters)

    def visit_search_query_context(self, query_ctx):
        params = {}

        q = self.get_filtered_query(query_ctx)
        if q is not None:
            params['query'] = self.visit(q)

        post_filter = self.get_post_filter(query_ctx)
        if post_filter:
            params['post_filter'] = self.visit(post_filter)

        if query_ctx.order_by:
            params['sort'] = self.visit(query_ctx.order_by)
        if query_ctx.source:
            params['_source'] = self.visit(query_ctx.source)
        if query_ctx.fields is not None:
            stored_fields_param = self.features.stored_fields_param
            if query_ctx.fields is True:
                params[stored_fields_param] = '*'
            elif query_ctx.fields is False:
                pass
            else:
                params[stored_fields_param] = self.visit(
                    query_ctx.fields
                )
        if query_ctx.aggregations:
            params['aggregations'] = self.visit(
                query_ctx.aggregations
            )
        if query_ctx.limit is not None:
            params['size'] = query_ctx.limit
        if query_ctx.offset is not None:
            params['from'] = query_ctx.offset
        if query_ctx.min_score is not None:
            params['min_score'] = query_ctx.min_score
        if query_ctx.rescores:
            params['rescore'] = self.visit(query_ctx.rescores)
        if query_ctx.suggest:
            params['suggest'] = self.visit(query_ctx.suggest)
        if query_ctx.highlight:
            params['highlight'] = self.visit(query_ctx.highlight)
        if query_ctx.script_fields:
            params['script_fields'] = self.visit(
                query_ctx.script_fields
            )
        return params


class CompiledScalarQuery(CompiledSearchQuery):
    def visit_search_query_context(self, query_ctx):
        params = {}

        q = self.get_filtered_query(query_ctx)
        if q is not None:
            params['query'] = self.visit(q)

        post_filter = self.get_post_filter(query_ctx)
        if post_filter:
            params['post_filter'] = self.visit(post_filter)

        if query_ctx.min_score is not None:
            params['min_score'] = query_ctx.min_score
        return params


class CompiledCountQuery(CompiledScalarQuery):
    def api_method(self, client):
        return client.count

    def process_result(self, raw_result):
        return CountResult(raw_result)


class CompiledExistsQuery(CompiledScalarQuery):
    def __init__(self, query, **kwargs):
        super(CompiledExistsQuery, self).__init__(query, **kwargs)
        if not self.features.supports_search_exists_api:
            if self.body is None:
                self.body = {}
            self.body['size'] = 0
            self.body['terminate_after'] = 1

    def api_method(self, client):
        if self.features.supports_search_exists_api:
            return client.exists
        else:
            return client.search

    def process_result(self, raw_result):
        if self.features.supports_search_exists_api:
            return ExistsResult(raw_result)
        return ExistsResult({
            'exists': SearchResult(raw_result).total >= 1
        })


class CompiledDeleteByQuery(CompiledScalarQuery):
    def api_method(self, client):
        return client.delete_by_query

    def process_result(self, raw_result):
        return DeleteByQueryResult(raw_result)


class CompiledMultiSearch(Compiled):
    compiled_search = None

    def __init__(self, queries, raise_on_error, **kwargs):
        self.expression = queries
        self.raise_on_error = raise_on_error
        self.body = []
        self.compiled_queries = []
        self.params = {}
        for q in queries:
            compiled_query = self.compiled_search(q, **kwargs)
            self.compiled_queries.append(compiled_query)
            params = compiled_query.params
            if isinstance(compiled_query.expression, SearchQueryContext):
                index = compiled_query.expression.index
                if index:
                    params['index'] = index.get_name()
            if 'doc_type' in params:
                params['type'] = params.pop('doc_type')
            self.body.append(params)
            self.body.append(compiled_query.body)

    def api_method(self, client):
        return client.msearch

    def process_result(self, raw_result):
        errors = []
        for raw, query, compiled_query in zip(
                raw_result['responses'], self.expression, self.compiled_queries
        ):
            result = compiled_query.process_result(raw)
            query._cached_result = result
            if result.error:
                errors.append(result.error)

        if self.raise_on_error and errors:
            if len(errors) == 1:
                error_msg = '1 query was failed'
            else:
                error_msg = '{} queries were failed'.format(len(errors))
            raise MultiSearchError(error_msg, errors)

        return [q.get_result() for q in self.expression]


class CompiledPutMapping(Compiled):
    def __init__(self, doc_cls_or_mapping, ordered=False, **kwargs):
        self._dict_type = collections.OrderedDict if ordered else dict
        self._dynamic_templates = []
        super(CompiledPutMapping, self).__init__(doc_cls_or_mapping, **kwargs)

    def api_method(self, client):
        return client.indices.put_mapping

    def prepare_params(self, params):
        if params.get('doc_type') is None:
            params['doc_type'] = getattr(
                self.expression, '__doc_type__', None
            )
        return params

    def process_result(self, raw_result):
        return PutMappingResult(raw_result)

    def _visit_dynamic_field(self, field):
        self._dynamic_templates.append(
            {
                field._field._name: {
                    'path_match': field._field._name,
                    'mapping': next(iter(self.visit(field).values()))
                }
            }
        )

    def visit_field(self, field):
        field_type = field.get_type()
        mapping = self._dict_type()
        mapping['type'] = field_type.__visit_name__

        if field_type.doc_cls:
            mapping.update(field_type.doc_cls.__mapping_options__)
            mapping['properties'] = self.visit(field_type.doc_cls.user_fields)

        if field._fields:
            if isinstance(field._fields, collections.Mapping):
                for subfield_name, subfield in field._fields.items():
                    subfield_name = subfield.get_name() or subfield_name
                    subfield_mapping = next(iter(
                        self.visit(subfield).values()
                    ))
                    mapping.setdefault('fields', {}) \
                        .update({subfield_name: subfield_mapping})
            else:
                for subfield in field._fields:
                    mapping.setdefault('fields', {}) \
                        .update(self.visit(subfield))

        mapping.update(field._mapping_options)

        return {
            field.get_name(): mapping
        }

    def visit_mapping_field(self, field):
        mapping = self._dict_type()
        if field._mapping_options:
            mapping[field.get_name()] = field._mapping_options
        return mapping

    def visit_attributed_field(self, field):
        for f in field.dynamic_fields:
            self._visit_dynamic_field(f)
        return self.visit(field.get_field())

    def visit_ordered_attributes(self, attrs):
        mapping = self._dict_type()
        for f in attrs:
            mapping.update(self.visit(f))
        return mapping

    def visit_document(self, doc_cls):
        mapping = self._dict_type()
        mapping.update(doc_cls.__mapping_options__)
        mapping.update(self.visit(doc_cls.mapping_fields))
        mapping['properties'] = self.visit(doc_cls.user_fields)
        for f in doc_cls.dynamic_fields:
            self._visit_dynamic_field(f)
        if self._dynamic_templates:
            mapping['dynamic_templates'] = self._dynamic_templates
        return {
            doc_cls.__doc_type__: mapping
        }


class CompiledGet(Compiled):
    META_FIELDS = ('_id', '_type', '_routing', '_parent', '_version')

    def __init__(self, doc_or_id, **kwargs):
        self.body = None
        self.params = {}
        self.doc_cls = kwargs.pop('doc_cls', None) or DynamicDocument

        if isinstance(doc_or_id, Document):
            doc = doc_or_id
            for meta_field_name in self.META_FIELDS:
                field_value = getattr(doc, meta_field_name, None)
                param_name = meta_field_name.lstrip('_')
                if field_value is not None:
                    self.params[param_name] = field_value
            self.doc_cls = doc.__class__
        elif isinstance(doc_or_id, dict):
            doc = doc_or_id
            self.params.update(doc)
            if doc.get('doc_cls'):
                self.doc_cls = doc.pop('doc_cls')
        else:
            doc_id = doc_or_id
            self.params.update({'id': doc_id})

        if self.params.get('doc_type') is None:
            self.params['doc_type'] = getattr(
                self.doc_cls, '__doc_type__', None
            )
        self.params.update(kwargs)

    def api_method(self, client):
        return client.get

    def process_result(self, raw_result):
        return self.doc_cls(_hit=raw_result)


class CompiledMultiGet(Compiled):
    compiled_get = None

    def __init__(self, docs_or_ids, **kwargs):
        default_doc_cls = kwargs.pop('doc_cls', None)
        if isinstance(default_doc_cls, collections.Iterable):
            self.doc_cls_map = {
                doc_cls.__doc_type__: doc_cls
                for doc_cls in default_doc_cls
            }
            self.default_doc_cls = DynamicDocument
        elif default_doc_cls:
            self.doc_cls_map = {}
            self.default_doc_cls = default_doc_cls
        else:
            self.doc_cls_map = {}
            self.default_doc_cls = DynamicDocument

        self.expression = docs_or_ids
        self.doc_classes = []
        docs = []
        for doc_or_id in docs_or_ids:
            if isinstance(doc_or_id, Document):
                doc = doc_or_id.to_meta()
                doc_cls = doc_or_id.__class__
            elif isinstance(doc_or_id, dict):
                doc = doc_or_id
                doc_cls = doc_or_id.pop('doc_cls', None)
            else:
                doc = {'_id': doc_or_id}
                doc_cls = None

            if not doc.get('_type') and hasattr(doc_cls, '__doc_type__'):
                doc['_type'] = doc_cls.__doc_type__

            docs.append(doc)
            self.doc_classes.append(doc_cls)

        self.body = {'docs': docs}
        self.params = self.prepare_params(kwargs)

    def api_method(self, client):
        return client.mget

    def process_result(self, raw_result):
        docs = []
        for doc_cls, raw_doc in zip(self.doc_classes, raw_result['docs']):
            doc_type = raw_doc.get('_type')
            if doc_cls is None and doc_type in self.doc_cls_map:
                doc_cls = self.doc_cls_map.get(doc_type)
            if doc_cls is None:
                doc_cls = self.default_doc_cls

            if raw_doc.get('found'):
                docs.append(doc_cls(_hit=raw_doc))
            else:
                docs.append(None)
        return docs


class CompiledDelete(CompiledGet):
    def api_method(self, client):
        return client.delete

    def process_result(self, raw_result):
        return DeleteResult(raw_result)


class CompiledBulk(Compiled):
    compiled_meta = None
    compiled_source = None

    def __init__(self, actions, **kwargs):
        self.expression = actions
        self.body = []
        for action in actions:
            meta = self.compiled_meta(action).body
            self.body.append(meta)
            source = self.compiled_source(action).body
            if source is not None:
                self.body.append(source)
        self.params = self.prepare_params(kwargs)

    def api_method(self, client):
        return client.bulk

    def process_result(self, raw_result):
        return BulkResult(raw_result)


class CompiledMeta(Compiled):
    META_FIELD_NAMES = (
        '_id',
        '_index',
        '_type',
        '_routing',
        '_parent',
        '_timestamp',
        '_ttl',
        '_version',
    )

    def __init__(self, doc_or_action):
        super(CompiledMeta, self).__init__(doc_or_action)

    def visit_action(self, action):
        meta = self.visit_document(action.doc)
        meta.update(action.meta_params)
        return {
            action.__action_name__: meta
        }

    def visit_document(self, doc):
        meta = {}
        if isinstance(doc, Document):
            self._populate_meta_from_document(doc, meta)
            if doc.__doc_type__:
                meta['_type'] = doc.__doc_type__
        else:
            self._populate_meta_from_dict(doc, meta)
        return meta

    def _populate_meta_from_document(self, doc, meta):
        for field_name in self.META_FIELD_NAMES:
            value = getattr(doc, field_name, None)
            if value:
                meta[field_name] = value

    def _populate_meta_from_dict(self, doc, meta):
        for field_name in self.META_FIELD_NAMES:
            value = doc.get(field_name)
            if value:
                meta[field_name] = value


class CompiledSource(Compiled):
    def __init__(self, doc_or_action, validate=False):
        self._validate = validate
        super(CompiledSource, self).__init__(doc_or_action)

    def visit_action(self, action):
        if action.__action_name__ == 'delete':
            return None

        if isinstance(action.doc, Document):
            source = self.visit(action.doc)
        else:
            source = action.doc.copy()
            for exclude_field in Document.mapping_fields:
                source.pop(exclude_field.get_field().get_name(), None)

        if action.__action_name__ == 'update':
            if source:
                source = {'doc': source}
            source.update(action.source_params)

        return source

    def visit_document(self, doc):
        res = {}
        for key, value in doc.__dict__.items():
            if key in doc.__class__.mapping_fields:
                continue

            attr_field = doc.__class__.fields.get(key)
            if attr_field:
                if value is None or value == '' or value == []:
                    if (
                            self._validate and
                            attr_field.get_field()._mapping_options.get(
                                'required'
                            )
                    ):
                        raise ValidationError("'{}' is required".format(
                            attr_field.get_attr_name()
                        ))
                    continue
                value = attr_field.get_type() \
                    .from_python(value, validate=self._validate)
                res[attr_field._field._name] = value

        for attr_field in doc._fields.values():
            if (
                    self._validate
                    and attr_field.get_field()._mapping_options.get('required')
                    and attr_field.get_field().get_name() not in res
            ):
                raise ValidationError(
                    "'{}' is required".format(attr_field.get_attr_name())
                )

        return res


features_1_0 = ElasticsearchFeatures(
    supports_missing_query=True,
    supports_parent_id_query=False,
    supports_bool_filter=False,
    supports_search_exists_api=True,
    stored_fields_param='fields',
)


class CompiledExpression_1_0(CompiledExpression):
    features = features_1_0


class CompiledSearchQuery_1_0(CompiledSearchQuery):
    features = features_1_0


class CompiledCountQuery_1_0(CompiledCountQuery):
    features = features_1_0


class CompiledExistsQuery_1_0(CompiledExistsQuery):
    features = features_1_0


class CompiledDeleteByQuery_1_0(CompiledDeleteByQuery):
    features = features_1_0


class CompiledMultiSearch_1_0(CompiledMultiSearch):
    features = features_1_0
    compiled_search = CompiledSearchQuery_1_0


class CompiledGet_1_0(CompiledGet):
    features = features_1_0


class CompiledMultiGet_1_0(CompiledMultiGet):
    features = features_1_0


class CompiledDelete_1_0(CompiledDelete):
    features = features_1_0


class CompiledMeta_1_0(CompiledMeta):
    features = features_1_0


class CompiledSource_1_0(CompiledSource):
    features = features_1_0


class CompiledBulk_1_0(CompiledBulk):
    features = features_1_0
    compiled_meta = CompiledMeta_1_0
    compiled_source = CompiledSource_1_0


class CompiledPutMapping_1_0(CompiledPutMapping):
    features = features_1_0


class Compiler_1_0(object):
    compiled_expression = CompiledExpression_1_0
    compiled_search_query = CompiledExpression_1_0
    compiled_query = compiled_search_query
    compiled_count_query = CompiledCountQuery_1_0
    compiled_exists_query = CompiledExistsQuery_1_0
    compiled_delete_by_query = CompiledDeleteByQuery_1_0
    compiled_multi_search = CompiledMultiSearch_1_0
    compiled_get = CompiledGet_1_0
    compiled_multi_get = CompiledMultiGet_1_0
    compiled_delete = CompiledDelete_1_0
    compiled_bulk = CompiledBulk_1_0
    compiled_put_mapping = CompiledPutMapping_1_0


features_2_0 = ElasticsearchFeatures(
    supports_missing_query=True,
    supports_parent_id_query=False,
    supports_bool_filter=True,
    supports_search_exists_api=True,
    stored_fields_param='fields',
)


class CompiledExpression_2_0(CompiledExpression):
    features = features_2_0


class CompiledSearchQuery_2_0(CompiledSearchQuery):
    features = features_2_0


class CompiledCountQuery_2_0(CompiledCountQuery):
    features = features_2_0


class CompiledExistsQuery_2_0(CompiledExistsQuery):
    features = features_2_0


class CompiledDeleteByQuery_2_0(CompiledDeleteByQuery):
    features = features_2_0


class CompiledMultiSearch_2_0(CompiledMultiSearch):
    features = features_2_0
    compiled_search = CompiledSearchQuery_2_0


class CompiledGet_2_0(CompiledGet):
    features = features_2_0


class CompiledMultiGet_2_0(CompiledMultiGet):
    features = features_2_0


class CompiledDelete_2_0(CompiledDelete):
    features = features_2_0


class CompiledMeta_2_0(CompiledMeta):
    features = features_2_0


class CompiledSource_2_0(CompiledSource):
    features = features_2_0


class CompiledBulk_2_0(CompiledBulk):
    features = features_2_0
    compiled_meta = CompiledMeta_2_0
    compiled_source = CompiledSource_2_0


class CompiledPutMapping_2_0(CompiledPutMapping):
    features = features_2_0


class Compiler_2_0(object):
    compiled_expression = CompiledExpression_2_0
    compiled_search_query = CompiledSearchQuery_2_0
    compiled_query = compiled_search_query
    compiled_count_query = CompiledCountQuery_2_0
    compiled_exists_query = CompiledExistsQuery_2_0
    compiled_delete_by_query = CompiledDeleteByQuery_2_0
    compiled_multi_search = CompiledMultiSearch_2_0
    compiled_get = CompiledGet_2_0
    compiled_multi_get = CompiledMultiGet_2_0
    compiled_delete = CompiledDelete_2_0
    compiled_bulk = CompiledBulk_2_0
    compiled_put_mapping = CompiledPutMapping_2_0


features_5_0 = ElasticsearchFeatures(
    supports_missing_query=False,
    supports_parent_id_query=True,
    supports_bool_filter=True,
    supports_search_exists_api=False,
    stored_fields_param='stored_fields',
)


class CompiledExpression_5_0(CompiledExpression):
    features = features_5_0


class CompiledSearchQuery_5_0(CompiledSearchQuery):
    features = features_5_0


class CompiledCountQuery_5_0(CompiledCountQuery):
    features = features_5_0


class CompiledExistsQuery_5_0(CompiledExistsQuery):
    features = features_5_0


class CompiledDeleteByQuery_5_0(CompiledDeleteByQuery):
    features = features_5_0


class CompiledMultiSearch_5_0(CompiledMultiSearch):
    compiled_search = CompiledSearchQuery_5_0


class CompiledGet_5_0(CompiledGet):
    features = features_5_0


class CompiledMultiGet_5_0(CompiledMultiGet):
    features = features_5_0


class CompiledDelete_5_0(CompiledDelete):
    features = features_5_0


class CompiledMeta_5_0(CompiledMeta):
    features = features_5_0


class CompiledSource_5_0(CompiledSource):
    features = features_5_0


class CompiledBulk_5_0(CompiledBulk):
    features = features_5_0
    compiled_meta = CompiledMeta_5_0
    compiled_source = CompiledSource_5_0


class CompiledPutMapping_5_0(CompiledPutMapping):
    features = features_5_0


class Compiler_5_0(object):
    compiled_expression = CompiledExpression_5_0
    compiled_search_query = CompiledSearchQuery_5_0
    compiled_query = compiled_search_query
    compiled_count_query = CompiledCountQuery_5_0
    compiled_exists_query = CompiledExistsQuery_5_0
    compiled_delete_by_query = CompiledDeleteByQuery_5_0
    compiled_multi_search = CompiledMultiSearch_5_0
    compiled_get = CompiledGet_5_0
    compiled_multi_get = CompiledMultiGet_5_0
    compiled_delete = CompiledDelete_5_0
    compiled_bulk = CompiledBulk_5_0
    compiled_put_mapping = CompiledPutMapping_5_0


Compiler10 = Compiler_1_0

Compiler20 = Compiler_2_0

Compiler50 = Compiler_5_0

DefaultCompiler = Compiler_5_0


def get_compiler_by_es_version(es_version):
    if es_version.major <= 1:
        return Compiler_1_0
    elif es_version.major == 2:
        return Compiler_2_0
    elif es_version.major == 5:
        return Compiler_5_0
    return Compiler_5_0
