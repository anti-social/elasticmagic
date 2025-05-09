from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable, Mapping
from functools import partial

from elasticsearch import ElasticsearchException

from elasticmagic.attribute import AttributedField
from .document import DOC_TYPE_JOIN_FIELD
from .document import DOC_TYPE_FIELD
from .document import DOC_TYPE_NAME_FIELD
from .document import DOC_TYPE_PARENT_FIELD
from .document import Document
from .document import DynamicDocument
from .document import get_doc_type_for_hit
from .document import mk_uid
from .expression import Bool
from .expression import Exists
from .expression import FunctionScore
from .expression import HighlightedField
from .expression import Ids
from .expression import MatchPhrase
from .expression import MatchPhrasePrefix
from .expression import Params
from .expression import Terms
from .result import BulkResult
from .result import CountResult
from .result import DeleteByQueryResult
from .result import DeleteResult
from .result import ExistsResult
from .result import ExplainResult
from .result import PutMappingResult
from .result import SearchResult
from .search import BaseSearchQuery
from .search import SearchQueryContext
from .types import ValidationError
from .util import collect_doc_classes


DEFAULT_DOC_TYPE = '_doc'

ESVersion = namedtuple(
    'ESVersion', ['major', 'minor', 'patch', 'distribution']
)

ElasticsearchFeatures = namedtuple(
    'ExpressionFeatures',
    [
        'requires_doc_type',
        'supports_track_total_hits',
        'bulk_update_underscore_retry_on_conflict',
    ]
)


class CompilationError(Exception):
    pass


class MultiSearchError(ElasticsearchException):
    pass


def _is_emulate_doc_types_mode(features, doc_cls):
    return (
        hasattr(doc_cls, 'get_doc_type') and
        doc_cls.get_doc_type() and
        doc_cls.has_parent_doc_cls()
    )


def _add_doc_type_fields_into_stored_fields(
        stored_fields, add_source
):
    extra_stored_fields = []
    if add_source:
        extra_stored_fields.append('_source')
    extra_stored_fields.extend([DOC_TYPE_NAME_FIELD, DOC_TYPE_PARENT_FIELD])
    if not stored_fields:
        return extra_stored_fields
    elif isinstance(stored_fields, str):
        return [stored_fields] + extra_stored_fields
    elif isinstance(stored_fields, list):
        return stored_fields + extra_stored_fields
    raise ValueError(
        'Unsupported stored_fields type: {}'.format(type(stored_fields))
    )


def _patch_stored_fields_in_params(features, params, add_source_field):
    stored_fields = params.get('stored_fields')
    if isinstance(stored_fields, str):
        stored_fields = stored_fields.split(',')
    stored_fields = _add_doc_type_fields_into_stored_fields(
        stored_fields, add_source_field
    )
    params['stored_fields'] = ','.join(stored_fields)
    return params


def _has_custom_source(params):
    source = params.get('_source')
    return bool(
        (
            source not in (False, 'false') and
            source != [False] and
            source != ['false']
        ) or
        params.get('_source_includes') or
        params.get('_source_excludes')
    )


def _mk_doc_type(doc_types):
    return ','.join(doc_types)


def _mk_doc_cls_map(doc_classes, requires_doc_type):
    if doc_classes is None:
        doc_classes = ()
    elif isinstance(doc_classes, Iterable):
        doc_classes = list(doc_classes)
    else:
        doc_classes = (doc_classes,)

    doc_cls_map = {
        doc_cls.__doc_type__: doc_cls for doc_cls in doc_classes
    }
    if not requires_doc_type and len(doc_classes) == 1:
        doc_cls_map['_doc'] = doc_classes[0]

    return doc_cls_map


class Compiled(object):
    compiler = None
    features = None

    def __init__(self, expression, params=None):
        self.expression = expression
        self.body = self.visit(expression)
        self.params = self.prepare_params(params or {})

    def prepare_params(self, params):
        return params

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

    def visit_script(self, script):
        res = dict()
        if script.lang:
            res['lang'] = script.lang
        if script.script_params:
            res['params'] = script.script_params
        if script.inline:
            res['source'] = script.inline
        elif script.id:
            res['id'] = script.id
        else:
            raise CompilationError('Invalid arguments for Script')
        return self.visit_dict(res)


class CompiledEndpoint(Compiled):
    def process_result(self, raw_result):
        raise NotImplementedError


class CompiledExpression(Compiled):
    def __init__(self, expr, params=None, doc_classes=None):
        self.doc_classes = doc_classes
        super(CompiledExpression, self).__init__(expr, params)

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

    def visit_field_query(self, expr, **kwargs):
        expr_params = Params(expr.params, **kwargs)
        if expr_params:
            params = {expr.__query_key__: self.visit(expr.query)}
            params.update(expr_params)
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

    def visit_match(self, expr):
        if expr.type:
            if expr.type == 'phrase':
                return self.visit(
                    MatchPhrase(expr.field, expr.query, **expr.params)
                )
            elif expr.type == 'phrase_prefix':
                return self.visit(
                    MatchPhrasePrefix(expr.field, expr.query, *expr.params)
                )
            else:
                raise ValueError(
                    'Match query type is not supported: [{}]'.format(expr.type)
                )
        params = self.visit_field_query(expr, type=expr.type)
        return params

    def visit_range(self, expr):
        field_params = {
            self.visit(expr.field): self.visit(expr.params)
        }
        return {
            'range': dict(self.visit(expr.range_params), **field_params)
        }

    @staticmethod
    def _get_field_doc_cls(field):
        if isinstance(field, AttributedField):
            return field.get_parent()

    def visit_term(self, term):
        field_name = self.visit(term.field)
        if field_name == '_id':
            doc_cls = self._get_field_doc_cls(term.field)
            if _is_emulate_doc_types_mode(self.features, doc_cls):
                return self.visit(Ids([term.query], doc_cls))
            elif (
                    self.doc_classes and
                    any(map(
                        partial(_is_emulate_doc_types_mode, self.features),
                        self.doc_classes
                    ))
            ):
                return self.visit(Ids([term.query], self.doc_classes))
        return self.visit_field_query(term)

    def visit_terms(self, expr):
        field_name = self.visit(expr.field)
        if field_name == '_id':
            doc_cls = self._get_field_doc_cls(expr.field)
            if _is_emulate_doc_types_mode(self.features, doc_cls):
                return self.visit(Ids(expr.terms, doc_cls))
            elif (
                    self.doc_classes and
                    any(map(
                        partial(_is_emulate_doc_types_mode, self.features),
                        self.doc_classes
                    ))
            ):
                return self.visit(Ids(expr.terms, self.doc_classes))
        params = {self.visit(expr.field): self.visit(expr.terms)}
        params.update(self.visit(expr.params))
        return {
            'terms': params
        }

    def visit_missing(self, expr):
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

    def visit_sort_script(self, sort_script):
        res = dict(script=dict())
        if sort_script.script_type:
            res['type'] = sort_script.script_type
        if sort_script.order:
            res['order'] = sort_script.order
        if sort_script.script:
            res['script'] = self.visit(sort_script.script)
        else:
            raise CompilationError('Invalid arguments for ScriptSort')
        return self.visit_dict({'_script': res})

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

    def visit_top_hits_agg(self, agg):
        params = self.visit(agg.params)
        if self.doc_classes:
            self._patch_docvalue_fields(params, self.doc_classes)
        return {
            agg.__agg_name__: params
        }

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
            if isinstance(highlight.fields, Mapping):
                compiled_fields = {}
                for f, options in highlight.fields.items():
                    compiled_fields[self.visit(f)] = self.visit(options)
                params['fields'] = compiled_fields
            elif isinstance(highlight.fields, Iterable):
                compiled_fields = []
                for f in highlight.fields:
                    if isinstance(f, (HighlightedField, Mapping)):
                        compiled_fields.append(self.visit(f))
                    else:
                        compiled_fields.append({self.visit(f): {}})
                params['fields'] = compiled_fields
        return params

    def visit_ids(self, expr):
        params = self.visit(expr.params)

        if (
                isinstance(expr.type, type) and
                issubclass(expr.type, Document) and
                _is_emulate_doc_types_mode(self.features, expr.type)
        ):
            params['values'] = [
                mk_uid(expr.type.get_doc_type(), v)
                for v in expr.values
            ]
        elif (
                self.doc_classes and
                any(map(
                    partial(_is_emulate_doc_types_mode, self.features),
                    self.doc_classes
                ))
        ):
            ids = []
            for doc_cls in self.doc_classes:
                if _is_emulate_doc_types_mode(self.features, doc_cls):
                    ids.extend(
                        mk_uid(doc_cls.__doc_type__, v)
                        for v in expr.values
                    )
            params['values'] = ids
        else:
            params['values'] = expr.values
            if expr.type:
                doc_type = getattr(expr.type, '__doc_type__', None)
                if doc_type:
                    params['type'] = doc_type
                else:
                    params['type'] = self.visit(expr.type)

        return {
            'ids': params
        }

    def visit_parent_id(self, expr):
        if _is_emulate_doc_types_mode(self.features, expr.child_type):
            parent_id = mk_uid(
                expr.child_type.__parent__.__doc_type__,
                expr.parent_id
            )
        else:
            parent_id = expr.parent_id

        child_type = expr.child_type
        if hasattr(child_type, '__doc_type__'):
            child_type = child_type.__doc_type__
        if not child_type:
            raise CompilationError(
                "Cannot detect child type, specify 'child_type' argument"
            )

        return {'parent_id': {'type': child_type, 'id': parent_id}}

    def visit_has_parent(self, expr):
        params = self.visit(expr.params)
        parent_type = expr.parent_type
        if hasattr(parent_type, '__doc_type__'):
            parent_type = parent_type.__doc_type__
        if not parent_type:
            parent_doc_classes = collect_doc_classes(expr.params)
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

    def _patch_docvalue_fields(self, params, doc_classes):
        parent_doc_types = set()
        should_inject_stored_fields = False
        for doc_cls in doc_classes:
            if not doc_cls.has_parent_doc_cls():
                continue
            parent_doc_cls = doc_cls.get_parent_doc_cls()
            should_inject_stored_fields = True
            if not parent_doc_cls:
                continue
            parent_doc_type = parent_doc_cls.get_doc_type()
            if parent_doc_type:
                parent_doc_types.add(parent_doc_type)
        if not should_inject_stored_fields:
            return params

        docvalue_fields = params.get('docvalue_fields', [])
        if DOC_TYPE_JOIN_FIELD not in docvalue_fields:
            docvalue_fields.append(DOC_TYPE_JOIN_FIELD)
        doc_type_join_parents = '%s#*' % DOC_TYPE_JOIN_FIELD
        if doc_type_join_parents not in docvalue_fields:
            docvalue_fields.append(doc_type_join_parents)
        params['docvalue_fields'] = docvalue_fields

        return params


class CompiledSearchQuery(CompiledExpression, CompiledEndpoint):
    features = None

    def __init__(self, query, params=None):
        if isinstance(query, BaseSearchQuery):
            expression = query.get_context()
            doc_classes = expression.doc_classes
            self.doc_types = expression.doc_types
        elif query is None:
            expression = None
            doc_classes = None
            self.doc_types = None
        else:
            expression = {
                'query': query,
            }
            doc_classes = collect_doc_classes(query)
            self.doc_types = SearchQueryContext._get_unique_doc_types(
                doc_classes=doc_classes
            )
        super(CompiledSearchQuery, self).__init__(
            expression, params, doc_classes=doc_classes
        )

    def api_method(self, client):
        return client.search

    def prepare_params(self, params):
        if isinstance(self.expression, SearchQueryContext):
            search_params = dict(self.expression.search_params)
            search_params.update(params)
        else:
            search_params = params

        return self._patch_doc_type(search_params)

    def process_result(self, raw_result):
        return SearchResult(
            raw_result,
            aggregations=self.expression.aggregations,
            doc_cls_map=_mk_doc_cls_map(
                self.expression.doc_classes, self.features.requires_doc_type
            ),
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
    def get_filtered_query(
            cls, query_context, wrap_function_score=True, doc_classes=None
    ):
        q = cls.get_query(
            query_context, wrap_function_score=wrap_function_score
        )
        filter_clauses = []
        if query_context.filters:
            filter_clauses.extend(query_context.iter_filters())
        if doc_classes:
            doc_types = []
            for doc_cls in doc_classes:
                if _is_emulate_doc_types_mode(cls.features, doc_cls):
                    doc_types.append(doc_cls.get_doc_type())
            if doc_types:
                filter_clauses.append(
                    Terms(DOC_TYPE_JOIN_FIELD, doc_types)
                )
        if filter_clauses:
            if len(filter_clauses) == 1:
                return Bool(must=q, filter=filter_clauses[0])
            return Bool(must=q, filter=filter_clauses)
        return q

    @classmethod
    def get_post_filter(cls, query_context):
        post_filters = list(query_context.iter_post_filters())
        if post_filters:
            return Bool.must(*post_filters)

    def visit_search_query_context(self, query_ctx):
        params = {}

        q = self.get_filtered_query(query_ctx, doc_classes=self.doc_classes)
        if q is not None:
            params['query'] = self.visit(q)

        post_filter = self.get_post_filter(query_ctx)
        if post_filter:
            params['post_filter'] = self.visit(post_filter)
        if query_ctx.ext:
            params['ext'] = self.visit(query_ctx.ext)
        if query_ctx.order_by:
            params['sort'] = self.visit(query_ctx.order_by)
        if query_ctx.source:
            params['_source'] = self.visit(query_ctx.source)
        if query_ctx.fields:
            params['stored_fields'] = self.visit(
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
        if query_ctx.docvalue_fields:
            params['docvalue_fields'] = self.visit(
                query_ctx.docvalue_fields
            )
        if query_ctx.script_fields:
            params['script_fields'] = self.visit(
                query_ctx.script_fields
            )
        if (
            self.features.supports_track_total_hits and
            query_ctx.track_total_hits is not None
        ):
            params['track_total_hits'] = query_ctx.track_total_hits

        if query_ctx.search_after:
            params['search_after'] = query_ctx.search_after

        self._patch_docvalue_fields(params, self.doc_classes)
        return params

    def _patch_doc_type(self, search_params):
        should_use_default_type = self.doc_classes and any(map(
            lambda doc_cls: doc_cls.has_parent_doc_cls(),
            self.doc_classes
        ))
        if should_use_default_type:
            search_params['doc_type'] = DEFAULT_DOC_TYPE

        if not self.features.requires_doc_type:
            search_params.pop('doc_type', None)

        return search_params


class CompiledExplain(CompiledSearchQuery):
    def __init__(self, query, doc_or_id, params=None, doc_cls=None):
        if isinstance(doc_or_id, Document):
            self.doc_id = doc_or_id._id
            self.doc_cls = doc_or_id.__class__
            self.doc_type = self.doc_cls.get_doc_type()
            self.routing = self.doc_cls._routing
        else:
            self.doc_id = doc_or_id
            self.doc_cls = doc_cls
            self.doc_type = doc_cls.get_doc_type() if doc_cls else None
            self.routing = None
        self._source = None
        self._stored_fields = None
        super(CompiledExplain, self).__init__(query, params)

    def api_method(self, client):
        return client.explain

    def prepare_params(self, params):
        params['id'] = self.doc_id
        if self.features.requires_doc_type and not params.get('doc_type'):
            params['doc_type'] = self.doc_type
        if not params.get('routing') and self.routing:
            params['routing'] = self.routing
        if self._source:
            if self._source.fields:
                params['_source'] = self._source.fields
            if self._source.include:
                params['_source_includes'] = self._source.include
            if self._source.exclude:
                params['_source_excludes'] = self._source.exclude
        if self._stored_fields:
            params['stored_fields'] = self._stored_fields
        if _is_emulate_doc_types_mode(self.features, self.doc_cls):
            params['id'] = mk_uid(
                self.doc_cls.get_doc_type(), params['id']
            )
            if self.features.requires_doc_type:
                params['doc_type'] = DEFAULT_DOC_TYPE
            _patch_stored_fields_in_params(
                self.features, params, not _has_custom_source(params)
            )
        return params

    def process_result(self, raw_result):
        return ExplainResult(
            raw_result,
            doc_cls_map=_mk_doc_cls_map(
                self.doc_cls, self.features.requires_doc_type
            ),
            # Only store hit in a result when there were custom source or
            # stored fields
            _store_hit=bool(self._source or self._stored_fields)
        )

    def visit_search_query_context(self, query_ctx):
        body = {}

        q = self.get_filtered_query(query_ctx)
        if q is not None:
            body['query'] = self.visit(q)

        self._source = query_ctx.source
        self._stored_fields = self.visit(query_ctx.fields)

        return body


class CompiledScroll(CompiledEndpoint):
    def __init__(self, params, doc_cls=None, instance_mapper=None):
        self.doc_cls = doc_cls
        self.instance_mapper = instance_mapper
        super(CompiledScroll, self).__init__(None, params)

    def api_method(self, client):
        return client.scroll

    def process_result(self, raw_result):
        return SearchResult(
            raw_result,
            doc_cls_map=_mk_doc_cls_map(
                self.doc_cls, self.features.requires_doc_type
            ),
            instance_mapper=self.instance_mapper,
        )


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
    def __init__(self, query, params=None):
        super(CompiledExistsQuery, self).__init__(query, params)
        if self.body is None:
            self.body = {}
        self.body['size'] = 0
        self.body['terminate_after'] = 1

    def api_method(self, client):
        return client.search

    def process_result(self, raw_result):
        return ExistsResult({
            'exists': SearchResult(raw_result).total >= 1
        })


class CompiledDeleteByQuery(CompiledScalarQuery):
    def api_method(self, client):
        return client.delete_by_query

    def process_result(self, raw_result):
        return DeleteByQueryResult(raw_result)


class CompiledMultiSearch(CompiledEndpoint):
    compiled_search = None

    class _MultiQueries(object):
        __visit_name__ = 'multi_queries'

        def __init__(self, queries):
            self.queries = queries

        def __iter__(self):
            return iter(self.queries)

    def __init__(self, queries, params=None, raise_on_error=False):
        self.raise_on_error = raise_on_error
        self.compiled_queries = []
        super(CompiledMultiSearch, self).__init__(
            self._MultiQueries(queries), params
        )

    def api_method(self, client):
        return client.msearch

    def visit_multi_queries(self, expr):
        body = []
        for q in expr.queries:
            compiled_query = self.compiled_search(q)
            self.compiled_queries.append(compiled_query)
            params = compiled_query.params
            if isinstance(compiled_query.expression, SearchQueryContext):
                index = compiled_query.expression.index
                if index:
                    params['index'] = index.get_name()
            if 'doc_type' in params:
                params['type'] = params.pop('doc_type')
            body.append(params)
            body.append(compiled_query.body)
        return body

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


class CompiledPutMapping(CompiledEndpoint):
    class _MultipleMappings(object):
        __visit_name__ = 'multiple_mappings'

        def __init__(self, mappings):
            self.mappings = mappings

    def __init__(self, doc_cls_or_mapping, params=None, ordered=False):
        self._dict_type = OrderedDict if ordered else dict
        self._dynamic_templates = []
        if isinstance(doc_cls_or_mapping, list):
            doc_cls_or_mapping = self._MultipleMappings(doc_cls_or_mapping)
        super(CompiledPutMapping, self).__init__(doc_cls_or_mapping, params)

    def api_method(self, client):
        return client.indices.put_mapping

    def prepare_params(self, params):
        doc_type = params.get('doc_type')
        if (
                isinstance(self.expression, type) and
                issubclass(self.expression, Document) and
                not doc_type
        ):
            if _is_emulate_doc_types_mode(self.features, self.expression):
                params['doc_type'] = DEFAULT_DOC_TYPE
            else:
                params['doc_type'] = self.expression.get_doc_type()
        elif (
            isinstance(self.expression, self._MultipleMappings) or
            isinstance(self.expression, type) and
            issubclass(self.expression, Document) and
            self.expression.has_parent_doc_cls()
        ):
            params['doc_type'] = DEFAULT_DOC_TYPE

        if not self.features.requires_doc_type:
            params.pop('doc_type', None)

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
            if isinstance(field._fields, Mapping):
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

    @staticmethod
    def _get_parent_doc_type(doc_cls):
        doc_type = doc_cls.get_doc_type()
        if not doc_type:
            return None
        parent_doc_cls = doc_cls.get_parent_doc_cls()
        if parent_doc_cls is None:
            return None
        return parent_doc_cls.get_doc_type()

    @staticmethod
    def _merge_properties(mappings, properties):
        mapping_properties = mappings.setdefault('properties', {})
        for name, value in properties.items():
            existing_value = mapping_properties.get(name)
            if existing_value is not None and value != existing_value:
                raise ValueError('Conflicting mapping properties: {}'.format(
                    name
                ))
            mapping_properties[name] = value

    def visit_multiple_mappings(self, multiple_mappings):
        mappings = {}
        relations = {}
        for mapping_or_doc_cls in multiple_mappings.mappings:
            if issubclass(mapping_or_doc_cls, Document):
                doc_type = mapping_or_doc_cls.get_doc_type()
                parent_doc_type = self._get_parent_doc_type(
                    mapping_or_doc_cls
                )
                if doc_type and parent_doc_type:
                    relations.setdefault(parent_doc_type, []) \
                        .append(doc_type)

            mapping = self.visit(mapping_or_doc_cls)
            self._merge_properties(mappings, mapping['properties'])

        if relations:
            doc_type_property = mappings['properties'][DOC_TYPE_JOIN_FIELD]
            doc_type_property['relations'] = relations

        return mappings

    def visit_document(self, doc_cls):
        mapping = self._dict_type()
        mapping.update(doc_cls.__mapping_options__)
        mapping.update(self.visit(doc_cls.mapping_fields))
        properties = self.visit(doc_cls.user_fields)
        if _is_emulate_doc_types_mode(self.features, doc_cls):
            properties[DOC_TYPE_JOIN_FIELD] = {
                'type': 'join'
            }
            properties[DOC_TYPE_FIELD] = {
                'type': 'object',
                'properties': {
                    'name': {
                        'type': 'keyword',
                        'index': False,
                        'doc_values': False,
                        'store': True,
                    },
                    'parent': {
                        'type': 'keyword',
                        'index': False,
                        'doc_values': False,
                        'store': True,
                    }
                }
            }
        elif doc_cls.get_parent_doc_cls():
            mapping['_parent'] = {
                'type': doc_cls.get_parent_doc_cls().get_doc_type()
            }
        mapping['properties'] = properties
        for f in doc_cls.dynamic_fields:
            self._visit_dynamic_field(f)
        if self._dynamic_templates:
            mapping['dynamic_templates'] = self._dynamic_templates
        return mapping


class CompiledCreateIndex(CompiledEndpoint):
    compiled_put_mapping = None

    class _CreateIndex(object):
        __visit_name__ = 'create_index'

        def __init__(self, settings, mappings):
            self.settings = settings
            self.mappings = mappings

    def __init__(self, settings=None, mappings=None, params=None):
        super(CompiledCreateIndex, self).__init__(
            self._CreateIndex(settings, mappings), params
        )

    def api_method(self, client):
        return client.indices.create

    def process_result(self, raw_result):
        return raw_result

    def visit_create_index(self, create_index):
        body = {}
        if create_index.settings:
            body['settings'] = self.visit(create_index.settings)
        if create_index.mappings:
            compiled_mappings = self.compiled_put_mapping(
                create_index.mappings
            )
            if isinstance(create_index.mappings, (list, tuple)):
                doc_type = DEFAULT_DOC_TYPE
            else:
                doc_type = create_index.mappings.get_doc_type()

            if self.features.requires_doc_type:
                mappings = {
                    doc_type: compiled_mappings.body
                }
            else:
                mappings = compiled_mappings.body
            body['mappings'] = mappings

        return body


class CompiledGet(CompiledEndpoint):
    META_FIELDS = (
        ('_id', 'id'),
        ('_type', 'doc_type'),
        ('_routing', 'routing'),
        ('_parent', 'parent'),
        ('_version', 'version'),
    )

    def __init__(self, doc_or_id, params=None, doc_cls=None):
        self.doc_or_id = doc_or_id
        self.doc_cls = doc_cls or DynamicDocument
        super(CompiledGet, self).__init__(None, params)

    def api_method(self, client):
        return client.get

    def prepare_params(self, params):
        get_params = {}
        if isinstance(self.doc_or_id, Document):
            doc = self.doc_or_id
            for meta_field_name, param_name in self.META_FIELDS:
                field_value = getattr(doc, meta_field_name, None)
                if field_value is not None:
                    get_params[param_name] = field_value
            self.doc_cls = doc.__class__
        elif isinstance(self.doc_or_id, dict):
            doc = self.doc_or_id
            get_params.update(doc)
            if doc.get('doc_cls'):
                self.doc_cls = doc.pop('doc_cls')
        else:
            doc_id = self.doc_or_id
            get_params.update({'id': doc_id})

        if get_params.get('doc_type') is None:
            get_params['doc_type'] = getattr(
                self.doc_cls, '__doc_type__', None
            )
        get_params.update(params)

        if _is_emulate_doc_types_mode(self.features, self.doc_cls):
            get_params['id'] = mk_uid(
                self.doc_cls.get_doc_type(), get_params['id']
            )
            get_params['doc_type'] = DEFAULT_DOC_TYPE
            _patch_stored_fields_in_params(
                self.features, get_params,
                not get_params.get('stored_fields')
            )

        if not self.features.requires_doc_type:
            get_params.pop('doc_type', None)

        self._patch_source_include_exclude(get_params)

        return get_params

    def _patch_source_include_exclude(self, get_params):
        params = {}

        source_include = get_params.pop(
            '_source_includes',
            get_params.pop('_source_include', None)
        )
        if source_include is not None:
            params['_source_includes'] = source_include

        source_exclude = get_params.pop(
            '_source_excludes',
            get_params.pop('_source_exclude', None)
        )
        if source_exclude is not None:
            params['_source_excludes'] = source_exclude

        if params:
            get_params['params'] = params

    def process_result(self, raw_result):
        return self.doc_cls(_hit=raw_result)


class CompiledMultiGet(CompiledEndpoint):
    class _DocsOrIds(object):
        __visit_name__ = 'docs_or_ids'

        def __init__(self, docs_or_ids):
            self.docs_or_ids = docs_or_ids

        def __iter__(self):
            return iter(self.docs_or_ids)

    def __init__(self, docs_or_ids, params=None, doc_cls=None):
        default_doc_cls = doc_cls
        if isinstance(default_doc_cls, Iterable):
            self.doc_cls_map = {
                _doc_cls.__doc_type__: _doc_cls
                for _doc_cls in default_doc_cls
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
        super(CompiledMultiGet, self).__init__(
            self._DocsOrIds(docs_or_ids), params
        )

    def api_method(self, client):
        return client.mget

    def visit_docs_or_ids(self, docs_or_ids):
        docs = []
        for doc_or_id in docs_or_ids:
            if isinstance(doc_or_id, Document):
                doc = {
                    '_id': doc_or_id._id,
                }
                if doc_or_id._index:
                    doc['_index'] = doc_or_id._index
                if doc_or_id._version:
                    doc['_version'] = doc_or_id._version
                if doc_or_id._routing:
                    doc['routing'] = doc_or_id._routing
                doc_cls = doc_or_id.__class__
            elif isinstance(doc_or_id, dict):
                doc = doc_or_id
                doc_cls = doc_or_id.pop('doc_cls', None)
            else:
                doc = {'_id': doc_or_id}
                doc_cls = None

            if not doc.get('_type') and hasattr(doc_cls, '__doc_type__'):
                doc['_type'] = doc_cls.__doc_type__

            doc_cls = doc_cls or self.default_doc_cls
            if _is_emulate_doc_types_mode(self.features, doc_cls):
                doc['_id'] = mk_uid(doc_cls.get_doc_type(), doc['_id'])
                doc['_type'] = DEFAULT_DOC_TYPE
                doc['stored_fields'] = [
                    '_source', DOC_TYPE_NAME_FIELD, DOC_TYPE_PARENT_FIELD
                ]

            if not self.features.requires_doc_type:
                doc.pop('_type', None)

            docs.append(doc)
            self.doc_classes.append(doc_cls)

        return {'docs': docs}

    def process_result(self, raw_result):
        docs = []
        for doc_cls, raw_doc in zip(self.doc_classes, raw_result['docs']):
            doc_type = get_doc_type_for_hit(raw_doc)
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


class CompiledBulk(CompiledEndpoint):
    compiled_meta = None
    compiled_source = None

    class _Actions(object):
        __visit_name__ = 'actions'

        def __init__(self, actions):
            self.actions = actions

        def __iter__(self):
            return iter(self.actions)

    def __init__(self, actions, params=None):
        super(CompiledBulk, self).__init__(self._Actions(actions), params)

    def api_method(self, client):
        return client.bulk

    def visit_actions(self, actions):
        body = []
        for action in actions:
            meta = self.compiled_meta(action).body
            body.append(meta)
            source = self.compiled_source(action).body
            if source is not None:
                body.append(source)
        return body

    def process_result(self, raw_result):
        return BulkResult(raw_result)


class CompiledMeta(Compiled):
    META_SPECIAL_PARAMS = {'_id', '_index', '_type'}
    META_FIELD_NAMES = (
        '_id',
        '_index',
        '_type',
        'routing',
        'parent',
        'timestamp',
        'ttl',
        'version',
    )

    def __init__(self, doc_or_action):
        super(CompiledMeta, self).__init__(doc_or_action)

    def visit_action(self, action):
        action_meta = self.visit_document(action.doc)
        action_meta.update(action.meta_params)
        if self.features.bulk_update_underscore_retry_on_conflict:
            retry_on_conflict = action_meta.pop('retry_on_conflict', None)
            if retry_on_conflict is not None:
                action_meta['_retry_on_conflict'] = retry_on_conflict
        return {
            action.__action_name__: action_meta
        }

    def visit_document(self, doc):
        meta = {}
        if isinstance(doc, Document):
            self._populate_meta_from_document(doc, meta)
            doc_type = doc.get_doc_type()
            if doc_type:
                meta['_type'] = doc_type
        else:
            self._populate_meta_from_dict(doc, meta)

        if _is_emulate_doc_types_mode(self.features, doc.__class__):
            meta.pop('parent', None)
            meta['_id'] = mk_uid(
                doc.__doc_type__, meta['_id']
            )
            meta['_type'] = DEFAULT_DOC_TYPE

        if not self.features.requires_doc_type:
            meta.pop('_type', None)

        return meta

    def _populate_meta_from_document(self, doc, meta):
        for field_name in self.META_FIELD_NAMES:
            if field_name.startswith('_'):
                doc_field_name = field_name
            else:
                doc_field_name = '_{}'.format(field_name)
            value = getattr(doc, doc_field_name, None)
            if value:
                meta[field_name] = value

    def _populate_meta_from_dict(self, doc, meta):
        for field_name in self.META_FIELD_NAMES:
            if field_name.startswith('_'):
                doc_field_name = field_name
            else:
                doc_field_name = '_{}'.format(field_name)
            value = doc.get(doc_field_name)
            if value:
                meta[field_name] = value


class CompiledSource(CompiledExpression):
    def __init__(self, doc_or_action, validate=False):
        self._validate = validate
        super(CompiledSource, self).__init__(doc_or_action)

    def visit_action(self, action):
        if action.__action_name__ == 'delete':
            return None

        if isinstance(action.doc, Document):
            doc = self.visit(action.doc)
        else:
            doc = action.doc.copy()
            for exclude_field in Document.mapping_fields:
                doc.pop(exclude_field.get_field().get_name(), None)

        if action.__action_name__ == 'update':
            script = action.source_params.pop('script', None)
            if script:
                source = {'script': self.visit(script)}
            else:
                source = {'doc': doc}
            source.update(self.visit(action.source_params))
        else:
            source = doc

        return source

    def visit_document(self, doc):
        source = {}
        doc_cls = doc.__class__
        for key, value in doc.__dict__.items():
            # skip mapping fields
            if key in doc_cls.mapping_fields:
                continue

            # ignore private attributes
            if key.startswith('_Document__'):
                continue

            attr_field = doc_cls.fields.get(key)
            if not attr_field:
                continue

            if value is None or value == '' or value == []:
                if (
                        self._validate and
                        attr_field.get_field().get_mapping_options().get(
                            'required'
                        )
                ):
                    raise ValidationError("'{}' is required".format(
                        attr_field.get_attr_name()
                    ))
            else:
                value = attr_field.get_type() \
                    .from_python(value, self.compiler, validate=self._validate)
            source[attr_field.get_field().get_name()] = value

        for attr_field in doc._fields.values():
            if not self._validate:
                continue

            field = attr_field.get_field()
            if (
                    field.get_mapping_options().get('required') and
                    field.get_name() not in source
            ):
                raise ValidationError(
                    "'{}' is required".format(attr_field.get_attr_name())
                )

        if _is_emulate_doc_types_mode(self.features, doc):
            doc_type_source = {}
            doc_type_source['name'] = doc.__doc_type__
            if doc._parent is not None:
                doc_type_source['parent'] = mk_uid(
                    doc.__parent__.__doc_type__,
                    doc._parent
                )
            source[DOC_TYPE_FIELD] = doc_type_source
            source[DOC_TYPE_JOIN_FIELD] = doc_type_source

        return source


def _featured_compiler(elasticsearch_features):
    def inject_features(cls):
        class _CompiledExpression(CompiledExpression):
            compiler = cls
            features = elasticsearch_features

        class _CompiledSearchQuery(CompiledSearchQuery):
            compiler = cls
            features = elasticsearch_features

        class _CompiledScroll(CompiledScroll):
            compiler = cls
            features = elasticsearch_features

        class _CompiledCountQuery(CompiledCountQuery):
            compiler = cls
            features = elasticsearch_features

        class _CompiledExistsQuery(CompiledExistsQuery):
            compiler = cls
            features = elasticsearch_features

        class _CompiledExplain(CompiledExplain):
            compiler = cls
            features = elasticsearch_features

        class _CompiledDeleteByQuery(CompiledDeleteByQuery):
            compiler = cls
            features = elasticsearch_features

        class _CompiledMultiSearch(CompiledMultiSearch):
            compiler = cls
            features = elasticsearch_features
            compiled_search = _CompiledSearchQuery

        class _CompiledGet(CompiledGet):
            compiler = cls
            features = elasticsearch_features

        class _CompiledMultiGet(CompiledMultiGet):
            compiler = cls
            features = elasticsearch_features

        class _CompiledDelete(CompiledDelete):
            compiler = cls
            features = elasticsearch_features

        class _CompiledMeta(CompiledMeta):
            compiler = cls
            features = elasticsearch_features

        class _CompiledSource(CompiledSource):
            compiler = cls
            features = elasticsearch_features

        class _CompiledBulk(CompiledBulk):
            compiler = cls
            features = elasticsearch_features
            compiled_meta = _CompiledMeta
            compiled_source = _CompiledSource

        class _CompiledPutMapping(CompiledPutMapping):
            compiler = cls
            features = elasticsearch_features

        class _CompiledCreateIndex(CompiledCreateIndex):
            compiler = cls
            features = elasticsearch_features
            compiled_put_mapping = _CompiledPutMapping

        cls.features = elasticsearch_features
        cls.compiled_expression = _CompiledExpression
        cls.compiled_search_query = _CompiledSearchQuery
        cls.compiled_query = cls.compiled_search_query
        cls.compiled_scroll = _CompiledScroll
        cls.compiled_count_query = _CompiledCountQuery
        cls.compiled_exists_query = _CompiledExistsQuery
        cls.compiled_explain = _CompiledExplain
        cls.compiled_delete_by_query = _CompiledDeleteByQuery
        cls.compiled_multi_search = _CompiledMultiSearch
        cls.compiled_get = _CompiledGet
        cls.compiled_multi_get = _CompiledMultiGet
        cls.compiled_delete = _CompiledDelete
        cls.compiled_bulk = _CompiledBulk
        cls.compiled_put_mapping = _CompiledPutMapping
        cls.compiled_create_index = _CompiledCreateIndex
        return cls

    return inject_features


@_featured_compiler(
    ElasticsearchFeatures(
        requires_doc_type=True,
        supports_track_total_hits=False,
        bulk_update_underscore_retry_on_conflict=True,
    )
)
class Compiler_6_0(object):
    pass


@_featured_compiler(
    ElasticsearchFeatures(
        requires_doc_type=False,
        supports_track_total_hits=True,
        bulk_update_underscore_retry_on_conflict=False,
    )
)
class Compiler_7_0(object):
    pass


def get_compiler_by_es_version(es_version):
    if es_version.distribution == "opensearch":
        if es_version.major < 2:
            raise ValueError(f"Unsuported Opensearch version: {es_version}")
        else:
            return Compiler_7_0
    else:
        if es_version.major < 6:
            raise ValueError(f"Unsuported Elasticsearch version: {es_version}")
        elif es_version.major == 6:
            return Compiler_6_0
        elif es_version.major == 7:
            return Compiler_7_0
        else:
            return Compiler_7_0


all_compilers = [
    Compiler_6_0,
    Compiler_7_0,
]
