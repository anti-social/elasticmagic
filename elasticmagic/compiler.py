import operator
import collections

from .compat import string_types
from .document import Document, DOC_TYPE_FIELD_NAME, TYPE_ID_DELIMITER
from .expression import Bool
from .expression import Exists
from .expression import Filtered
from .expression import FunctionScore
from .expression import HighlightedField
from .expression import Params
from .types import ValidationError


OPERATORS = {
    operator.and_: 'and',
    operator.or_: 'or',
}

ESVersion = collections.namedtuple('ESVersion', ['major', 'minor', 'patch'])


class CompilationError(Exception):
    pass


class Compiled(object):
    def __init__(self, expression):
        self.expression = expression
        self.params = self.visit(self.expression)

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


class ExpressionCompiled(Compiled):
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

    def visit_ids(self, expr):
        params = self.visit(expr.params)
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
        return {
            'missing': self.visit(expr.params)
        }

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

    def visit_has_parent(self, expr):
        params = self.visit(expr.params)
        parent_type = expr.parent_type
        if parent_type and parent_type.__doc_type__:
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
        if child_type and child_type.__doc_type__:
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


class ExpressionCompiled50(ExpressionCompiled):
    def visit_missing(self, expr):
        return self.visit(
            Bool.must_not(Exists(**expr.params))
        )

    def visit_parent_id(self, expr):
        child_type = expr.child_type
        if child_type.__doc_type__:
            child_type = child_type.__doc_type__
        if not child_type:
            raise CompilationError(
                "Cannot detect child type, specify 'child_type' argument")

        return {'parent_id': {'type': child_type, 'id': expr.parent_id}}


class ExpressionCompiled60(ExpressionCompiled50):
    def visit_ids(self, expr):
        params = self.visit(expr.params)
        if (
                expr.type and
                hasattr(expr.type, '__parent__')
        ):
            doc_type = expr.type.__doc_type__
            values = [
                '{}{}{}'.format(doc_type, TYPE_ID_DELIMITER, v)
                for v in expr.values
            ]
            params['values'] = values
        else:
            if expr.type:
                params['type'] = expr.type
            params['values'] = expr.values
        return {
            'ids': params
        }

    def visit_parent_id(self, expr):
        if hasattr(expr.child_type, '__parent__'):
            parent_id = '{}{}{}'.format(
                expr.child_type.__parent__.__doc_type__,
                TYPE_ID_DELIMITER,
                expr.parent_id
            )
        else:
            parent_id = expr.parent_id

        child_type = expr.child_type
        if child_type.__doc_type__:
            child_type = child_type.__doc_type__
        if not child_type:
            raise CompilationError(
                "Cannot detect child type, specify 'child_type' argument")

        return {'parent_id': {'type': child_type, 'id': parent_id}}


class QueryCompiled(Compiled):
    compiled_expression = ExpressionCompiled

    @classmethod
    def get_query(cls, query_context, wrap_function_score=True):
        q = query_context.q
        if wrap_function_score:
            if query_context.function_score:
                q = FunctionScore(
                    query=q,
                    functions=query_context.function_score,
                    **query_context.function_score_params
                )
            if query_context.boost_score:
                boost_score_params = Params(
                    dict(
                        score_mode='sum',
                        boost_mode='sum',
                    ),
                    **query_context.boost_score_params
                )
                q = FunctionScore(
                    query=q,
                    functions=query_context.boost_score,
                    **boost_score_params
                )
        return q

    @classmethod
    def get_filtered_query(cls, query_context, wrap_function_score=True):
        q = cls.get_query(
            query_context, wrap_function_score=wrap_function_score
        )
        if query_context.filters:
            return Filtered(
                query=q, filter=Bool.must(*query_context.iter_filters())
            )
        return q

    @classmethod
    def get_post_filter(self, query_context):
        post_filters = list(query_context.iter_post_filters())
        if post_filters:
            return Bool.must(*post_filters)

    def visit_expression(self, expr):
        return self.compiled_expression(expr).params

    def visit_search_query(self, query):
        params = {}
        query_ctx = query.get_context()

        q = self.get_filtered_query(query_ctx)
        if q is not None:
            params['query'] = self.visit_expression(q)

        post_filter = self.get_post_filter(query_ctx)
        if post_filter:
            params['post_filter'] = self.visit_expression(post_filter)

        if query_ctx.order_by:
            params['sort'] = self.visit_expression(query_ctx.order_by)
        if query_ctx.source:
            params['_source'] = self.visit_expression(query_ctx.source)
        if query_ctx.fields is not None:
            if query_ctx.fields is True:
                params['fields'] = '*'
            elif query_ctx.fields is False:
                params['fields'] = []
            else:
                params['fields'] = self.visit_expression(query_ctx.fields)
        if query_ctx.aggregations:
            params['aggregations'] = self.visit_expression(
                query_ctx.aggregations)
        if query_ctx.limit is not None:
            params['size'] = query_ctx.limit
        if query_ctx.offset is not None:
            params['from'] = query_ctx.offset
        if query_ctx.min_score is not None:
            params['min_score'] = query_ctx.min_score
        if query_ctx.rescores:
            params['rescore'] = self.visit_expression(query_ctx.rescores)
        if query_ctx.suggest:
            params['suggest'] = self.visit_expression(query_ctx.suggest)
        if query_ctx.highlight:
            params['highlight'] = self.visit_expression(query_ctx.highlight)
        if query_ctx.script_fields:
            params['script_fields'] = self.visit_expression(
                query_ctx.script_fields
            )
        return params


class QueryCompiled20(QueryCompiled):
    @classmethod
    def get_filtered_query(cls, query_context, wrap_function_score=True):
        q = cls.get_query(
            query_context, wrap_function_score=wrap_function_score)
        if query_context.filters:
            return Bool(
                must=q, filter=Bool.must(*query_context.iter_filters())
            )
        return q


class QueryCompiled50(QueryCompiled20):
    compiled_expression = ExpressionCompiled50

    def visit_search_query(self, query):
        params = super(QueryCompiled50, self).visit_search_query(query)
        stored_fields = params.pop('fields', None)
        if stored_fields:
            params['stored_fields'] = stored_fields
        return params


class QueryCompiled60(QueryCompiled50):
    compiled_expression = ExpressionCompiled60

    def visit_search_query(self, query):
        params = super(QueryCompiled60, self).visit_search_query(query)
        source = params.get('_source')
        if not source:
            params['_source'] = [DOC_TYPE_FIELD_NAME]
        elif isinstance(source, string_types):
            params['_source'] = [source, DOC_TYPE_FIELD_NAME]
        elif isinstance(source, list):
            source.append(DOC_TYPE_FIELD_NAME)
        elif isinstance(source, dict):
            includes = source.get('includes')
            if not includes:
                source['includes'] = [DOC_TYPE_FIELD_NAME]
            elif isinstance(includes, list):
                includes.append(DOC_TYPE_FIELD_NAME)
        return params


class MappingCompiled10(Compiled):
    def __init__(self, expression, ordered=False):
        self._dict_type = collections.OrderedDict if ordered else dict
        self._dynamic_templates = []
        super(MappingCompiled10, self).__init__(expression)

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


class MappingCompiled60(MappingCompiled10):
    def visit_document(self, doc_cls):
        mapping = self._dict_type()
        mapping.update(doc_cls.__mapping_options__)
        mapping.update(self.visit(doc_cls.mapping_fields))
        properties = self.visit(doc_cls.user_fields)
        if doc_cls.__doc_type__ and hasattr(doc_cls, '__parent__'):
            properties[DOC_TYPE_FIELD_NAME] = {'type': 'join'}
        mapping['properties'] = properties
        for f in doc_cls.dynamic_fields:
            self._visit_dynamic_field(f)
        if self._dynamic_templates:
            mapping['dynamic_templates'] = self._dynamic_templates
        return mapping


class MetaCompiled10(Compiled):
    META_FIELD_NAMES = {
        '_id',
        '_index',
        '_type',
        '_routing',
        '_parent',
        '_timestamp',
        '_ttl',
        '_version',
    }

    def __init__(self, document):
        super(MetaCompiled10, self).__init__(document)

    def visit_document(self, doc):
        meta = {}
        if doc.__doc_type__:
            meta['_type'] = doc.__doc_type__
        self._populate_meta_from_document(doc, meta)
        return meta

    def visit_action(self, action):
        if isinstance(action.doc, Document):
            meta = self.visit_document(action.doc)
        else:
            meta = {}
            self._populate_meta_from_dict(action.doc, meta)
        meta.update(action.params)
        return {action.__action_name__: meta}

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


class MetaCompiled60(MetaCompiled10):
    META_FIELD_NAMES = {
        '_id',
        '_index',
        '_routing',
        '_timestamp',
        '_ttl',
        '_version',
    }
    DEFAULT_TYPE = '_doc'

    def __init__(self, document):
        super(MetaCompiled60, self).__init__(document)

    def visit_document(self, doc):
        meta = {}
        self._populate_meta_from_document(doc, meta)
        if doc.__doc_type__:
            meta['_type'] = doc.__doc_type__
        if doc.__doc_type__ and hasattr(doc, '__parent__') and '_id' in meta:
            meta['_id'] = '{}{}{}'.format(
                doc.__doc_type__, TYPE_ID_DELIMITER, meta['_id']
            )
            meta['_type'] = self.DEFAULT_TYPE
        return meta


class SourceCompiled10(Compiled):
    def __init__(self, document, validate=False):
        self._validate = validate
        super(SourceCompiled10, self).__init__(document)

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
                        attr_field.get_field()._mapping_options.get('required')
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

    def visit_action(self, action):
        if action.__action_name__ == 'delete':
            return None

        if isinstance(action.doc, Document):
            source = self.visit_document(action.doc)
        else:
            source = action.doc.copy()
            for exclude_field in Document.mapping_fields:
                source.pop(exclude_field.get_field().get_name(), None)

        if action.__action_name__ == 'update':
            if source:
                source = {'doc': source}
            source.update(action.source_params)

        return source


class SourceCompiled60(SourceCompiled10):
    def visit_document(self, doc):
        source = super(SourceCompiled60, self).visit_document(doc)
        if doc.__doc_type__ and hasattr(doc, '__parent__'):
            doc_type = {'name': doc.__doc_type__}
            if doc._parent is not None:
                doc_type['parent'] = '{}{}{}'.format(
                    doc.__parent__.__doc_type__, TYPE_ID_DELIMITER, doc._parent
                )
            source[DOC_TYPE_FIELD_NAME] = doc_type
        return source


class Compiler(object):
    def compiled_expression(self, *args, **kwargs):
        raise NotImplementedError()

    def compiled_query(self, *args, **kwargs):
        raise NotImplementedError()

    def compiled_mapping(self, *args, **kwargs):
        raise NotImplementedError()

    def compiled_meta(self, *args, **kwargs):
        raise NotImplementedError()

    def compiled_source(self, *args, **kwargs):
        raise NotImplementedError()


class Compiler10(Compiler):
    compiled_expression = QueryCompiled.compiled_expression
    compiled_query = QueryCompiled
    compiled_mapping = MappingCompiled10
    compiled_meta = MetaCompiled10
    compiled_source = SourceCompiled10


class Compiler20(Compiler):
    compiled_expression = QueryCompiled20.compiled_expression
    compiled_query = QueryCompiled20
    compiled_mapping = MappingCompiled10
    compiled_meta = MetaCompiled10
    compiled_source = SourceCompiled10


class Compiler50(Compiler):
    compiled_expression = QueryCompiled50.compiled_expression
    compiled_query = QueryCompiled50
    compiled_mapping = MappingCompiled10
    compiled_meta = MetaCompiled10
    compiled_source = SourceCompiled10


class Compiler60(Compiler):
    compiled_query = QueryCompiled60
    compiled_expression = compiled_query.compiled_expression
    compiled_mapping = MappingCompiled60
    compiled_meta = MetaCompiled60
    compiled_source = SourceCompiled60


DefaultCompiler = Compiler60


def get_compiler_by_es_version(es_version):
    if es_version.major <= 1:
        return Compiler10
    elif es_version.major == 2:
        return Compiler20
    elif es_version.major == 5:
        return Compiler50
    return DefaultCompiler
