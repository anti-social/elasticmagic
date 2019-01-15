from __future__ import absolute_import
import inspect
import operator
import collections
from itertools import count

from .util import clean_params, collect_doc_classes
from .types import instantiate, Type
from .compat import string_types


class Expression(object):
    def _collect_doc_classes(self):
        return set()

    def compile(self, compiler=None):
        from .compiler import DefaultCompiler

        compiler = compiler or DefaultCompiler
        return compiler.compiled_expression(self)

    def to_elastic(self, compiler=None):
        return self.compile(compiler=compiler).params

    to_dict = to_elastic


class Literal(object):
    __visit_name__ = 'literal'

    def __init__(self, obj):
        self.obj = obj


class Params(Expression, collections.Mapping):
    __visit_name__ = 'params'

    def __init__(self, *args, **kwargs):
        params = {}
        for d in args:
            params.update(d)
        params.update(kwargs)
        self._params = {}
        for k, v in clean_params(params).items():
            if k.endswith('_') and not k.startswith('_'):
                k = k.rstrip('_')
            self._params[k] = v

    def _collect_doc_classes(self):
        return collect_doc_classes(self._params)

    def __len__(self):
        return len(self._params)

    def __iter__(self):
        return iter(self._params)

    def __getitem__(self, key):
        return self._params[key]

    def __contains__(self, key):
        return key in self._params


class ParamsExpression(Expression):
    def __init__(self, **kwargs):
        super(ParamsExpression, self).__init__()
        self.params = Params(kwargs)

    def _collect_doc_classes(self):
        return collect_doc_classes(self.params)


class QueryExpression(ParamsExpression):
    __visit_name__ = 'query_expression'

    def __invert__(self):
        return Bool.must_not(self)


class FieldExpression(QueryExpression):
    __visit_name__ = 'field_expression'

    def __init__(self, field, **kwargs):
        super(FieldExpression, self).__init__(**kwargs)
        self.field = _wrap_literal(field)

    def _collect_doc_classes(self):
        return set().union(
            super(FieldExpression, self)._collect_doc_classes(),
            collect_doc_classes(self.field),
        )


class FieldQueryExpression(FieldExpression):
    __visit_name__ = 'field_query'
    __query_key__ = 'query'

    def __init__(self, field, query, **kwargs):
        super(FieldQueryExpression, self).__init__(field, **kwargs)
        self.query = query

    def _collect_doc_classes(self):
        return set().union(
            super(FieldQueryExpression, self)._collect_doc_classes(),
            collect_doc_classes(self.query),
        )


class Term(FieldQueryExpression):
    __query_name__ = 'term'
    __query_key__ = 'value'

    def __init__(self, field, value, boost=None, **kwargs):
        super(Term, self).__init__(field, value, boost=boost, **kwargs)


class Terms(FieldExpression):
    __visit_name__ = 'terms'

    def __init__(
            self, field, terms, minimum_should_match=None, boost=None,
            **kwargs
    ):
        super(Terms, self).__init__(
            field, minimum_should_match=minimum_should_match, boost=boost,
            **kwargs
        )
        self.terms = list(terms)


class Match(FieldQueryExpression):
    __query_name__ = 'match'

    def __init__(
            self, field, query,
            type=None, analyzer=None, boost=None,
            operator=None, minimum_should_match=None,
            fuzziness=None, prefix_length=None, max_expansions=None,
            zero_terms_query=None, cutoff_frequency=None, lenient=None,
            **kwargs
    ):
        super(Match, self).__init__(
            field, query,
            type=type, analyzer=analyzer, boost=boost,
            operator=operator, minimum_should_match=minimum_should_match,
            fuzziness=fuzziness, prefix_length=prefix_length,
            max_expansions=max_expansions, zero_terms_query=zero_terms_query,
            cutoff_frequency=cutoff_frequency, lenient=lenient, **kwargs
        )


class MultiMatch(QueryExpression):
    __visit_name__ = 'multi_match'

    def __init__(
            self, query, fields,
            type=None, analyzer=None, boost=None, operator=None,
            minimum_should_match=None, fuzziness=None, prefix_length=None,
            max_expansions=None, rewrite=None, zero_terms_query=None,
            cutoff_frequency=None, tie_breaker=None, **kwargs
    ):
        super(MultiMatch, self).__init__(
            type=type, analyzer=analyzer, boost=boost, operator=operator,
            minimum_should_match=minimum_should_match, fuzziness=fuzziness,
            prefix_length=prefix_length, max_expansions=max_expansions,
            rewrite=rewrite, zero_terms_query=zero_terms_query,
            cutoff_frequency=cutoff_frequency, tie_breaker=tie_breaker,
            **kwargs
        )
        self.query = query
        self.fields = fields

    def _collect_doc_classes(self):
        return set().union(
            super(MultiMatch, self)._collect_doc_classes(),
            collect_doc_classes(self.fields),
        )


class MatchAll(QueryExpression):
    __visit_name__ = 'match_all'

    def __init__(self, boost=None, **kwargs):
        super(MatchAll, self).__init__(boost=boost, **kwargs)


class Bool(QueryExpression):
    __query_name__ = 'bool'

    def __init__(
            self, must=None, filter=None, must_not=None, should=None,
            minimum_should_match=None, boost=None, disable_coord=None,
            **kwargs
    ):
        super(Bool, self).__init__(
            must=must, filter=filter, must_not=must_not, should=should,
            minimum_should_match=minimum_should_match,
            boost=boost, disable_coord=disable_coord, **kwargs)

    @classmethod
    def must(cls, *expressions):
        if len(expressions) == 1:
            return expressions[0]
        return cls(must=expressions)

    @classmethod
    def must_not(cls, *expressions):
        return cls(must_not=expressions)

    @classmethod
    def should(cls, *expressions):
        if len(expressions) == 1:
            return expressions[0]
        return cls(should=expressions)


class Boosting(QueryExpression):
    __query_name__ = 'boosting'

    def __init__(
            self, positive=None, negative=None, negative_boost=None,
            boost=None, **kwargs
    ):
        super(Boosting, self).__init__(
            positive=positive, negative=negative,
            negative_boost=negative_boost, boost=boost, **kwargs
        )


class Common(FieldQueryExpression):
    __query_name__ = 'common'

    def __init__(
            self, field, query,
            cutoff_frequency=None, minimum_should_match=None,
            high_freq_operator=None, low_freq_operator=None,
            boost=None, analyzer=None, disable_coord=None, **kwargs
    ):
        super(Common, self).__init__(
            field, query,
            cutoff_frequency=cutoff_frequency,
            minimum_should_match=minimum_should_match,
            high_freq_operator=high_freq_operator,
            low_freq_operator=low_freq_operator,
            boost=boost, analyzer=analyzer, disable_coord=disable_coord,
            **kwargs
        )


class ConstantScore(QueryExpression):
    __query_name__ = 'constant_score'

    def __init__(self, query=None, filter=None, boost=None, **kwargs):
        super(ConstantScore, self).__init__(
            filter=filter, query=query, boost=boost, **kwargs)


class FunctionScore(QueryExpression):
    __query_name__ = 'function_score'

    def __init__(
            self, query=None, filter=None, boost=None,
            script_score=None, boost_factor=None, random_score=None,
            field_value_factor=None, linear=None, exp=None, gauss=None,
            functions=None, max_boost=None, score_mode=None, boost_mode=None,
            **kwargs
    ):
        super(FunctionScore, self).__init__(
            query=query, filter=filter, boost=boost,
            script_score=script_score, boost_factor=boost_factor,
            random_score=random_score, field_value_factor=field_value_factor,
            linear=linear, exp=exp, gauss=gauss, functions=functions,
            max_boost=max_boost, score_mode=score_mode, boost_mode=boost_mode,
            **kwargs
        )


class DisMax(QueryExpression):
    __query_name__ = 'dis_max'

    def __init__(self, queries=None, boost=None, tie_breaker=None, **kwargs):
        super(DisMax, self).__init__(
            queries=queries, boost=boost, tie_breaker=tie_breaker, **kwargs
        )


class Filtered(QueryExpression):
    __query_name__ = 'filtered'

    def __init__(self, filter=None, query=None, strategy=None, **kwargs):
        super(Filtered, self).__init__(
            filter=filter, query=query, strategy=strategy, **kwargs)


class Ids(QueryExpression):
    __query_name__ = 'ids'

    def __init__(self, values, type=None, **kwargs):
        super(Ids, self).__init__(values=values, type=type, **kwargs)


class Range(FieldExpression):
    __visit_name__ = 'range'

    def __init__(self, field, gte=None, gt=None, lte=None, lt=None,
                 from_=None, to=None, include_lower=None, include_upper=None,
                 boost=None, time_zone=None, format=None, execution=None,
                 _name=None, _cache=None, _cache_key=None, **kwargs):
        super(Range, self).__init__(
            field, gte=gte, gt=gt, lte=lte, lt=lt,
            from_=from_, to=to, include_lower=include_lower,
            include_upper=include_upper, boost=boost, time_zone=time_zone,
            format=format,
        )
        self.range_params = Params(
            execution=execution, _name=_name, _cache=_cache,
            _cache_key=_cache_key, **kwargs
        )


class Prefix(FieldQueryExpression):
    __query_name__ = 'prefix'
    __query_key__ = 'value'

    def __init__(self, field, value, boost=None, **kwargs):
        super(Prefix, self).__init__(field, value, boost=boost, **kwargs)


class Query(QueryExpression):
    __visit_name__ = 'query'

    def __init__(self, query, **kwargs):
        super(Query, self).__init__(**kwargs)
        self.query = query

    def _collect_doc_classes(self):
        return set().union(
            super(Query, self)._collect_doc_classes(),
            collect_doc_classes(self.query),
        )


class BooleanExpression(QueryExpression):
    __visit_name__ = 'boolean_expression'

    def __init__(self, *args, **kwargs):
        raise NotImplementedError('Use and_ & or_ class methods instead')

    @classmethod
    def _construct(cls, operator, *expressions, **kwargs):
        if len(expressions) == 1:
            return expressions[0]

        self = cls.__new__(cls)
        super(BooleanExpression, self).__init__(**kwargs)
        self.operator = operator
        self.expressions = expressions
        return self

    def _collect_doc_classes(self):
        return set().union(
            super(BooleanExpression, self)._collect_doc_classes(),
            collect_doc_classes(self.expressions),
        )

    @classmethod
    def and_(cls, *expressions, **kwargs):
        return cls._construct(operator.and_, *expressions, **kwargs)

    @classmethod
    def or_(cls, *expressions, **kwargs):
        return cls._construct(operator.or_, *expressions, **kwargs)


And = BooleanExpression.and_
Or = BooleanExpression.or_


class Not(QueryExpression):
    __visit_name__ = 'not'

    def __init__(self, expr, **kwargs):
        super(Not, self).__init__(**kwargs)
        self.expr = expr

    def _collect_doc_classes(self):
        return set().union(
            super(Not, self)._collect_doc_classes(),
            collect_doc_classes(self.expr),
        )


class Exists(QueryExpression):
    __query_name__ = 'exists'

    def __init__(self, field, **kwargs):
        super(Exists, self).__init__(field=field, **kwargs)


class Missing(QueryExpression):
    __visit_name__ = 'missing'

    def __init__(self, field, **kwargs):
        super(Missing, self).__init__(field=field, **kwargs)


class Limit(QueryExpression):
    __query_name__ = 'limit'

    def __init__(self, value, **kwargs):
        super(Limit, self).__init__(value=value, **kwargs)


class Sort(QueryExpression):
    __visit_name__ = 'sort'

    def __init__(
            self, expr, order=None, mode=None,
            nested_path=None, nested_filter=None,
            missing=None, ignore_unmapped=None, **kwargs
    ):
        super(Sort, self).__init__(
            mode=mode,
            nested_path=nested_path, nested_filter=nested_filter,
            missing=missing, ignore_unmapped=ignore_unmapped, **kwargs
        )
        self.expr = expr
        self.order = order

    def _collect_doc_classes(self):
        return set().union(
            super(Sort, self)._collect_doc_classes(),
            collect_doc_classes(self.expr),
        )


class FieldOperators(object):
    def __eq__(self, other):
        if other is None:
            return self.missing()
        return self.term(other)

    def __ne__(self, other):
        if other is None:
            return self.exists()
        return Bool.must_not(operator.eq(self, other))

    def __gt__(self, other):
        return Range(self, gt=other)

    def __ge__(self, other):
        return Range(self, gte=other)

    def __lt__(self, other):
        return Range(self, lt=other)

    def __le__(self, other):
        return Range(self, lte=other)

    def term(self, term, **kwargs):
        return Term(self, term, **kwargs)

    def span_term(self, term, **kwargs):
        return SpanTerm(self, term, **kwargs)

    def span_first(self, term, end, **kwargs):
        return SpanFirst(self.span_term(term), end, **kwargs)

    def exists(self, **kwargs):
        return Exists(self, **kwargs)

    def missing(self, **kwargs):
        return Missing(self, **kwargs)

    def in_(self, terms, **kwargs):
        return Terms(self, terms, **kwargs)

    def not_in_(self, terms, **kwargs):
        return Bool.must_not(Terms(self, terms, **kwargs))

    def match(self, query, **kwargs):
        return Match(self, query, **kwargs)

    def range(self, gte=None, lte=None, gt=None, lt=None, **kwargs):
        return Range(self, gte=gte, lte=lte, gt=gt, lt=lt, **kwargs)

    def asc(
            self, mode=None, missing=None, nested_path=None,
            nested_filter=None, ignore_unmapped=None, **kwargs
    ):
        return Sort(
            self, 'asc', mode=mode,
            nested_path=nested_path, nested_filter=nested_filter,
            missing=missing, ignore_unmapped=ignore_unmapped, **kwargs
        )

    def desc(
            self, mode=None, missing=None, nested_path=None,
            nested_filter=None, ignore_unmapped=None, **kwargs
    ):
        return Sort(
            self, 'desc', mode=mode,
            nested_path=nested_path, nested_filter=nested_filter,
            missing=missing, ignore_unmapped=ignore_unmapped, **kwargs
        )

    def boost(self, weight):
        return BoostExpression(self, weight)

    def highlight(
            self, type=None, pre_tags=None, post_tags=None,
            fragment_size=None, number_of_fragments=None, order=None,
            encoder=None, require_field_match=None, boundary_max_scan=None,
            highlight_query=None, matched_fields=None, fragment_offset=None,
            no_match_size=None, phrase_limit=None,
            **kwargs
    ):
        return HighlightedField(
            self, type=type,
            pre_tags=pre_tags,
            post_tags=post_tags,
            fragment_size=fragment_size,
            number_of_fragments=number_of_fragments,
            order=order,
            encoder=encoder,
            require_field_match=require_field_match,
            boundary_max_scan=boundary_max_scan,
            highlight_query=highlight_query,
            matched_fields=matched_fields,
            fragment_offset=fragment_offset,
            no_match_size=no_match_size,
            phrase_limit=phrase_limit,
            **kwargs
        )


class Field(Expression, FieldOperators):
    __visit_name__ = 'field'

    _counter = count()

    def __init__(self, *args, **kwargs):
        self._name = None
        self._type = None
        if len(args) == 1:
            if isinstance(args[0], string_types):
                self._name = args[0]
            elif (
                    isinstance(args[0], Type) or (
                        inspect.isclass(args[0]) and issubclass(args[0], Type)
                    )
            ):
                self._type = args[0]
            else:
                raise TypeError(
                    'Argument must be string or field type: {} found'.format(
                        args[0].__class__.__name__))
        elif len(args) == 2:
            self._name, self._type = args
        elif len(args) >= 3:
            raise TypeError('Too many positional arguments: %s' % len(args))

        if self._type is None:
            self._type = Type()
        self._type = instantiate(self._type)

        self._fields = kwargs.pop('fields', {})
        self._count = kwargs.pop('_counter', next(self._counter))
        self._mapping_options = kwargs

    def clone(self, cls=None):
        cls = cls or self.__class__
        assert issubclass(cls, Field)
        return cls(
            self._name, self._type, fields=self._fields,
            **self._mapping_options
        )

    def get_name(self):
        return self._name

    def get_type(self):
        return self._type

    def _to_python(self, value):
        return self._type.to_python(value)

    def _from_python(self, value):
        return self._type.from_python(value)

    def to_mapping(self, compiler=None):
        from .compiler import DefaultCompiler

        mapping_compiler = (compiler or DefaultCompiler).compiled_mapping
        return mapping_compiler(self).params


class MappingField(Field):
    __visit_name__ = 'mapping_field'


class HighlightedField(Expression):
    __visit_name__ = 'highlighted_field'

    def __init__(self, field, **kwargs):
        self.field = field
        self.params = Params(kwargs)

    def _collect_doc_classes(self):
        return set().union(
            collect_doc_classes(self.field),
            collect_doc_classes(self.params),
        )


class BoostExpression(Expression):
    __visit_name__ = 'boost_expression'

    def __init__(self, expr, weight):
        self.expr = expr
        self.weight = weight

    def _collect_doc_classes(self):
        return collect_doc_classes(self.expr)


class SpanFirst(QueryExpression):
    __query_name__ = 'span_first'

    def __init__(self, match, end, boost=None, **kwargs):
        super(SpanFirst, self).__init__(
            match=match, end=end, boost=boost, **kwargs)


class SpanMulti(QueryExpression):
    __query_name__ = 'span_multi'

    def __init__(self, match, **kwargs):
        super(SpanMulti, self).__init__(match=match, **kwargs)


class SpanNear(QueryExpression):
    __query_name__ = 'span_near'

    def __init__(
            self, clauses, slop=None, in_order=None, collect_payloads=None,
            **kwargs
    ):
        super(SpanNear, self).__init__(
            clauses=clauses, slop=slop, in_order=in_order,
            collect_payloads=collect_payloads, **kwargs
        )


class SpanNot(QueryExpression):
    __query_name__ = 'span_not'

    def __init__(
            self, include, exclude, dist=None, pre=None, post=None, boost=None,
            **kwargs
    ):
        super(SpanNot, self).__init__(
            include=include, exclude=exclude, dist=dist, pre=pre, post=post,
            boost=boost, **kwargs
        )


class SpanOr(QueryExpression):
    __query_name__ = 'span_or'

    def __init__(self, clauses, boost=None, **kwargs):
        super(SpanOr, self).__init__(clauses=clauses, boost=boost, **kwargs)


class SpanTerm(Term):
    __query_name__ = 'span_term'


class Nested(QueryExpression):
    __query_name__ = 'nested'

    def __init__(self, path, query, score_mode=None, **kwargs):
        super(Nested, self).__init__(
            path=path, query=query, score_mode=score_mode, **kwargs
        )


class ParentId(QueryExpression):
    __visit_name__ = 'parent_id'

    def __init__(self, child_type, parent_id, **kwargs):
        super(ParentId, self).__init__(**kwargs)
        self.child_type = child_type
        self.parent_id = parent_id

    def _collect_doc_classes(self):
        if hasattr(self.child_type, '__doc_type__'):
            return {self.child_type}

        return set()


class HasParent(QueryExpression):
    __visit_name__ = 'has_parent'

    def __init__(self, query, parent_type=None, score_mode=None, **kwargs):
        super(HasParent, self).__init__(
            query=query, score_mode=score_mode, **kwargs
        )
        self.parent_type = parent_type

    def _collect_doc_classes(self):
        return set()


class HasChild(QueryExpression):
    __visit_name__ = 'has_child'

    def __init__(self, query, type=None, score_mode=None, **kwargs):
        super(HasChild, self).__init__(
            query=query, score_mode=score_mode, **kwargs
        )
        self.type = type

    def _collect_doc_classes(self):
        return set()


def _wrap_literal(obj):
    if not isinstance(obj, Expression):
        return Literal(obj)
    return obj


class Source(Expression):
    __visit_name__ = 'source'

    def __init__(self, fields, include=None, exclude=None):
        self.fields = fields
        self.include = include
        self.exclude = exclude

    def _collect_doc_classes(self):
        return set().union(
            collect_doc_classes(self.fields),
            collect_doc_classes(self.include),
            collect_doc_classes(self.exclude),
        )


class QueryRescorer(ParamsExpression):
    __visit_name__ = 'query_rescorer'

    def __init__(
            self, rescore_query, query_weight=None, rescore_query_weight=None,
            score_mode=None, **kwargs
    ):
        super(QueryRescorer, self).__init__(
            rescore_query=rescore_query, query_weight=query_weight,
            rescore_query_weight=rescore_query_weight, score_mode=score_mode,
            **kwargs
        )


class Rescore(Expression):
    __visit_name__ = 'rescore'

    def __init__(self, rescorer, window_size=None,
                 ):
        self.rescorer = rescorer
        self.window_size = window_size

    def _collect_doc_classes(self):
        return collect_doc_classes(self.rescorer)


class Highlight(Expression):
    __visit_name__ = 'highlight'

    def __init__(self, fields=None, **kwargs):
        self.fields = fields
        self.params = Params(kwargs)

    def _collect_doc_classes(self):
        return set().union(
            collect_doc_classes(self.fields),
            collect_doc_classes(self.params),
        )
