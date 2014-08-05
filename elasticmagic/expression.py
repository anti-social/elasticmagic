import inspect
import operator
import collections
from itertools import chain

from .types import instantiate, Type
from .compat import string_types


OPERATORS = {
    operator.and_: 'and',
    operator.or_: 'or',
    operator.inv: 'inv',
}

class Expression(object):
    _doc_types = ()

    def __init__(self, *args, **kwargs):
        params = {}
        for k, v in kwargs.items():
            if v is None:
                continue
            if k.endswith('_') and not k.startswith('_'):
                k = k.rstrip('_')
            params[k] = v
        for expr in args:
            if hasattr(expr, '__param_name__'):
                param_name = expr.__param_name__
                if param_name in params:
                    if isinstance(params[param_name], (list, tuple)):
                        params[param_name] = tuple(params[param_name]) + tuple(expr.expressions)
                    else:
                        params[param_name] = tuple(expr.expressions)
                else:
                    params[param_name] = tuple(expr.expressions)
        self.params = Params(params)

    def compile(self):
        return Compiled(self)


class Params(Expression, collections.Mapping):
    __visit_name__ = 'params'

    def __init__(self, *args, **kwargs):
        self._params = dict(*args, **kwargs)

    def __len__(self):
        return len(self._params)

    def __iter__(self):
        return iter(self._params)

    def __getitem__(self, key):
        return self._params[key]

    def __contains__(self, key):
        return key in self._params


class QueryExpression(Expression):
    __visit_name__ = 'query_expression'


class FieldQuery(QueryExpression):
    __visit_name__ = 'field_query'
    __query_key__ = 'query'

    def __init__(self, field, query, **kwargs):
        super(FieldQuery, self).__init__(**kwargs)
        self.field = field
        self.query = query

    @property
    def _doc_types(self):
        return self.field._doc_types


class Term(FieldQuery):
    __query_name__ = 'term'
    __query_key__ = 'value'

    def __init__(self, field, value, boost=None, **kwargs):
        super(Term, self).__init__(field, value, boost=boost, **kwargs)


class Terms(QueryExpression):
    __visit_name__ = 'terms'

    def __init__(self, field, terms, minimum_should_match=None, boost=None, **kwargs):
        super(Terms, self).__init__(
            minimum_should_match=minimum_should_match, boost=boost, **kwargs
        )
        self.field = field
        self.terms = terms

    @property
    def _doc_types(self):
        return self.field._doc_types


class Match(FieldQuery):
    __query_name__ = 'match'

    def __init__(self, field, query,
                 type=None, analyzer=None, boost=None,
                 operator=None, minimum_should_match=None,
                 fuzziness=None, prefix_length=None, max_expansions=None,
                 zero_terms_query=None, cutoff_frequency=None, lenient=None,
                 **kwargs):
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
            minimum_should_match=minimum_should_match, fuzziness=fuzziness, prefix_length=prefix_length,
            max_expansions=max_expansions, rewrite=rewrite, zero_terms_query=zero_terms_query,
            cutoff_frequency=cutoff_frequency, tie_breaker=tie_breaker, **kwargs
        )
        self.query = query
        self.fields = fields

    @property
    def _doc_types(self):
        return list(chain(f._doc_types for f in self.fields))



class MatchAll(QueryExpression):
    __query_name__ = 'match_all'

    def __init__(self, boost=None, **kwargs):
        super(MatchAll, self).__init__(boost=boost, **kwargs)


class NamedParam(collections.Sequence):
    def __init__(self, *expressions):
        self.expressions = expressions

    def __len__(self):
        return len(self.expressions)

    def __getitem__(self, index):
        return self.expressions[index]

    @property
    def _doc_types(self):
        return set(chain(e._doc_types for e in self.expressions))


class Must(NamedParam):
    __param_name__ = 'must'


class MustNot(NamedParam):
    __param_name__ = 'must_not'


class Should(NamedParam):
    __param_name__ = 'should'


class Bool(QueryExpression):
    __query_name__ = 'bool'

    # TODO: make for Python3
    # def __init__(
    #         self, *args, must=None, must_not=None, should=None,
    #         minimum_should_match=None, boost=None, disable_coord=None,
    #         **kwargs
    # ):
    #     super(Bool, self).__init__(
    #         must=must, must_not=must_not, should=should,
    #         minimum_should_match=minimum_should_match,
    #         boost=boost, disable_coord=disable_coord, **kwargs)

    @property
    def _doc_types(self):
        doc_types = set()
        for expressions in [self.params.get('must', []),
                            self.params.get('must_not', []),
                            self.params.get('should', [])]:
            if not isinstance(expressions, (tuple, list)):
                expressions = [expressions]
            for e in expressions:
                doc_types.update(e._doc_types)
        return doc_types


class Boosting(QueryExpression):
    __query_name__ = 'boosting'

    def __init__(self, positive=None, negative=None, negative_boost=None, boost=None, **kwargs):
        super(Boosting, self).__init__(
            positive=positive, negative=negative,
            negative_boost=negative_boost, boost=boost, **kwargs
        )


class Common(FieldQuery):
    __query_name__ = 'common'

    def __init__(
            self, field, query,
            cutoff_frequency=None, minimum_should_match=None,
            high_freq_operator=None, low_freq_operator=None,
            boost=None, analyzer=None, disable_coord=None, **kwargs
    ):
        super(Common, self).__init__(
            field, query,
            cutoff_frequency=cutoff_frequency, minimum_should_match=minimum_should_match,
            high_freq_operator=high_freq_operator, low_freq_operator=low_freq_operator,
            boost=boost, analyzer=analyzer, disable_coord=disable_coord, **kwargs
        )


class ConstantScore(QueryExpression):
    __query_name__ = 'constant_score'

    def __init__(self, filter=None, query=None, boost=None, **kwargs):
        super(ConstantScore, self).__init__(filter=filter, query=query, boost=boost, **kwargs)


class DisMax(QueryExpression):
    __query_name__ = 'dis_max'

    def __init__(self, queries=None, boost=None, tie_breaker=None, **kwargs):
        super(DisMax, self).__init__(
            queries=queries, boost=boost, tie_breaker=tie_breaker, **kwargs
        )


class Filtered(QueryExpression):
    __query_name__ = 'filtered'

    def __init__(self, filter=None, query=None, strategy=None, **kwargs):
        super(Filtered, self).__init__(filter=filter, query=query, strategy=strategy, **kwargs)


class Range(QueryExpression):
    __visit_name__ = 'range'

    def __init__(self, field, gte=None, gt=None, lte=None, lt=None, boost=None, **kwargs):
        super(Range, self).__init__(gte=gte, gt=gt, lte=lte, lt=lt, boost=boost, **kwargs)
        self.field = field

    @property
    def _doc_types(self):
        return self.field._doc_types


class Prefix(FieldQuery):
    __query_name__ = 'prefix'
    __query_key__ = 'value'

    def __init__(self, field, value, boost=None, **kwargs):
        super(Prefix, self).__init__(field, value, boost=boost, **kwargs)


class Query(QueryExpression):
    __visit_name__ = 'query'

    def __init__(self, query, **kwargs):
        super(Query, self).__init__(**kwargs)
        self.query = query

    @property
    def _doc_types(self):
        return self.query._doc_types


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
        
    @classmethod
    def and_(cls, *expressions, **kwargs):
        return cls._construct(operator.and_, *expressions, **kwargs)

    @classmethod
    def or_(cls, *expressions, **kwargs):
        return cls._construct(operator.or_, *expressions, **kwargs)


    @property
    def _doc_types(self):
        return set(chain(e._doc_types for e in self.expressions))


And = BooleanExpression.and_
Or = BooleanExpression.or_


class Not(QueryExpression):
    __visit_name__ = 'not'

    def __init__(self, expr, **kwargs):
        super(Not, self).__init__(**kwargs)
        self.expr = expr

    @property
    def _doc_types(self):
        return self.expr._doc_types


class Exists(QueryExpression):
    __query_name__ = 'exists'

    def __init__(self, field, **kwargs):
        super(Exists, self).__init__(field=field, **kwargs)

    @property
    def _doc_types(self):
        return self.params['field']._doc_types


class Missing(QueryExpression):
    __query_name__ = 'missing'

    def __init__(self, field, **kwargs):
        super(Missing, self).__init__(field=field, **kwargs)

    @property
    def _doc_types(self):
        return self.params['field']._doc_types


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

    @property
    def _doc_types(self):
        return self.expr._doc_types


class Field(Expression):
    __visit_name__ = 'field'

    def __init__(self, *args):
        self.name = None
        self.type = None
        self.doc_cls = None

        if len(args) == 1:
            if isinstance(args[0], string_types):
                self.name = args[0]
            elif (
                    isinstance(args[0], Type) or (
                        inspect.isclass(args[0]) and issubclass(args[0], Type)
                    )
            ):
                self.type = args[0]
            else:
                raise TypeError('Argument must be string or field type: %s found' % args[0].__class__.__name__)
        elif len(args) == 2:
            self.name, self.type = args
        else:
            raise TypeError('Takes 1 or 2 positional arguments: %s given' % len(args))

        if self.type is None:
            self.type = Type()
        self.type = instantiate(self.type)

    def __getattr__(self, name):
        return Field('{}.{}'.format(self.name, name))

    def __set__(self, obj, value):
        if self.type is not None:
            value = self.type.to_python(value)
        obj.__dict__[self.name] = value

    @property
    def _doc_types(self):
        if self.doc_cls:
            return [self.doc_cls]
        return []

    def __eq__(self, other):
        if other is None:
            return self.missing()
        return self.term(other)

    def __ne__(self, other):
        if other is None:
            return self.exists()
        return Not(operator.eq(self, other))

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

    def exists(self, **kwargs):
        return Exists(self, **kwargs)

    def missing(self, **kwargs):
        return Missing(self, **kwargs)

    def in_(self, terms, **kwargs):
        return Terms(self, terms, **kwargs)

    def match(self, query, **kwargs):
        return Match(self, query, **kwargs)

    def range(self, gte=None, lte=None, gt=None, lt=None):
        return Range(self, gte=gte, lte=lte, gt=gt, lt=lt)

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


class BoostExpression(Expression):
    __visit_name__ = 'boost_expression'

    def __init__(self, expr, weight):
        self.expr = expr
        self.weight = weight

    @property
    def _doc_types(self):
        return self.expr._doc_types


class FieldExpression(Expression):
    __visit_name__ = 'field_expression'

    def __init__(self, field, other, operator):
        self.field = field
        self.other = other
        self.operator = operator

    @property
    def _doc_types(self):
        return self.field._doc_types


class Fields(object):
    def __getattr__(self, name):
        return Field(name)

    def _wildcard(self, name):
        return Field(name)

    _w = _wildcard


class Compiled(object):
    def __init__(self, expression):
        self.expression = expression
        self.params = self.visit(self.expression)
        
    def __str__(self):
        return self.string

    def visit(self, expr, **kwargs):
        visit_name = None
        if hasattr(expr, '__visit_name__'):
            visit_name = expr.__visit_name__

        if visit_name:
            visit_func = getattr(self, 'visit_{}'.format(visit_name))
            return visit_func(expr, **kwargs)
        return expr

    def visit_params(self, params):
        res = {}
        for k, v in params.items():
            key = self.visit(k)
            if isinstance(v, (list, tuple)):
                if len(v) == 1:
                    res[key] = self.visit(v[0])
                else:
                    res[key] = [self.visit(w) for w in v]
            else:
                res[key] = self.visit(v)
        return res

    def visit_field(self, field):
        return field.name

    def visit_boost_expression(self, boost):
        return '{}^{}'.format(self.visit(boost.expr), self.visit(boost.weight))

    def visit_field_expression(self, expr, key=None, params=None):
        if expr.operator == operator.eq:
            if key:
                options = {
                    key: expr.other
                }
                if params:
                    options.update(params)
                return {
                    expr.field.name: options
                }
            else:
                return {expr.field.name: expr.other}

    def visit_query_expression(self, expr):
        return {
            expr.__query_name__: self.visit(expr.params)
        }

    def visit_field_query(self, fq):
        if fq.params:
            params = {fq.__query_key__: self.visit(fq.query)}
            params.update(fq.params)
            return {
                fq.__query_name__: {
                    self.visit(fq.field): params
                }
            }
        else:
            return {
                fq.__query_name__: {
                    self.visit(fq.field): self.visit(fq.query)
                }
            }

    def visit_range(self, range):
        return {
            'range': {
                self.visit(range.field): self.visit(range.params)
            }
        }

    def visit_terms(self, expr):
        params = {self.visit(expr.field): self.visit(expr.terms)}
        params.update(self.visit(expr.params))
        return {
            'terms': params
        }

    def visit_multi_match(self, expr):
        params = {
            'query': self.visit(expr.query),
            'fields': [self.visit(f) for f in expr.fields],
        }
        params.update(self.visit(params))
        return {
            'multi_match': params
        }

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
        if agg._aggs:
            params['aggregations'] = self.visit(agg._aggs)
        return params
        
    def visit_search_query(self, query):
        params = {}
        q = query._q
        if query._filters:
            q = Filtered(query=q, filter=And(query._filters))
        if q is not None:
            params['query'] = self.visit(q)
        if query._order_by:
            params['sort'] = [self.visit(o) for o in query._order_by]
        if query._fields:
            params['_source'] = [self.visit(f) for f in query._fields]
        if query._aggregations:
            params['aggregations'] = self.visit(query._aggregations)
        if query._limit is not None:
            params['size'] = query._limit
        if query._offset is not None:
            params['from'] = query._offset
        return params
