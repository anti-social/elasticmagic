
from .expression import Params, ParamsExpression


class Script(ParamsExpression):
    __visit_name__ = 'script'

    def __init__(
            self, lang=None, inline=None, id=None, file=None, params=None
    ):
        assert inline or id or file, \
            "Specify one of the next arguments: 'inline', 'id' of 'file'"
        super(Script, self).__init__(
            lang=lang, inline=inline, id=id, file=file, params=params
        )


class Function(ParamsExpression):
    __visit_name__ = 'function'

    def __init__(self, filter=None, weight=None, **kwargs):
        self.filter = filter
        self.weight = weight
        super(Function, self).__init__(**kwargs)


class Weight(Function):
    __func_name__ = 'weight'
    __visit_name__ = 'weight_function'

    def __init__(self, weight, filter=None):
        super(Weight, self).__init__(filter=filter, weight=weight)


class FieldValueFactor(Function):
    __func_name__ = 'field_value_factor'

    def __init__(
            self, field, factor=None, modifier=None, missing=None,
            filter=None, **kwargs
    ):
        super(FieldValueFactor, self).__init__(
            field=field, factor=factor, modifier=modifier, missing=missing,
            filter=filter, **kwargs
        )


Factor = FieldValueFactor


class ScriptScore(Function):
    __func_name__ = 'script_score'

    def __init__(self, script, filter=None, **kwargs):
        super(ScriptScore, self).__init__(
            script=script, filter=filter, **kwargs
        )


class RandomScore(Function):
    __func_name__ = 'random_score'

    def __init__(self, seed=None, filter=None, **kwargs):
        super(RandomScore, self).__init__(seed=seed, filter=filter, **kwargs)


class DecayFunction(Function):
    __visit_name__ = 'decay_function'

    def __init__(
            self, field, origin, scale, offset=None, decay=None,
            multi_value_mode=None, **kwargs
    ):
        self.field = field
        self.decay_params = Params(
            origin=origin, scale=scale, offset=offset, decay=decay,
        )
        super(DecayFunction, self).__init__(
            multi_value_mode=multi_value_mode, **kwargs
        )


class Gauss(DecayFunction):
    __func_name__ = 'gauss'


class Exp(DecayFunction):
    __func_name__ = 'exp'


class Linear(DecayFunction):
    __func_name__ = 'linear'
