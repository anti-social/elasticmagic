
def assert_expr(expr, params):
    compiled = expr.to_dict()
    assert compiled == params
