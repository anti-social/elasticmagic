from elasticmagic import (
    Weight, Factor, ScriptScore, RandomScore, Gauss, Exp, Linear, Script,
    DynamicDocument, Bool,
)
from elasticmagic.compiler import Compiler_7_0


PostDocument = DynamicDocument


def test_weight():
    assert Weight(3).to_elastic(Compiler_7_0) == {"weight": 3}
    assert Weight(2, filter=Bool.must(
        PostDocument.status.in_([0, 1]),
        PostDocument.created_date >= 'now/d-7d')
    ).to_elastic(Compiler_7_0) == {
        "weight": 2,
        "filter": {
            "bool": {
                "must": [
                    {"terms": {"status": [0, 1]}},
                    {"range": {"created_date": {"gte": "now/d-7d"}}}
                ]
            }
        }
    }


def test_factor():
    assert Factor(PostDocument.popularity).to_elastic(Compiler_7_0) == {
        "field_value_factor": {
            "field": "popularity"
        }
    }


def test_script_score():
    assert ScriptScore(
        Script(lang='painless',
               inline='_score * doc[params.field].value',
               params={'field': PostDocument.popularity})
    ).to_elastic(Compiler_7_0) == {
        "script_score": {
            "script": {
                "lang": "painless",
                "source": "_score * doc[params.field].value",
                "params": {"field": "popularity"}
            }
        }
    }


def test_random_score():
    assert RandomScore(17).to_elastic(Compiler_7_0) == {"random_score": {"seed": 17}}


def test_gauss():
    assert Gauss(PostDocument.created_date, origin='now', scale='1h').to_elastic(Compiler_7_0) == {
        "gauss": {
            "created_date": {"origin": "now", "scale": "1h"}
        }
    }


def test_exp():
    assert Exp(PostDocument.popularity, origin=0, scale=20).to_elastic(Compiler_7_0) == {
        "exp": {
            "popularity": {"origin": 0, "scale": 20}
        }
    }


def test_linear():
    assert Linear(
        PostDocument.places, origin='11,12', scale='2km', multi_value_mode='avg'
    ).to_elastic(Compiler_7_0) == {
        "linear": {
            "places": {"origin": "11,12", "scale": "2km"},
            "multi_value_mode": "avg"
        }
    }
