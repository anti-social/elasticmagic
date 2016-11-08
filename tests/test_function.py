from elasticmagic import (
    Weight, Factor, ScriptScore, RandomScore, Gauss, Exp, Linear, Script,
    DynamicDocument, Bool,
)


PostDocument = DynamicDocument


def test_weight():
    assert Weight(3).to_elastic() == {"weight": 3}
    assert Weight(2, filter=Bool.must(
        PostDocument.status.in_([0, 1]),
        PostDocument.created_date >= 'now/d-7d')
    ).to_elastic() == {
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
    assert Factor(PostDocument.popularity).to_elastic() == {
        "field_value_factor": {
            "field": "popularity"
        }
    }


def test_script_score():
    assert ScriptScore(
        Script(lang='painless',
               inline='_score * doc[field].value',
               params={'field': PostDocument.popularity})
    ).to_elastic() == {
        "script_score": {
            "script": {
                "lang": "painless",
                "inline": "_score * doc[field].value",
                "params": {"field": "popularity"}
            }
        }
    }


def test_random_score():
    assert RandomScore(17).to_elastic() == {"random_score": {"seed": 17}}


def test_gauss():
    assert Gauss(PostDocument.created_date, origin='now', scale='1h').to_elastic() == {
        "gauss": {
            "created_date": {"origin": "now", "scale": "1h"}
        }
    }


def test_exp():
    assert Exp(PostDocument.popularity, origin=0, scale=20).to_elastic() == {
        "exp": {
            "popularity": {"origin": 0, "scale": 20}
        }
    }


def test_linear():
    assert Linear(
        PostDocument.places, origin='11,12', scale='2km', multi_value_mode='avg'
    ).to_elastic() == {
        "linear": {
            "places": {"origin": "11,12", "scale": "2km"},
            "multi_value_mode": "avg"
        }
    }
