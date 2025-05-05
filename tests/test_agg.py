import math
from unittest.mock import Mock, patch

from elasticmagic import agg, Params, Term, Document, DynamicDocument
from elasticmagic.compiler import Compiler_6_0
from elasticmagic.compiler import Compiler_7_0
from elasticmagic.expression import Field, Script
from elasticmagic.types import Integer, Boolean, List

import pytest

from .base import BaseTestCase


class AggregationTest(BaseTestCase):
    def test_aggs(self):
        f = DynamicDocument.fields

        a = agg.AggExpression()
        self.assertRaises(NotImplementedError, a.build_agg_result, {})

        a = agg.Avg(f.price)
        self.assert_expression(
            a,
            {
                "avg": {"field": "price"}
            }
        )
        res = a.build_agg_result({
            'value': 75.3
        })
        self.assertAlmostEqual(res.value, 75.3)
        res = a.build_agg_result({
            'value': None
        })
        self.assertIs(res.value, None)

        aa = a.clone()
        self.assertIsNot(a, aa)
        self.assertEqual(a.__visit_name__, aa.__visit_name__)
        self.assertEqual(a.params, aa.params)

        a = agg.Min(f.price)
        self.assert_expression(
            a,
            {
                "min": {"field": "price"}
            }
        )
        res = a.build_agg_result({
            'value': 38
        })
        self.assertAlmostEqual(res.value, 38)
        res = a.build_agg_result({
            'value': 1297167619690,
            'value_as_string': '2011-02-08T12:20:19.690Z'
        })
        self.assertAlmostEqual(res.value, 1297167619690)

        a = agg.Max(f.price)
        self.assert_expression(
            a,
            {
                "max": {"field": "price"}
            }
        )
        res = a.build_agg_result({
            'value': 45693.5
        })
        self.assertAlmostEqual(res.value, 45693.5)

        a = agg.Stats(f.grade)
        self.assert_expression(
            a,
            {
                "stats": {"field": "grade"}
            }
        )
        a = a.build_agg_result(
            {
                "count": 6,
                "min": 60,
                "max": 98,
                "avg": 78.5,
                "sum": 471
            }
        )
        self.assertEqual(a.count, 6)
        self.assertEqual(a.min, 60)
        self.assertEqual(a.max, 98)
        self.assertAlmostEqual(a.avg, 78.5)
        self.assertEqual(a.sum, 471)

        a = agg.ExtendedStats(f.grade)
        self.assert_expression(
            a,
            {
                "extended_stats": {"field": "grade"}
            }
        )
        a = a.build_agg_result(
            {
                "count": 6,
                "min": 72,
                "max": 117.6,
                "avg": 94.2,
                "sum": 565.2,
                "sum_of_squares": 54551.51999999999,
                "variance": 218.2799999999976,
                "std_deviation": 14.774302013969987
            }
        )
        self.assertEqual(a.count, 6)
        self.assertEqual(a.min, 72)
        self.assertAlmostEqual(a.max, 117.6)
        self.assertAlmostEqual(a.avg, 94.2)
        self.assertAlmostEqual(a.sum, 565.2)
        self.assertAlmostEqual(a.sum_of_squares, 54551.51999999999)
        self.assertAlmostEqual(a.variance, 218.2799999999976)
        self.assertAlmostEqual(a.std_deviation, 14.774302013969987)

        percentiles_agg = agg.Percentiles(f.load_time, percents=[95, 99, 99.9])
        self.assert_expression(
            percentiles_agg,
            {
                "percentiles": {
                    "field": "load_time",
                    "percents": [95, 99, 99.9]
                }
            }
        )
        a = percentiles_agg.build_agg_result(
            {
                "values": {
                    "95.0": 60,
                    "99.0": 150,
                    "99.9": 153,
                }
            }
        )
        self.assertEqual(
            a.values,
            [(95.0, 60), (99.0, 150), (99.9, 153)],
        )
        self.assertEqual(a.get_value(95), 60)
        self.assertEqual(a.get_value(95.0), 60)
        self.assertEqual(a.get_value(99), 150)
        self.assertEqual(a.get_value(99.0), 150)
        self.assertEqual(a.get_value(99.9), 153)
        a = percentiles_agg.build_agg_result(
            {
                "values": {
                    "95.0": 60,
                    "95.0_as_string": "60",
                    "99.0": 150,
                    "99.0_as_string": "150",
                    "99.9": 153,
                    "99.9_as_string": "153",
                }
            }
        )
        self.assertEqual(
            a.values,
            [(95.0, 60), (99.0, 150), (99.9, 153)],
        )
        self.assertEqual(a.get_value(95), 60)
        self.assertEqual(a.get_value(95.0), 60)
        self.assertEqual(a.get_value(99), 150)
        self.assertEqual(a.get_value(99.0), 150)
        self.assertEqual(a.get_value(99.9), 153)

        percentiles_agg = agg.Percentiles(f.load_time, percents=[50])
        self.assert_expression(
            percentiles_agg,
            {
                "percentiles": {
                    "field": "load_time",
                    "percents": [50]
                }
            }
        )
        a = percentiles_agg.build_agg_result(
            {
                "values": {
                    "50.0": "NaN",
                }
            }
        )
        self.assertEqual(
            len(a.values),
            1
        )
        self.assertAlmostEqual(
            a.values[0][0],
            50.0
        )
        self.assertTrue(
            math.isnan(a.values[0][1])
        )
        self.assertTrue(math.isnan(a.get_value(50)))
        self.assertTrue(math.isnan(a.get_value(50.0)))

        ranks_agg = agg.PercentileRanks(f.load_time, values=[14.8, 30])
        self.assert_expression(
            ranks_agg,
            {
                "percentile_ranks": {
                    "field": "load_time",
                    "values": [14.8, 30.0]
                }
            }
        )
        a = ranks_agg.build_agg_result(
            {
                "values": {
                    "14.8": 12.32,
                    "30": 100,
                }
            }
        )
        self.assertEqual(
            a.values,
            [(14.8, 12.32), (30.0, 100)],
        )
        self.assertEqual(
            a.values,
            [(14.8, 12.32), (30.0, 100)],
        )
        self.assertAlmostEqual(a.get_percent(14.8), 12.32)
        self.assertAlmostEqual(a.get_percent(13.7 + 1.1), 12.32)
        self.assertAlmostEqual(a.get_percent(30), 100.0)
        self.assertAlmostEqual(a.get_percent(30.0), 100.0)
        a = ranks_agg.build_agg_result(
            {
                "values": {
                    "14.8": 12.32,
                    "14.8_as_string": "12.32",
                    "30": 100,
                    "30_as_string": "100",
                }
            }
        )
        self.assertEqual(
            a.values,
            [(14.8, 12.32), (30.0, 100)],
        )
        self.assertEqual(
            a.values,
            [(14.8, 12.32), (30.0, 100)],
        )
        self.assertAlmostEqual(a.get_percent(14.8), 12.32)
        self.assertAlmostEqual(a.get_percent(13.7 + 1.1), 12.32)
        self.assertAlmostEqual(a.get_percent(30), 100.0)
        self.assertAlmostEqual(a.get_percent(30.0), 100.0)

        a = agg.Cardinality(f.author, precision_threshold=100)
        self.assert_expression(
            a,
            {
                "cardinality": {
                    "field": "author",
                    "precision_threshold": 100
                }
            }
        )
        a = a.build_agg_result(
            {
                "value": 184
            }
        )
        self.assertEqual(a.value, 184)

        a = agg.Global()
        self.assert_expression(a, {"global": {}})
        a = a.build_agg_result(
            {"doc_count": 185}
        )
        self.assertEqual(a.doc_count, 185)

        a = agg.Filter(f.company == 1)
        self.assert_expression(a, {"filter": {"term": {"company": 1}}})
        a2 = a.clone()
        self.assertIsNot(a, a2)
        self.assert_expression(a2, {"filter": {"term": {"company": 1}}})
        a = a.build_agg_result(
            {"doc_count": 148}
        )
        self.assertEqual(a.doc_count, 148)

        a = agg.Terms(f.status)
        self.assert_expression(
            a,
            {
                "terms": {"field": "status"}
            }
        )
        a1 = a.clone()
        self.assertIsNot(a, a1)
        a = a.build_agg_result(
            {
                'buckets': [
                    {'doc_count': 7353499, 'key': 0},
                    {'doc_count': 2267139, 'key': 1},
                    {'doc_count': 1036951, 'key': 4},
                    {'doc_count': 438384, 'key': 2},
                    {'doc_count': 9594, 'key': 3},
                    {'doc_count': 46, 'key': 5}
                ]
            }
        )
        self.assertEqual(len(a.buckets), 6)
        self.assertEqual(list(iter(a)), a.buckets)
        self.assertEqual(a.buckets[0].key, 0)
        self.assertEqual(a.buckets[0].doc_count, 7353499)
        self.assertEqual(repr(a.buckets[0]), '<Bucket key=0 doc_count=7353499>')

        self.assertIs(a.buckets[0], a.get_bucket(0))
        self.assertEqual(a.buckets[1].key, 1)
        self.assertEqual(a.buckets[1].doc_count, 2267139)
        self.assertIs(a.buckets[1], a.get_bucket(1))
        self.assertEqual(repr(a.buckets[1]), '<Bucket key=1 doc_count=2267139>')

        self.assertEqual(a.buckets[2].key, 4)
        self.assertEqual(a.buckets[2].doc_count, 1036951)
        self.assertIs(a.buckets[2], a.get_bucket(4))
        self.assertEqual(repr(a.buckets[2]), '<Bucket key=4 doc_count=1036951>')

        self.assertEqual(a.buckets[3].key, 2)
        self.assertEqual(a.buckets[3].doc_count, 438384)
        self.assertIs(a.buckets[3], a.get_bucket(2))
        self.assertEqual(repr(a.buckets[3]), '<Bucket key=2 doc_count=438384>')

        self.assertEqual(a.buckets[4].key, 3)
        self.assertEqual(a.buckets[4].doc_count, 9594)
        self.assertIs(a.buckets[4], a.get_bucket(3))
        self.assertEqual(repr(a.buckets[4]), '<Bucket key=3 doc_count=9594>')

        self.assertEqual(a.buckets[5].key, 5)
        self.assertEqual(a.buckets[5].doc_count, 46)
        self.assertIs(a.buckets[5], a.get_bucket(5))
        self.assertEqual(repr(a.buckets[5]), '<Bucket key=5 doc_count=46>')

        a = agg.Terms(f.is_visible, type=Boolean)
        self.assert_expression(
            a,
            {
                "terms": {"field": "is_visible"}
            }
        )
        a = a.build_agg_result(
            {
                'buckets': [
                    {'doc_count': 7, 'key': 'T'},
                    {'doc_count': 2, 'key': 'F'},
                ]
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertEqual(a.buckets[0].key, True)
        self.assertEqual(a.buckets[0].doc_count, 7)
        self.assertIs(a.buckets[0], a.get_bucket(True))
        self.assertEqual(a.buckets[1].key, False)
        self.assertEqual(a.buckets[1].doc_count, 2)
        self.assertIs(a.buckets[1], a.get_bucket(False))

        a = agg.Terms(f.category, type=List(Integer))
        self.assert_expression(
            a,
            {
                "terms": {"field": "category"}
            }
        )
        a = a.build_agg_result(
            {
                'buckets': [
                    {'doc_count': 792, 'key': 28},
                    {'doc_count': 185, 'key': 3},
                ]
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertEqual(a.buckets[0].key, 28)
        self.assertEqual(a.buckets[0].doc_count, 792)
        self.assertIs(a.buckets[0], a.get_bucket(28))
        self.assertEqual(a.buckets[1].key, 3)
        self.assertEqual(a.buckets[1].doc_count, 185)
        self.assertIs(a.buckets[1], a.get_bucket(3))

        class ProductDocument(Document):
            is_visible = Field(Boolean)
        a = agg.Terms(ProductDocument.is_visible)
        self.assert_expression(
            a,
            {
                "terms": {"field": "is_visible"}
            }
        )
        a = a.build_agg_result(
            {
                'buckets': [
                    {'doc_count': 7, 'key': 'T'},
                    {'doc_count': 2, 'key': 'F'},
                ]
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertEqual(a.buckets[0].key, True)
        self.assertEqual(a.buckets[0].doc_count, 7)
        self.assertIs(a.buckets[0], a.get_bucket(True))
        self.assertEqual(a.buckets[1].key, False)
        self.assertEqual(a.buckets[1].doc_count, 2)
        self.assertIs(a.buckets[1], a.get_bucket(False))

        a = agg.SignificantTerms(f.crime_type)
        self.assert_expression(
            a,
            {
                "significant_terms": {"field": "crime_type"}
            }
        )
        a = a.build_agg_result(
            {
                "doc_count": 47347,
                "buckets" : [
                    {
                        "key": "Bicycle theft",
                        "doc_count": 3640,
                        "score": 0.371,
                        "bg_count": 66799,
                    },
                    {
                        "key": "Mobile phone theft",
                        "doc_count": 27617,
                        "score": 0.0599,
                        "bg_count": 53182,
                    }
                ]
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertEqual(a.buckets[0].key, 'Bicycle theft')
        self.assertEqual(a.buckets[0].doc_count, 3640)
        self.assertAlmostEqual(a.buckets[0].score, 0.371)
        self.assertEqual(a.buckets[0].bg_count, 66799)
        self.assertIs(a.buckets[0], a.get_bucket('Bicycle theft'))
        self.assertEqual(a.buckets[1].key, 'Mobile phone theft')
        self.assertEqual(a.buckets[1].doc_count, 27617)
        self.assertAlmostEqual(a.buckets[1].score, 0.0599)
        self.assertEqual(a.buckets[1].bg_count, 53182)
        self.assertIs(a.buckets[1], a.get_bucket('Mobile phone theft'))

        a = agg.Range(
            f.price,
            ranges=[{'to': 200}, {'from': 200, 'to': 1000}, {'from': 1000}],
            type=Integer,
        )
        self.assert_expression(
            a,
            {
                "range": {
                    "field": "price",
                    "ranges": [
                        {"to": 200},
                        {"from": 200, "to": 1000},
                        {"from": 1000}
                    ]
                }
            }
        )
        a1 = a.clone()
        self.assertIsNot(a1, a)
        a = a.build_agg_result(
            {
                "buckets": [
                    {
                        "to": 200,
                        "doc_count": 12
                    },
                    {
                        "from": 200,
                        "to": 1000,
                        "doc_count": 197
                    },
                    {
                        "from": 1000,
                        "doc_count": 8
                    }
                ]
            }
        )
        self.assertEqual(len(a.buckets), 3)
        self.assertEqual(a.buckets[0].doc_count, 12)
        self.assertEqual(a.buckets[1].doc_count, 197)
        self.assertEqual(a.buckets[2].doc_count, 8)

        a = agg.Filters([Term(f.body, 'error'), Term(f.body, 'warning')])
        self.assert_expression(
            a,
            {
                "filters": {
                    "filters": [
                        {"term": {"body": "error"}},
                        {"term": {"body": "warning"}}
                    ]
                }
            }
        )
        a = a.build_agg_result(
            {
                "buckets": [
                    {
                        "doc_count" : 34
                    },
                    {
                        "doc_count" : 439
                    },
                ]
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertIs(a.buckets[0].key, None)
        self.assertEqual(a.buckets[0].doc_count, 34)
        self.assertIs(a.buckets[1].key, None)
        self.assertEqual(a.buckets[1].doc_count, 439)
        self.assertIs(a.get_bucket(None), None)

        a = agg.Filters(Params(errors=Term(f.body, 'error'), warnings=Term(f.body, 'warning')))
        self.assert_expression(
            a,
            {
                "filters": {
                    "filters": {
                        "errors": {"term": {"body": "error"}},
                        "warnings": {"term": {"body": "warning"}}
                    }
                }
            }
        )
        a = a.build_agg_result(
            {
                "buckets": {
                    "errors": {
                        "doc_count" : 34
                    },
                    "warnings": {
                        "doc_count" : 439
                    },
                }
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertEqual(a.buckets[0].key, 'errors')
        self.assertEqual(a.buckets[0].doc_count, 34)
        self.assertIs(a.buckets[0], a.get_bucket('errors'))
        self.assertEqual(a.buckets[1].key, 'warnings')
        self.assertEqual(a.buckets[1].doc_count, 439)
        self.assertIs(a.buckets[1], a.get_bucket('warnings'))

        a = agg.Nested(f.resellers, aggs={'min_price': agg.Min(f.resellers.price)})
        self.assert_expression(
            a,
            {
                "nested": {"path": "resellers"},
                "aggregations": {
                    "min_price": {"min": {"field": "resellers.price"}}
                }
            }
        )
        a = a.build_agg_result(
            {
                "min_price": {
                    "value" : 350
                }
            }
        )
        self.assertEqual(a.get_aggregation('min_price').value, 350)

        a = agg.Nested(
            f.resellers,
            aggs={"resellers": agg.Terms(
                f.resellers.id,
                aggs={"reverse": agg.ReverseNested(
                    f.resellers,
                    aggs={"reseller_volume": agg.Sum(f.price)}
                )})})
        self.assert_expression(
            a,
            {
                "nested": {"path": "resellers"},
                "aggregations": {
                    "resellers": {
                        "terms": {"field": "resellers.id"},
                        "aggregations": {
                            "reverse": {
                                "reverse_nested": {"path": "resellers"},
                                "aggregations": {
                                    "reseller_volume": {
                                        "sum": {"field": "price"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )
        a = a.build_agg_result(
            {
                "doc_count": 100,
                "resellers": {
                    "buckets": [
                        {
                            "key": 1122,
                            "doc_count": 48,
                            "reverse": {
                                "reseller_volume": {"value": 500100}
                            }
                        },
                        {
                            "key": 2233,
                            "doc_count": 52,
                            "reverse": {
                                "reseller_volume": {"value": 100500}
                            }
                        }
                    ]
                }
            }
        )
        self.assertEqual(
            a
            .get_aggregation("resellers")
            .buckets[1]
            .get_aggregation("reverse")
            .get_aggregation("reseller_volume")
            .value,
            100500
        )

        a = agg.Sampler(shard_size=1000, aggs={'avg_price': agg.Avg(f.price)})
        self.assert_expression(
            a,
            {
                "sampler": {"shard_size": 1000},
                "aggregations": {
                    "avg_price": {"avg": {"field": "price"}}
                }
            }
        )
        a = a.build_agg_result(
            {
                "doc_count": 1000,
                "avg_price": {
                    "value" : 750
                }
            }
        )
        self.assertEqual(a.doc_count, 1000)
        self.assertEqual(a.get_aggregation('avg_price').value, 750)
        
        # complex aggregation with sub aggregations
        a = agg.Global()
        a = a.aggs({
                'selling_type': agg.Terms(
                    f.selling_type,
                    aggs={
                        'price_avg': agg.Avg(f.price),
                        'price_min': agg.Min(f.price),
                        'price_max': agg.Max(f.price),
                        'price_hist': agg.Histogram(f.price, interval=50),
                    }
                ),
                'price_avg': agg.Avg(f.price),
            }
        )
        self.assert_expression(
            a,
            {
                "global": {},
                "aggregations": {
                    "selling_type": {
                        "terms": {"field": "selling_type"},
                        "aggregations": {
                            "price_avg": {"avg": {"field": "price"}},
                            "price_min": {"min": {"field": "price"}},
                            "price_max": {"max": {"field": "price"}},
                            "price_hist": {
                                "histogram": {
                                    "field": "price",
                                    "interval": 50,
                                }
                            },
                        }
                    },
                    "price_avg": {"avg": {"field": "price"}}
                }
            }
        )
        a = a.build_agg_result(
            {
                'doc_count': 100,
                'selling_type': {
                    'buckets': [
                        {
                            'key': 'retail',
                            'doc_count': 70,
                            'price_avg': {'value': 60.5},
                            'price_min': {'value': 1.1},
                            'price_max': {'value': 83.4},
                            'price_hist': {
                                'buckets': [
                                    {'key': 50, 'doc_count': 60},
                                    {'key': 100, 'doc_count': 7},
                                    {'key': 150, 'doc_count': 3},
                                ]
                            },
                        },
                        {
                            'key': 'wholesale',
                            'doc_count': 30,
                            'price_avg': {'value': 47.9},
                            'price_min': {'value': 20.1},
                            'price_max': {'value': 64.8},
                            'price_hist': {
                                'buckets': [
                                    {'key': 0, 'doc_count': 17},
                                    {'key': 50, 'doc_count': 5},
                                    {'key': 100, 'doc_count': 6},
                                    {'key': 150, 'doc_count': 2},
                                ]
                            },
                        },
                    ],
                },
                'price_avg': {'value': 56.3},
            }
        )
        self.assertEqual(a.doc_count, 100)
        type_agg = a.get_aggregation('selling_type')
        self.assertEqual(len(type_agg.buckets), 2)
        self.assertEqual(type_agg.buckets[0].key, 'retail')
        self.assertEqual(type_agg.buckets[0].doc_count, 70)
        self.assertIs(type_agg.buckets[0], type_agg.get_bucket('retail'))
        self.assertAlmostEqual(type_agg.buckets[0].get_aggregation('price_avg').value, 60.5)
        self.assertAlmostEqual(type_agg.buckets[0].get_aggregation('price_min').value, 1.1)
        self.assertAlmostEqual(type_agg.buckets[0].get_aggregation('price_max').value, 83.4)
        price_hist_agg = type_agg.buckets[0].get_aggregation('price_hist')
        self.assertEqual(price_hist_agg.buckets[0].key, 50)
        self.assertEqual(price_hist_agg.buckets[0].doc_count, 60)
        self.assertIs(price_hist_agg.buckets[0], price_hist_agg.get_bucket(50))
        self.assertEqual(price_hist_agg.buckets[1].key, 100)
        self.assertEqual(price_hist_agg.buckets[1].doc_count, 7)
        self.assertIs(price_hist_agg.buckets[1], price_hist_agg.get_bucket(100))
        self.assertEqual(price_hist_agg.buckets[2].key, 150)
        self.assertEqual(price_hist_agg.buckets[2].doc_count, 3)
        self.assertIs(price_hist_agg.buckets[2], price_hist_agg.get_bucket(150))
        self.assertEqual(len(price_hist_agg.buckets), 3)
        self.assertEqual(type_agg.buckets[1].key, 'wholesale')
        self.assertEqual(type_agg.buckets[1].doc_count, 30)
        self.assertIs(type_agg.buckets[1], type_agg.get_bucket('wholesale'))
        self.assertAlmostEqual(type_agg.buckets[1].get_aggregation('price_avg').value, 47.9)
        self.assertAlmostEqual(type_agg.buckets[1].get_aggregation('price_min').value, 20.1)
        self.assertAlmostEqual(type_agg.buckets[1].get_aggregation('price_max').value, 64.8)
        price_hist_agg = type_agg.buckets[1].get_aggregation('price_hist')
        self.assertEqual(len(price_hist_agg.buckets), 4)
        self.assertEqual(price_hist_agg.buckets[0].key, 0)
        self.assertEqual(price_hist_agg.buckets[0].doc_count, 17)
        self.assertIs(price_hist_agg.buckets[0], price_hist_agg.get_bucket(0))
        self.assertEqual(price_hist_agg.buckets[1].key, 50)
        self.assertEqual(price_hist_agg.buckets[1].doc_count, 5)
        self.assertIs(price_hist_agg.buckets[1], price_hist_agg.get_bucket(50))
        self.assertEqual(price_hist_agg.buckets[2].key, 100)
        self.assertEqual(price_hist_agg.buckets[2].doc_count, 6)
        self.assertIs(price_hist_agg.buckets[2], price_hist_agg.get_bucket(100))
        self.assertEqual(price_hist_agg.buckets[3].key, 150)
        self.assertEqual(price_hist_agg.buckets[3].doc_count, 2)
        self.assertIs(price_hist_agg.buckets[3], price_hist_agg.get_bucket(150))
        self.assertEqual(a.get_aggregation('price_avg').value, 56.3)

        class QuestionDocument(DynamicDocument):
            pass
        
        class PaperDocument(DynamicDocument):
            pass
        
        question_mapper = Mock(
            return_value={
                '602679': Mock(id=602679, type='question'),
                '602678': Mock(id=602678, type='question'),
            }
        )
        paper_mapper = Mock(return_value={'602672': Mock(id=602672, type='paper')})
        top_hits_agg = agg.Terms(
            f.tags,
            size=3,
            aggs={
                'top_tags_hits': agg.TopHits(
                    size=1,
                    sort=f.last_activity_date.desc(),
                    _source={'include': f.title},
                    instance_mapper={
                        QuestionDocument: question_mapper, PaperDocument: paper_mapper
                    },
                )
            }
        )
        self.assert_expression(
            top_hits_agg,
            {
                "terms": {
                    "field": "tags",
                    "size": 3
                },
                "aggregations": {
                    "top_tags_hits": {
                        "top_hits": {
                            "sort": {
                                "last_activity_date": "desc"
                            },
                            "_source": {
                                "include": "title"
                            },
                            "size" : 1
                        }
                    }
                }
            }
        )
        a = top_hits_agg.build_agg_result(
            {
                "buckets": [
                    {
                        "key": "windows-7",
                        "doc_count": 25365,
                        "top_tags_hits": {
                            "hits": {
                                "total": 25365,
                                "max_score": 1,
                                "hits": [
                                    {
                                        "_index": "stack",
                                        "_type": "question",
                                        "_id": "602679",
                                        "_score": 1,
                                        "_source": {
                                            "title": "Windows port opening"
                                        },
                                        "sort": [
                                            1370143231177
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "key": "linux",
                        "doc_count": 18342,
                        "top_tags_hits": {
                            "hits": {
                                "total": 18342,
                                "max_score": 1,
                                "hits": [
                                    {
                                        "_index": "stack",
                                        "_type": "paper",
                                        "_id": "602672",
                                        "_score": 1,
                                        "_source": {
                                            "title": "Ubuntu RFID Screensaver lock-unlock"
                                        },
                                        "sort": [
                                            1370143379747
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "key": "windows",
                        "doc_count": 18119,
                        "top_tags_hits": {
                            "hits": {
                                "total": 18119,
                                "max_score": 1,
                                "hits": [
                                    {
                                        "_index": "stack",
                                        "_type": "question",
                                        "_id": "602678",
                                        "_score": 1,
                                        "_source": {
                                            "title": "If I change my computers date / time, what could be affected?"
                                        },
                                        "sort": [
                                            1370142868283
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            doc_cls_map={'question': QuestionDocument, 'paper': PaperDocument},
            mapper_registry={},
        )
        self.assertEqual(len(a.buckets), 3)
        self.assertEqual(a.buckets[0].doc_count, 25365)
        self.assertEqual(a.buckets[0].key, 'windows-7')
        top_tags_agg = a.buckets[0].get_aggregation('top_tags_hits')
        self.assertEqual(top_tags_agg.total, 25365)
        self.assertEqual(top_tags_agg.max_score, 1)
        self.assertEqual(len(top_tags_agg.hits), 1)
        self.assertIsInstance(top_tags_agg.hits[0], QuestionDocument)
        self.assertEqual(top_tags_agg.hits[0]._index, 'stack')
        self.assertEqual(top_tags_agg.hits[0]._type, 'question')
        self.assertEqual(top_tags_agg.hits[0]._score, 1)
        self.assertEqual(top_tags_agg.hits[0]._id, '602679')
        self.assertEqual(top_tags_agg.hits[0].title, 'Windows port opening')
        self.assertEqual(top_tags_agg.hits[0].instance.id, 602679)
        self.assertEqual(top_tags_agg.hits[0].instance.type, 'question')
        self.assertEqual(a.buckets[1].doc_count, 18342)
        self.assertEqual(a.buckets[1].key, 'linux')
        top_tags_agg = a.buckets[1].get_aggregation('top_tags_hits')
        self.assertEqual(top_tags_agg.total, 18342)
        self.assertEqual(top_tags_agg.max_score, 1)
        self.assertEqual(len(top_tags_agg.hits), 1)
        self.assertIsInstance(top_tags_agg.hits[0], PaperDocument)
        self.assertEqual(top_tags_agg.hits[0]._index, 'stack')
        self.assertEqual(top_tags_agg.hits[0]._type, 'paper')
        self.assertEqual(top_tags_agg.hits[0]._score, 1)
        self.assertEqual(top_tags_agg.hits[0]._id, '602672')
        self.assertEqual(top_tags_agg.hits[0].title, 'Ubuntu RFID Screensaver lock-unlock')
        self.assertEqual(top_tags_agg.hits[0].instance.id, 602672)
        self.assertEqual(top_tags_agg.hits[0].instance.type, 'paper')
        self.assertEqual(a.buckets[2].doc_count, 18119)
        self.assertEqual(a.buckets[2].key, 'windows')
        top_tags_agg = a.buckets[2].get_aggregation('top_tags_hits')
        self.assertEqual(top_tags_agg.total, 18119)
        self.assertEqual(top_tags_agg.max_score, 1)
        self.assertEqual(len(top_tags_agg.hits), 1)
        self.assertIsInstance(top_tags_agg.hits[0], DynamicDocument)
        self.assertEqual(top_tags_agg.hits[0]._index, 'stack')
        self.assertEqual(top_tags_agg.hits[0]._type, 'question')
        self.assertEqual(top_tags_agg.hits[0]._score, 1)
        self.assertEqual(top_tags_agg.hits[0]._id, '602678')
        self.assertEqual(top_tags_agg.hits[0].title, 'If I change my computers date / time, what could be affected?')
        self.assertEqual(top_tags_agg.hits[0].instance.id, 602678)
        self.assertEqual(top_tags_agg.hits[0].instance.type, 'question')
        self.assertEqual(question_mapper.call_count, 1)
        self.assertEqual(paper_mapper.call_count, 1)

    def test_instance_mapper(self):
        class _Gender(object):
            def __init__(self, key, title):
                self.key = key
                self.title = title

        Male = _Gender('m', 'Male')
        Female = _Gender('f', 'Female')
        GENDERS = {g.key: g for g in [Male, Female]}

        f = DynamicDocument.fields

        gender_mapper = Mock(return_value=GENDERS)
        a = agg.Terms(f.gender, instance_mapper=gender_mapper)
        a = a.build_agg_result(
            {
                "buckets": [
                    {
                        "key": "m",
                        "doc_count": 10
                    },
                    {
                        "key": "f",
                        "doc_count": 10
                    },
                ]
            }
        )
        self.assertEqual(len(a.buckets), 2)
        self.assertEqual(a.buckets[0].instance.title, 'Male')
        self.assertEqual(a.buckets[1].instance.title, 'Female')
        self.assertEqual(gender_mapper.call_count, 1)

        gender_mapper = Mock(return_value=GENDERS)
        a = agg.Global(
            aggs={
                'all_genders': agg.Terms(f.gender, instance_mapper=gender_mapper),
                'all_salary': agg.Range(
                    f.month_salary,
                    ranges=[
                        {'to': 1000},
                        {'from': 1000, 'to': 2000},
                        {'from': 2000, 'to': 3000},
                        {'from': 3000},
                    ],
                    aggs={
                        'gender': agg.Terms(f.gender, instance_mapper=gender_mapper)
                    }
                )
            }
        )
        a = a.build_agg_result(
            {
                "doc_count": 1819,
                "all_genders": {
                    "buckets": [
                        {
                            "key": "m",
                            "doc_count": 1212
                        },
                        {
                            "key": "f",
                            "doc_count": 607
                        }
                    ]
                },
                "all_salary": {
                    "buckets": [
                        {
                            "to": 1000,
                            "doc_count": 183,
                            "gender": {
                                "buckets": [
                                    {
                                        "key": "f",
                                        "doc_count": 101
                                    },
                                    {
                                        "key": "m",
                                        "doc_count": 82
                                    }
                                ]
                            }
                        },
                        {
                            "from": 1000,
                            "to": 2000,
                            "doc_count": 456,
                            "gender": {
                                "buckets": [
                                    {
                                        "key": "f",
                                        "doc_count": 231
                                    },
                                    {
                                        "key": "m",
                                        "doc_count": 225
                                    }
                                ]
                            }
                        },
                        {
                            "from": 2000,
                            "to": 3000,
                            "doc_count": 1158,
                            "gender": {
                                "buckets": [
                                    {
                                        "key": "m",
                                        "doc_count": 894
                                    },
                                    {
                                        "key": "f",
                                        "doc_count": 264
                                    }
                                ]
                            }
                        },
                        {
                            "from": 3000,
                            "doc_count": 22,
                            "gender": {
                                "buckets": [
                                    {
                                        "key": "m",
                                        "doc_count": 11
                                    },
                                    {
                                        "key": "f",
                                        "doc_count": 11
                                    }
                                ]
                            }
                        },
                    ]
                }
            },
            mapper_registry={}
        )
        self.assertEqual(a.doc_count, 1819)
        all_genders_agg = a.get_aggregation('all_genders')
        self.assertEqual(len(all_genders_agg.buckets), 2)
        self.assertEqual(all_genders_agg.buckets[0].key, 'm')
        self.assertEqual(all_genders_agg.buckets[0].doc_count, 1212)
        self.assertEqual(all_genders_agg.buckets[0].instance.title, 'Male')
        self.assertEqual(all_genders_agg.buckets[1].key, 'f')
        self.assertEqual(all_genders_agg.buckets[1].doc_count, 607)
        self.assertEqual(all_genders_agg.buckets[1].instance.title, 'Female')
        all_salary_agg = a.get_aggregation('all_salary')
        self.assertEqual(len(all_salary_agg.buckets), 4)
        self.assertIs(all_salary_agg.buckets[0].from_, None)
        self.assertEqual(all_salary_agg.buckets[0].to, 1000)
        self.assertEqual(all_salary_agg.buckets[0].doc_count, 183)
        gender_agg = all_salary_agg.buckets[0].get_aggregation('gender')
        self.assertEqual(len(gender_agg.buckets), 2)
        self.assertEqual(gender_agg.buckets[0].key, 'f')
        self.assertEqual(gender_agg.buckets[0].doc_count, 101)
        self.assertEqual(gender_agg.buckets[0].instance.title, 'Female')
        self.assertEqual(gender_agg.buckets[1].key, 'm')
        self.assertEqual(gender_agg.buckets[1].doc_count, 82)
        self.assertEqual(gender_agg.buckets[1].instance.title, 'Male')
        self.assertEqual(all_salary_agg.buckets[1].from_, 1000)
        self.assertEqual(all_salary_agg.buckets[1].to, 2000)
        self.assertEqual(all_salary_agg.buckets[1].doc_count, 456)
        gender_agg = all_salary_agg.buckets[1].get_aggregation('gender')
        self.assertEqual(len(gender_agg.buckets), 2)
        self.assertEqual(gender_agg.buckets[0].key, 'f')
        self.assertEqual(gender_agg.buckets[0].doc_count, 231)
        self.assertEqual(gender_agg.buckets[0].instance.title, 'Female')
        self.assertEqual(gender_agg.buckets[1].key, 'm')
        self.assertEqual(gender_agg.buckets[1].doc_count, 225)
        self.assertEqual(gender_agg.buckets[1].instance.title, 'Male')
        self.assertEqual(gender_mapper.call_count, 1)


@pytest.mark.parametrize('compiler', [Compiler_6_0, Compiler_7_0])
def test_bucket_script(compiler):
    f = DynamicDocument.fields
    a = agg.DateHistogram(
        f.date,
        interval='month',
        aggs={
            'total_sales': agg.Sum(f.price),
            't_shirts': agg.Filter(
                f.type == 't-shirt',
                aggs={
                    'sales': agg.Sum(f.price),
                }
            ),
            't_shirt_percentage': agg.BucketScript(
                buckets_path={
                    't_shirt_sales': 't_shirts>sales',
                    'total_sales': 'total_sales',
                },
                script=Script('params.t_shirt_sales / params.total_sales * 100')
            )
        }
    )
    assert a.compile(compiler).body == {
        'date_histogram': {
            'field': 'date',
            'interval': 'month',
        },
        'aggregations': {
            'total_sales': {
                'sum': {
                    'field': 'price'
                }
            },
            't_shirts': {
                'filter': {
                    'term': {
                        'type': 't-shirt'
                    }
                },
                'aggregations': {
                    'sales': {
                        'sum': {
                            'field': 'price'
                        }
                    }
                }
            },
            't_shirt_percentage': {
                'bucket_script': {
                    'buckets_path': {
                        't_shirt_sales': 't_shirts>sales',
                        'total_sales': 'total_sales',
                    },
                    'script': {
                        'source': 'params.t_shirt_sales / params.total_sales * 100'
                    }
                }
            }
        }
    }

    r = a.build_agg_result({
        "buckets": [
            {
                "key_as_string": "2015/01/01 00:00:00",
                "key": 1420070400000,
                "doc_count": 3,
                "total_sales": {
                    "value": 50
                },
                "t_shirts": {
                    "doc_count": 2,
                    "sales": {
                        "value": 10
                    }
                },
                "t_shirt_percentage": {
                    "value": 20
                }
            },
            {
                "key_as_string": "2015/02/01 00:00:00",
                "key": 1422748800000,
                "doc_count": 2,
                "total_sales": {
                    "value": 60
                },
                "t_shirts": {
                    "doc_count": 1,
                    "sales": {
                        "value": 15
                    }
                },
                "t_shirt_percentage": {
                    "value": 25
                }
            },
            {
                "key_as_string": "2015/03/01 00:00:00",
                "key": 1425168000000,
                "doc_count": 2,
                "total_sales": {
                    "value": 40
                },
                "t_shirts": {
                    "doc_count": 1,
                    "sales": {
                        "value": 20
                    }
                },
                "t_shirt_percentage": {
                    "value": 50
                }
            }
        ]
    })

    assert len(r.buckets) == 3
    assert r.buckets[0].get_aggregation('t_shirt_percentage').value == 20
    assert r.buckets[1].get_aggregation('t_shirt_percentage').value == 25
    assert r.buckets[2].get_aggregation('t_shirt_percentage').value == 50


@pytest.mark.parametrize('compiler', [Compiler_6_0, Compiler_7_0])
def test_bucket_selector(compiler):
    f = DynamicDocument.fields
    a = agg.DateHistogram(
        f.date,
        interval='month',
        aggs={
            'total_sales': agg.Sum(f.price),
            'sales_bucket_filter': agg.BucketSelector(
                buckets_path={
                    'total_sales': 'total_sales',
                },
                script=Script('params.total_sales > 200')
            )
        }
    )
    assert a.compile(compiler).body == {
        'date_histogram': {
            'field': 'date',
            'interval': 'month',
        },
        'aggregations': {
            'total_sales': {
                'sum': {
                    'field': 'price'
                }
            },
            'sales_bucket_filter': {
                'bucket_selector': {
                    'buckets_path': {
                        'total_sales': 'total_sales',
                    },
                    'script': {
                        'source': 'params.total_sales > 200'
                    }
                }
            }
        }
    }

    r = a.build_agg_result({
        "buckets": [
            {
                "key_as_string": "2015/01/01 00:00:00",
                "key": 1420070400000,
                "doc_count": 3,
                "total_sales": {
                    "value": 550.0
                }
            },
            {
                "key_as_string": "2015/03/01 00:00:00",
                "key": 1425168000000,
                "doc_count": 2,
                "total_sales": {
                    "value": 375.0
                },
            }
        ]
    })

    assert len(r.buckets) == 2
    assert r.buckets[0].get_aggregation('sales_bucket_filter') is None
    assert r.buckets[1].get_aggregation('sales_bucket_filter') is None
