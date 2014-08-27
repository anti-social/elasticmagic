import unittest

from elasticmagic import agg, Term
from elasticmagic.expression import Fields


class AggregationTest(unittest.TestCase):
    def test_aggs(self):
        f = Fields()

        a = agg.Avg(f.price)
        a.process_results({
            'value': 75.3
        })
        self.assertAlmostEqual(a.value, 75.3)

        a = agg.Terms(f.status)
        a.process_results(
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
        self.assertEqual(a.buckets[0].key, 0)
        self.assertEqual(a.buckets[0].doc_count, 7353499)
        self.assertEqual(a.buckets[1].key, 1)
        self.assertEqual(a.buckets[1].doc_count, 2267139)
        self.assertEqual(a.buckets[2].key, 4)
        self.assertEqual(a.buckets[2].doc_count, 1036951)
        self.assertEqual(a.buckets[3].key, 2)
        self.assertEqual(a.buckets[3].doc_count, 438384)
        self.assertEqual(a.buckets[4].key, 3)
        self.assertEqual(a.buckets[4].doc_count, 9594)
        self.assertEqual(a.buckets[5].key, 5)
        self.assertEqual(a.buckets[5].doc_count, 46)

        a = agg.SignificantTerms(f.crime_type)
        a.process_results(
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
        self.assertEqual(a.buckets[1].key, 'Mobile phone theft')
        self.assertEqual(a.buckets[1].doc_count, 27617)
        self.assertAlmostEqual(a.buckets[1].score, 0.0599)
        self.assertEqual(a.buckets[1].bg_count, 53182)

        a = agg.Filters([Term(f.body, 'error'), Term(f.body, 'warning')])
        a.process_results(
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

        a = agg.Filters({'errors': Term(f.body, 'error'), 'warnings': Term(f.body, 'warning')})
        a.process_results(
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
        self.assertIs(a.buckets[0].key, 'errors')
        self.assertEqual(a.buckets[0].doc_count, 34)
        self.assertIs(a.buckets[1].key, 'warnings')
        self.assertEqual(a.buckets[1].doc_count, 439)

        a = agg.Global(
            aggs={
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
        a.process_results(
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
        self.assertAlmostEqual(type_agg.buckets[0].get_aggregation('price_avg').value, 60.5)
        self.assertAlmostEqual(type_agg.buckets[0].get_aggregation('price_min').value, 1.1)
        self.assertAlmostEqual(type_agg.buckets[0].get_aggregation('price_max').value, 83.4)
        price_hist_agg = type_agg.buckets[0].get_aggregation('price_hist')
        self.assertEqual(price_hist_agg.buckets[0].key, 50)
        self.assertEqual(price_hist_agg.buckets[0].doc_count, 60)
        self.assertEqual(price_hist_agg.buckets[1].key, 100)
        self.assertEqual(price_hist_agg.buckets[1].doc_count, 7)
        self.assertEqual(price_hist_agg.buckets[2].key, 150)
        self.assertEqual(price_hist_agg.buckets[2].doc_count, 3)
        self.assertEqual(len(price_hist_agg.buckets), 3)
        self.assertEqual(type_agg.buckets[1].key, 'wholesale')
        self.assertEqual(type_agg.buckets[1].doc_count, 30)
        self.assertAlmostEqual(type_agg.buckets[1].get_aggregation('price_avg').value, 47.9)
        self.assertAlmostEqual(type_agg.buckets[1].get_aggregation('price_min').value, 20.1)
        self.assertAlmostEqual(type_agg.buckets[1].get_aggregation('price_max').value, 64.8)
        price_hist_agg = type_agg.buckets[1].get_aggregation('price_hist')
        self.assertEqual(len(price_hist_agg.buckets), 4)
        self.assertEqual(price_hist_agg.buckets[0].key, 0)
        self.assertEqual(price_hist_agg.buckets[0].doc_count, 17)
        self.assertEqual(price_hist_agg.buckets[1].key, 50)
        self.assertEqual(price_hist_agg.buckets[1].doc_count, 5)
        self.assertEqual(price_hist_agg.buckets[2].key, 100)
        self.assertEqual(price_hist_agg.buckets[2].doc_count, 6)
        self.assertEqual(price_hist_agg.buckets[3].key, 150)
        self.assertEqual(price_hist_agg.buckets[3].doc_count, 2)
        self.assertEqual(a.get_aggregation('price_avg').value, 56.3)

        return
        a = agg.Global(
            aggs={
                'selling_type': agg.Terms(
                    f.selling_type,
                    aggs={
                        'price_avg': agg.Avg(f.price),
                        'price_min': agg.Min(f.price),
                        'price_max': agg.Max(f.price),
                    }
                )
            }
        )
        a = agg.Global(
            selling_type=agg.Terms(
                f.selling_type,
                price_avg=agg.Avg(f.price),
                price_min=agg.Min(f.price),
                price_max=agg.Max(f.price),
            )
        )
        # a = agg.Global(
        #     agg.Terms('selling_type', f.selling_type, aggs=)
        # )
