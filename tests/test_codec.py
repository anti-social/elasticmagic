import unittest

from elasticmagic.types import Integer, Float, Boolean
from elasticmagic.ext.queryfilter.codec import SimpleCodec


class SimpleCodecTest(unittest.TestCase):
    def test_decode(self):
        codec = SimpleCodec()
        self.assertEqual(
            codec.decode({'country': ['ru', 'ua', 'null']}),
            {
                'country': {
                    'exact': [['ru'], ['ua'], [None]],
                }
            }
        )
        self.assertEqual(
            codec.decode({'category': ['5', '6:a', 'b:c', 'null']}, {'category': [Integer]}),
            {
                'category': {
                    'exact': [[5], [6, 'a'], [None]]
                }
            }
        )
        self.assertEqual(
            codec.decode({'manu': ['1:nokia:true', '2:samsung:false']}, {'manu': [Integer, None, Boolean]}),
            {
                'manu': {
                    'exact': [[1, 'nokia', True], [2, 'samsung', False]],
                }
            }
        )
        self.assertEqual(
            codec.decode({'is_active': ['true']}, {'is_active': Boolean}),
            {
                'is_active': {
                    'exact': [[True]],
                }
            }
        )
        self.assertEqual(
            codec.decode([('price__gte', ['100.1', '101.0']), ('price__lte', ['200'])], {'price': Float}),
            {
                'price': {
                    'gte': [[100.1], [101.0]],
                    'lte': [[200.0]],
                }
            }
        )
        self.assertEqual(
            codec.decode({'price__lte': '123a:bc'}, {'price': [Float]}),
            {}
        )
        self.assertRaises(TypeError, lambda: codec.decode(''))
