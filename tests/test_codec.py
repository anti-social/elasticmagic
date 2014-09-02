import unittest

from elasticmagic.types import Integer, Float, Boolean
from elasticmagic.ext.queryfilter.codec import SimpleCodec


class SimpleCodecTest(unittest.TestCase):
    def test_decode(self):
        codec = SimpleCodec()
        self.assertEqual(
            codec.decode({'country': ['ru', 'ua', 'null']}),
            {
                'country': [('exact', ['ru']), ('exact', ['ua']), ('exact', [None])],
                # 'country': [('exact', ['ru']), ('exact', ['ua']), ('isnull', [True])],
            }
        )
        self.assertEqual(
            codec.decode({'manu': ['1:nokia:true', '2:samsung:false']}, {'manu': [Integer, None, Boolean]}),
            {
                'manu': [('exact', [1, 'nokia', True]), ('exact', [2, 'samsung', False])],
            }
        )
        self.assertEqual(
            codec.decode({'is_active': ['true']}, {'is_active': Boolean}),
            {
                'is_active': [('exact', [True])],
            }
        )
        self.assertEqual(
            codec.decode((('price__gte', ['100.1', 'Inf']), ('price__lte', ['200', 'NaN'])), {'price': Float}),
            {
                'price': [('gte', [100.1]), ('lte', [200])],
            }
        )
        self.assertRaises(TypeError, lambda: codec.decode(''))
