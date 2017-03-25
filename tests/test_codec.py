import unittest

from mock import Mock

import pytest

from elasticmagic.types import Integer, Long, Float, Boolean
from elasticmagic.ext.queryfilter.codec import BaseCodec, SimpleCodec


def test_base_codec():
    codec = BaseCodec()
    with pytest.raises(NotImplementedError):
        codec.decode({})
    with pytest.raises(NotImplementedError):
        codec.encode({})
    with pytest.raises(NotImplementedError):
        codec.decode_value('1')
    with pytest.raises(NotImplementedError):
        codec.encode_value(1)


def test_simple_codec_decode():
    codec = SimpleCodec()
    assert \
        codec.decode({'country': ['ru', 'ua', 'null']}) == \
        {
            'country': {
                'exact': [['ru'], ['ua'], [None]],
            }
        }
    # Webob's MultiDict
    assert \
        codec.decode(
            Mock(
                spec=['dict_of_lists'],
                dict_of_lists=Mock(
                    return_value={'country': ['ru', 'ua', 'null']}
                )
            )
        ) == \
        {
            'country': {
                'exact': [['ru'], ['ua'], [None]],
            }
        }
    # Django's QueryDict
    assert \
        codec.decode(
            Mock(
                spec=['lists'],
                lists=Mock(
                    return_value=[('country', ['ru', 'ua', 'null'])]
                )
            )
        ) == \
        {
            'country': {
                'exact': [['ru'], ['ua'], [None]],
            }
        }
    assert \
        codec.decode({'country': ['ru', 'ua', 'null']}) == \
        {
            'country': {
                'exact': [['ru'], ['ua'], [None]],
            }
        }
    assert \
        codec.decode({'category': ['5', '6:a', 'b:c', 'null']},
                     {'category': [Integer]}) == \
        {
            'category': {
                'exact': [[5], [6, 'a'], [None]]
            }
        }
    assert \
        codec.decode({'category': ['5']},
                     {'category': [Integer, Boolean]}) == \
        {
            'category': {
                'exact': [[5]]
            }
        }
    assert \
        codec.decode({'manu': ['1:nokia:true', '2:samsung:false']},
                     {'manu': [Integer, None, Boolean]}) == \
        {
            'manu': {
                'exact': [[1, 'nokia', True], [2, 'samsung', False]],
            }
        }
    assert \
        codec.decode({'is_active': ['true']}, {'is_active': Boolean}) == \
        {
            'is_active': {
                'exact': [[True]],
            }
        }
    assert \
        codec.decode(
            [('price__gte', ['100.1', '101.0']), ('price__lte', ['200'])],
            {'price': Float}
        ) == \
        {
            'price': {
                'gte': [[100.1], [101.0]],
                'lte': [[200.0]],
            }
        }
    assert \
        codec.decode({'price__lte': '123a:bc'}, {'price': [Float]}) == \
        {}
    assert \
        codec.decode({'price__gte': 'Inf', 'price__lte': 'NaN'},
                     {'price': [Float]}) == \
        {}
    assert \
        codec.decode({'size': '{}'.format(2 ** 31)}, {'size': [Integer]}) == \
        {}
    assert \
        codec.decode({'size': '{}'.format(2 ** 31)}, {'size': [Long]}) == \
        {
            'size': {'exact': [[2147483648]]}
        }
    assert \
        codec.decode({'size': '{}'.format(2 ** 63)}, {'size': [Long]}) == \
        {}
    with pytest.raises(TypeError):
        codec.decode('')

def test_simple_codec_encode():
    codec = SimpleCodec()

    assert codec.encode_value(1) == '1'
    assert codec.encode_value(1.1) == '1.1'
    assert codec.encode_value(True) == 'true'
    assert codec.encode_value(False) == 'false'
    assert codec.encode_value(None) == 'null'

    assert \
        codec.encode(
            {
                'country': {
                    'exact': [['ru'], ['ua'], [None]],
                }
            }
        ) == \
        {'country': ['ru', 'ua', 'null']}
    assert \
        codec.encode(
            {
                'price': {
                    'gte': [[100.1], [101.0]],
                    'lte': [[200.0]],
                }
            }
        ) == \
        {
            'price__gte': ['100.1', '101.0'],
            'price__lte': ['200.0'],
        }
    assert \
        codec.encode(
            {
                'price': {
                    'gte': [[100.1], [101.0]],
                    'lte': [[200.0]],
                }
            },
            {'price': Integer}
        ) == \
        {
            'price__gte': ['100', '101'],
            'price__lte': ['200'],
        }
