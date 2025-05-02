from datetime import date, datetime
from unittest.mock import Mock

import pytest

from elasticmagic.types import Integer, Long, Float, Boolean, Date, List
from elasticmagic.ext.queryfilter.codec import BaseCodec, SimpleCodec


def test_base_codec():
    codec = BaseCodec()
    with pytest.raises(NotImplementedError):
        codec.decode({})
    with pytest.raises(NotImplementedError):
        codec.encode({})
    with pytest.raises(NotImplementedError):
        codec.decode_value('1', None)
    with pytest.raises(NotImplementedError):
        codec.encode_value(1, None)


def test_simple_codec_decode():
    codec = SimpleCodec()
    assert \
        codec.decode({'category_id': [1, '2', 'null']}) == \
        {
            'category_id': {
                'exact': ['1', '2', None],
            }
        }
    assert \
        codec.decode({'country': ['ru', 'ua', 'null']}) == \
        {
            'country': {
                'exact': ['ru', 'ua', None],
            }
        }
    # Webob's MultiDict
    data = {'country': ['ru', 'ua', 'null']}
    assert \
        codec.decode(
            Mock(
                spec=['dict_of_lists'],
                dict_of_lists=Mock(
                    return_value=data
                ),
                getall=Mock(
                    return_value=data
                )
            )
        ) == \
        {
            'country': {
                'exact': ['ru', 'ua', None],
            }
        }
    # Django's QueryDict
    assert \
        codec.decode(
            Mock(
                spec=['lists'],
                getlist=Mock(),
                lists=Mock(
                    return_value=data
                )
            )
        ) == \
        {
            'country': {
                'exact': ['ru', 'ua', None],
            }
        }
    assert \
        codec.decode({'country': ['ru', 'ua', 'null']}) == \
        {
            'country': {
                'exact': ['ru', 'ua', None],
            }
        }
    assert \
        codec.decode({'category': ['5', '6', 'a', 'null']},
                     {'category': Integer}) == \
        {
            'category': {
                'exact': [5, 6, None]
            }
        }
    assert \
        codec.decode({'is_available': ['true', 'false', '', 'null']},
                     {'is_available': Boolean}) == \
        {
            'is_available': {
                'exact': [True, False, None]
            }
        }
    assert \
        codec.decode({'manu': ['nokia', 'samsung']},
                     {'manu': None}) == \
        {
            'manu': {
                'exact': ['nokia', 'samsung'],
            }
        }
    assert \
        codec.decode({'is_active': ['true']}, {'is_active': Boolean}) == \
        {
            'is_active': {
                'exact': [True],
            }
        }
    assert \
        codec.decode(
            [('price__gte', ['100.1', '101.0']), ('price__lte', ['200'])],
            {'price': Float}
        ) == \
        {
            'price': {
                'gte': [100.1, 101.0],
                'lte': [200.0],
            }
        }
    assert \
        codec.decode({'price__lte': '123a:bc'}, {'price': Float}) == \
        {}
    assert \
        codec.decode({'price__gte': 'Inf', 'price__lte': 'NaN'},
                     {'price': Float}) == \
        {}
    assert \
        codec.decode({'size': '{}'.format(2 ** 31)}, {'size': Integer}) == \
        {}
    assert \
        codec.decode({'size': '{}'.format(2 ** 31)}, {'size': Long}) == \
        {
            'size': {'exact': [2147483648]}
        }
    assert \
        codec.decode({'size': '{}'.format(2 ** 63)}, {'size': Long}) == \
        {}
    with pytest.raises(TypeError):
        codec.decode('')


def test_simple_coded_decode_custom_type():
    class IntegerKeyword(Integer):
        """Integer that stored as keyword
        """

        __visit_name__ = 'keyword'

    codec = SimpleCodec()
    assert \
        codec.decode(
            {'company_id': ['123', 'asdf']},
            {'company_id': IntegerKeyword}
        ) == \
        {
            'company_id': {
                'exact': [123],
            }
        }


def test_simple_codec_encode():
    codec = SimpleCodec()

    assert \
        codec.encode(
            {
                'country': {
                    'exact': ['ru', 'ua', None],
                }
            }
        ) == \
        {'country': ['ru', 'ua', 'null']}
    assert \
        codec.encode(
            {
                'price': {
                    'gte': [100.1, 101.0],
                    'lte': [200.0],
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
                    'gte': [100.1, 101.0],
                    'lte': [200.0],
                }
            },
            {'price': Integer}
        ) == \
        {
            'price__gte': ['100', '101'],
            'price__lte': ['200'],
        }
    assert \
        codec.encode(
            {
                'category': {
                    'exact': [11, 13],
                }
            },
            {'category': List(Integer)}
        ) == \
        {
            'category': ['11', '13'],
        }
    assert \
        codec.encode(
            {
                'date_modified': {
                    'gt': [date(2019, 9, 1)]
                }
            },
            {'date_modified': Date}
        ) == \
        {
            'date_modified__gt': ['2019-09-01']
        }
    assert \
        codec.encode(
            {
                'date_modified': {
                    'gt': [datetime(2019, 9, 1, 23, 59, 59, 999999)]
                }
            },
            {'date_modified': Date}
        ) == \
        {
            'date_modified__gt': ['2019-09-01T23:59:59.999999']
        }
    assert \
        codec.encode(
            {
                'date_modified': {
                    'gt': [datetime(2019, 9, 1, 23, 59, 59, 999999)]
                }
            }
        ) == \
        {
            'date_modified__gt': ['2019-09-01 23:59:59.999999']
        }
    with pytest.raises(ValueError):
        codec.encode(
            {
                'date_modified': {
                    'gt': ['yesterday']
                }
            },
            {'date_modified': Date}
        )
    assert \
        codec.encode(
            {
                'is_available': {
                    'exact': [True]
                }
            },
            {'is_available': Boolean}
        ) == \
        {
            'is_available': ['true']
        }
