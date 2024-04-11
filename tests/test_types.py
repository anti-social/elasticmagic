import binascii
import datetime

import pytest

from elasticmagic.compiler import Compiler_5_0
from elasticmagic.document import DynamicDocument
from elasticmagic.types import (
    Type, String, Byte, Short, Integer, Long, Float, Double, Date, Boolean,
    Binary, Ip, Object, List, GeoPoint, Completion, ValidationError,
)


def test_type():
    t = Type()
    assert t.to_python(None) is None
    assert t.to_python('test') == 'test'
    assert t.to_python(123) == 123
    assert t.from_python(None, Compiler_5_0) is None
    assert t.from_python('test', Compiler_5_0) == 'test'
    assert t.from_python('test', Compiler_5_0, validate=True) == 'test'
    assert t.from_python(123, Compiler_5_0, validate=True) == 123


def test_string():
    t = String()
    assert t.to_python(None) is None
    assert t.to_python('test') == 'test'
    assert t.to_python(123) == '123'
    assert t.from_python('test', Compiler_5_0) == 'test'
    assert t.from_python('test', Compiler_5_0, validate=True) == 'test'
    assert t.from_python(123, Compiler_5_0, validate=True) == '123'


def test_byte():
    t = Byte()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test', Compiler_5_0) == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(128, Compiler_5_0, validate=True)


def test_short():
    t = Short()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test', Compiler_5_0) == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 15, Compiler_5_0, validate=True)


def test_integer():
    t = Integer()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test', Compiler_5_0) == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 31, Compiler_5_0, validate=True)


def test_float():
    t = Float()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test', Compiler_5_0) == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    assert t.from_python('128', Compiler_5_0, validate=True) == \
        pytest.approx(128.0)
    assert t.from_python(128, Compiler_5_0, validate=True) == \
        pytest.approx(128.0)


def test_double():
    t = Double()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test', Compiler_5_0) == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    assert t.from_python('128', Compiler_5_0, validate=True) == \
        pytest.approx(128.0)
    assert t.from_python(128, Compiler_5_0, validate=True) == \
        pytest.approx(128.0)


def test_date():
    t = Date()
    assert t.to_python(None) is None
    assert t.to_python('2009-11-15T14:12:12') == \
        datetime.datetime(2009, 11, 15, 14, 12, 12)
    with pytest.raises(ValueError):
        t.to_python('test')
    with pytest.raises(ValueError):
        t.from_python('test', Compiler_5_0)
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    assert \
        t.from_python(
            datetime.datetime(2009, 11, 15, 14, 12, 12),
            Compiler_5_0
        ) == \
        datetime.datetime(2009, 11, 15, 14, 12, 12)


def test_boolean():
    t = Boolean()
    assert t.to_python(None) is None
    assert t.to_python(False) is False
    assert t.to_python(True) is True
    assert t.to_python(0) is False
    assert t.to_python(1) is True
    assert t.to_python('false') is False
    assert t.to_python('F') is False
    assert t.to_python('') is False
    assert t.to_python('true') is True
    assert t.to_python('T') is True
    assert t.from_python(False, Compiler_5_0) is False
    assert t.from_python(True, Compiler_5_0) is True
    assert t.from_python(0, Compiler_5_0) is False
    assert t.from_python(1, Compiler_5_0) is True
    assert t.from_python('true', Compiler_5_0) is True
    assert t.from_python('false', Compiler_5_0) is True


def test_binary():
    t = Binary()
    assert t.to_python(None) is None
    assert t.to_python('dGVzdA==') == b'test'
    with pytest.raises(binascii.Error):
        t.to_python('dGVzdA=')
    assert t.from_python(b'test', Compiler_5_0) == 'dGVzdA=='
    with pytest.raises(TypeError):
        t.from_python(True, Compiler_5_0)
    with pytest.raises(ValidationError):
        t.from_python(True, Compiler_5_0, validate=True)


def test_ip():
    t = Ip()
    assert t.to_python(None) is None
    assert t.to_python('8.8.8.8') == '8.8.8.8'
    assert t.from_python('8.8.8.8', Compiler_5_0) == '8.8.8.8'
    assert t.from_python('8.8.8.', Compiler_5_0) == '8.8.8.'
    assert t.from_python(8888, Compiler_5_0) == 8888
    assert t.from_python('8.8.8.8', Compiler_5_0, validate=True) == '8.8.8.8'
    with pytest.raises(ValidationError):
        t.from_python('8.8.8.', Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(8888, Compiler_5_0, validate=True)


def test_object():
    t = Object(DynamicDocument)
    assert t.to_python(None) is None
    doc = t.to_python({'name': 'Test', 'status': 1})
    assert doc.name == 'Test'
    assert doc.status == 1


def test_list():
    t = List(Integer)
    with pytest.raises(ValueError):
        t.to_python('test')
    with pytest.raises(ValueError):
        t.to_python(['test'])
    assert t.to_python(123) == [123]
    assert t.to_python('123') == [123]
    assert t.to_python([1, '2']) == [1, 2]
    assert t.to_python_single([1, '2']) == 1
    assert t.from_python('test', Compiler_5_0) == ['test']
    with pytest.raises(ValidationError):
        t.from_python('test', Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(['test'], Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 31, Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python([1 << 31], Compiler_5_0, validate=True)


def test_geo_poing():
    t = GeoPoint()
    assert t.to_python(None) == None
    assert t.to_python('41.12,-71.34') == {'lat': 41.12, 'lon': -71.34}
    p = t.to_python('drm3btev3e86')
    assert p['lat'] == pytest.approx(41.12)
    assert p['lon'] == pytest.approx(-71.3400001)
    assert t.to_python([-71.34, 41.12]) == {'lat': 41.12, 'lon': -71.34}
    assert t.to_python({'lat': 41.12, 'lon': -71.32}) == \
        {'lat': 41.12, 'lon': -71.32}
    assert t.from_python({'lon': -71.34, 'lat': 41.12}, Compiler_5_0) == \
        {'lon': -71.34, 'lat': 41.12}
    assert t.from_python('drm3btev3e86', Compiler_5_0) == 'drm3btev3e86'
    assert t.from_python('41.12,-71.34', Compiler_5_0) == '41.12,-71.34'
    assert t.from_python([-71.34, 41.12], Compiler_5_0) == [-71.34, 41.12]
    with pytest.raises(ValidationError):
        t.from_python(
            {'lon': -71.34, 'lat': 41.12, 'description': 'Test'},
            Compiler_5_0,
            validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python('drm3btev3e86', Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python([-71.34], Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(['1test', '2test'], Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(
            {'lat': 'lon', 'lon': 'lat'}, Compiler_5_0, validate=True
        )


def test_completion():
    t = Completion()
    assert t.to_python(None) is None
    assert t.from_python('complete this', Compiler_5_0, validate=True) == \
        'complete this'
    assert \
        t.from_python(
            {'input': 'Complete this'}, Compiler_5_0, validate=True
        ) == \
        {'input': 'Complete this'}
    assert \
        t.from_python(
            {'input': ['Complete this']}, Compiler_5_0, validate=True
        ) == \
        {'input': ['Complete this']}
    assert \
        t.from_python(
            {'input': 'Complete this', 'weight': 1},
            Compiler_5_0,
            validate=True
        ) == \
        {'input': 'Complete this', 'weight': 1}
    assert \
        t.from_python(
            {'input': 'Complete this', 'output': 'complete'},
            Compiler_5_0,
            validate=True
        ) == \
        {'input': 'Complete this', 'output': 'complete'}
    assert \
        t.from_python(
            {
                'input': ['Complete this', 'Complete'],
                'output': 'complete',
                'weight': 100500,
                'payload': {'hits': 123}
            },
            Compiler_5_0,
            validate=True
        ) == \
        {'input': ['Complete this', 'Complete'],
         'output': 'complete',
         'weight': 100500,
         'payload': {'hits': 123}}

    with pytest.raises(ValidationError):
        t.from_python([''], Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': ''}, Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': None}, Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': {'foo': 'bar'}}, Compiler_5_0, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(
            {'input': 'foo', 'weight': -1},
            Compiler_5_0,
            validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python(
            {'input': 'foo', 'weight': None},
            Compiler_5_0,
            validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python(
            {'input': 'foo', 'weight': ''},
            Compiler_5_0,
            validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python(
            {'input': 'foo', 'weight': -1},
            Compiler_5_0,
            validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python(
            {'input': 'foo', 'output': -1},
            Compiler_5_0,
            validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python(
            {'input': 'foo', 'payload': ''},
            Compiler_5_0,
            validate=True
        )
