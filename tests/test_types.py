import binascii
import datetime

import pytest

from elasticmagic.compat import PY2
from elasticmagic.document import DynamicDocument
from elasticmagic.types import (
    Type, String, Byte, Short, Integer, Long, Float, Double, Date, Boolean,
    Binary, Ip, Object, Nested, List, GeoPoint, Completion, ValidationError,
)


def test_type():
    t = Type()
    assert t.to_python(None) is None
    assert t.to_python('test') == 'test'
    assert t.to_python(123) == 123
    assert t.from_python('test') == 'test'
    assert t.from_python('test', validate=True) == 'test'
    assert t.from_python(123, validate=True) == 123


def test_string():
    t = String()
    assert t.to_python(None) is None
    assert t.to_python('test') == 'test'
    assert t.to_python(123) == '123'
    assert t.from_python('test') == 'test'
    assert t.from_python('test', validate=True) == 'test'
    assert t.from_python(123, validate=True) == '123'


def test_byte():
    t = Byte()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test') == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    with pytest.raises(ValidationError):
        t.from_python(128, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(-129, validate=True)


def test_short():
    t = Short()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test') == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 15, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(-(1 << 15) - 1, validate=True)


def test_integer():
    t = Integer()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test') == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 31, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(-(1 << 31) - 1, validate=True)


def test_long():
    t = Long()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test') == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 63, validate=True)
    with pytest.raises(ValidationError):
        t.from_python(-(1 << 63) - 1, validate=True)


def test_float():
    t = Float()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test') == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    assert t.from_python('128', validate=True) == pytest.approx(128.0)
    assert t.from_python(128, validate=True) == pytest.approx(128.0)


def test_double():
    t = Double()
    assert t.to_python(None) is None
    with pytest.raises(ValueError):
        t.to_python('test')
    assert t.to_python(123) == 123
    assert t.to_python('123') == 123
    assert t.from_python('test') == 'test'
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    assert t.from_python('128', validate=True) == pytest.approx(128.0)
    assert t.from_python(128, validate=True) == pytest.approx(128.0)


def test_date():
    t = Date()
    assert t.to_python(None) is None
    assert t.to_python('2009-11-15T14:12:12') == \
        datetime.datetime(2009, 11, 15, 14, 12, 12)
    with pytest.raises(ValueError):
        t.to_python('test')
    with pytest.raises(ValueError):
        t.from_python('test')
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    assert \
        t.from_python(datetime.datetime(2009, 11, 15, 14, 12, 12)) == \
        datetime.datetime(2009, 11, 15, 14, 12, 12)


def test_boolean():
    t = Boolean()
    assert t.to_python(None) is None
    assert t.to_python(False) == False
    assert t.to_python(True) == True
    assert t.to_python(0) == False
    assert t.to_python(1) == True
    assert t.to_python('false') == False
    assert t.to_python('F') == False
    assert t.to_python('') == False
    assert t.to_python('true') == True
    assert t.to_python('T') == True
    assert t.from_python(False) == False
    assert t.from_python(True) == True
    assert t.from_python(0) == False
    assert t.from_python(1) == True
    assert t.from_python('true') == True
    assert t.from_python('false') == True


def test_binary():
    t = Binary()
    assert t.to_python(None) is None
    assert t.to_python('dGVzdA==') == b'test'
    if PY2:
        with pytest.raises(TypeError):
            t.to_python('dGVzdA=')
    else:
        with pytest.raises(binascii.Error):
            t.to_python('dGVzdA=')
    assert t.from_python(b'test') == 'dGVzdA=='
    with pytest.raises(TypeError):
        t.from_python(True)
    with pytest.raises(ValidationError):
        t.from_python(True, validate=True)


def test_ip():
    t = Ip()
    assert t.to_python(None) is None
    assert t.to_python('8.8.8.8') == '8.8.8.8'
    assert t.from_python('8.8.8.8') == '8.8.8.8'
    assert t.from_python('8.8.8.') == '8.8.8.'
    assert t.from_python(8888) == 8888
    assert t.from_python('8.8.8.8', validate=True) == '8.8.8.8'
    with pytest.raises(ValidationError):
        t.from_python('8.8.8.', validate=True)
    with pytest.raises(ValidationError):
        t.from_python(8888, validate=True)


def test_object():
    t = Object(DynamicDocument)
    assert t.to_python(None) is None
    doc = t.to_python({'name': 'Test', 'status': 1})
    assert doc.name, 'Test'
    assert doc.status, 1


def test_nested():
    t = Object(DynamicDocument)
    assert t.to_python(None) is None
    doc = t.to_python({'name': 'Test', 'status': 1})
    assert doc.name, 'Test'
    assert doc.status, 1


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
    assert t.from_python('test') == ['test']
    with pytest.raises(ValidationError):
        t.from_python('test', validate=True)
    with pytest.raises(ValidationError):
        t.from_python(['test'], validate=True)
    with pytest.raises(ValidationError):
        t.from_python(1 << 31, validate=True)
    with pytest.raises(ValidationError):
        t.from_python([1 << 31], validate=True)


def test_geo_poing():
    t = GeoPoint()
    assert t.to_python(None) is None
    assert t.to_python('41.12,-71.34') == {'lat': 41.12, 'lon': -71.34}
    p = t.to_python('drm3btev3e86')
    assert p['lat'] == pytest.approx(41.12)
    assert p['lon'] == pytest.approx(-71.34)
    assert t.to_python([-71.34, 41.12]) == {'lat': 41.12, 'lon': -71.34}
    assert t.to_python({'lat': 41.12, 'lon': -71.32}) == \
        {'lat': 41.12, 'lon': -71.32}
    assert t.from_python({'lon': -71.34, 'lat': 41.12}) == \
        {'lon': -71.34, 'lat': 41.12}
    assert t.from_python('drm3btev3e86') == 'drm3btev3e86'
    assert t.from_python('41.12,-71.34') == '41.12,-71.34'
    assert t.from_python([-71.34, 41.12]) == [-71.34, 41.12]
    with pytest.raises(ValidationError):
        t.from_python(
            {'lon': -71.34, 'lat': 41.12, 'description': 'Test'}, validate=True
        )
    with pytest.raises(ValidationError):
        t.from_python('drm3btev3e86', validate=True)
    with pytest.raises(ValidationError):
        t.from_python([-71.34], validate=True)
    with pytest.raises(ValidationError):
        t.from_python(['1test', '2test'], validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'lat': 'lon', 'lon': 'lat'}, validate=True)


def test_completion():
    t = Completion()
    assert t.to_python(None) is None
    assert t.from_python(None) is None
    assert t.from_python('complete this', validate=True) == 'complete this'
    assert t.from_python({'input': 'Complete this'}, validate=True) == \
        {'input': 'Complete this'}
    assert t.from_python({'input': ['Complete this']}, validate=True) == \
        {'input': ['Complete this']}
    assert t.from_python(
        {'input': 'Complete this', 'weight': 1}, validate=True
    ) == \
        {'input': 'Complete this', 'weight': 1}
    assert t.from_python(
        {'input': 'Complete this', 'output': 'complete'}, validate=True
    ) == \
        {'input': 'Complete this', 'output': 'complete'}
    assert t.from_python(
        {
            'input': ['Complete this', 'Complete'],
            'output': 'complete',
            'weight': 100500,
            'payload': {'hits': 123}
        },
        validate=True
    ) == \
        {
            'input': ['Complete this', 'Complete'],
            'output': 'complete',
            'weight': 100500,
            'payload': {'hits': 123}
        }
    with pytest.raises(ValidationError):
        t.from_python([''], validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': ''}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': None}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': {'foo': 'bar'}}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': 'foo', 'weight': -1}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': 'foo', 'weight': None}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': 'foo', 'weight': ''}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': 'foo', 'weight': -1}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': 'foo', 'output': -1}, validate=True)
    with pytest.raises(ValidationError):
        t.from_python({'input': 'foo', 'payload': ''}, validate=True)
