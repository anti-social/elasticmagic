import binascii
import datetime
import unittest

from elasticmagic.compat import PY2
from elasticmagic.document import DynamicDocument
from elasticmagic.types import (
    Type, String, Byte, Short, Integer, Long, Float, Double, Date, Boolean,
    Binary, Ip, Object, Nested, List, GeoPoint, ValidationError,
)


class TypesTestCase(unittest.TestCase):
    def test_type(self):
        t = Type()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(t.to_python('test'), 'test')
        self.assertEqual(t.to_python(123), 123)
        self.assertEqual(t.from_python('test'), 'test')
        self.assertEqual(t.from_python('test', validate=True), 'test')
        self.assertEqual(t.from_python(123, validate=True), 123)

    def test_string(self):
        t = String()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(t.to_python('test'), 'test')
        self.assertEqual(t.to_python(123), '123')
        self.assertEqual(t.from_python('test'), 'test')
        self.assertEqual(t.from_python('test', validate=True), 'test')
        self.assertEqual(t.from_python(123, validate=True), '123')
        
    def test_byte(self):
        t = Byte()
        self.assertIs(t.to_python(None), None)
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertEqual(t.to_python(123), 123)
        self.assertEqual(t.to_python('123'), 123)
        self.assertEqual(t.from_python('test'), 'test')
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(128, validate=True))

    def test_short(self):
        t = Short()
        self.assertIs(t.to_python(None), None)
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertEqual(t.to_python(123), 123)
        self.assertEqual(t.to_python('123'), 123)
        self.assertEqual(t.from_python('test'), 'test')
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(1 << 15, validate=True))

    def test_integer(self):
        t = Integer()
        self.assertIs(t.to_python(None), None)
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertEqual(t.to_python(123), 123)
        self.assertEqual(t.to_python('123'), 123)
        self.assertEqual(t.from_python('test'), 'test')
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(1 << 31, validate=True))

    def test_float(self):
        t = Float()
        self.assertIs(t.to_python(None), None)
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertEqual(t.to_python(123), 123)
        self.assertEqual(t.to_python('123'), 123)
        self.assertEqual(t.from_python('test'), 'test')
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertAlmostEqual(t.from_python('128', validate=True), 128.0)
        self.assertAlmostEqual(t.from_python(128, validate=True), 128.0)

    def test_doubel(self):
        t = Double()
        self.assertIs(t.to_python(None), None)
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertEqual(t.to_python(123), 123)
        self.assertEqual(t.to_python('123'), 123)
        self.assertEqual(t.from_python('test'), 'test')
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertAlmostEqual(t.from_python('128', validate=True), 128.0)
        self.assertAlmostEqual(t.from_python(128, validate=True), 128.0)

    def test_date(self):
        t = Date()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(
            t.to_python('2009-11-15T14:12:12'),
            datetime.datetime(2009, 11, 15, 14, 12, 12)
        )
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertRaises(ValueError, lambda: t.from_python('test'))
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertEqual(
            t.from_python(datetime.datetime(2009, 11, 15, 14, 12, 12)),
            datetime.datetime(2009, 11, 15, 14, 12, 12)
        )
        
    def test_boolean(self):
        t = Boolean()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(t.to_python(False), False)
        self.assertEqual(t.to_python(True), True)
        self.assertEqual(t.to_python(0), False)
        self.assertEqual(t.to_python(1), True)
        self.assertEqual(t.to_python('false'), False)
        self.assertEqual(t.to_python('F'), False)
        self.assertEqual(t.to_python(''), False)
        self.assertEqual(t.to_python('true'), True)
        self.assertEqual(t.to_python('T'), True)
        self.assertEqual(t.from_python(False), False)
        self.assertEqual(t.from_python(True), True)
        self.assertEqual(t.from_python(0), False)
        self.assertEqual(t.from_python(1), True)
        self.assertEqual(t.from_python('true'), True)
        self.assertEqual(t.from_python('false'), True)
        
    def test_binary(self):
        t = Binary()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(t.to_python('dGVzdA=='), b'test')
        if PY2:
            self.assertRaises(TypeError, lambda: t.to_python('dGVzdA='))
        else:
            self.assertRaises(binascii.Error, lambda: t.to_python('dGVzdA='))
        self.assertEqual(t.from_python(b'test'), 'dGVzdA==')
        self.assertRaises(TypeError, lambda: t.from_python(True))
        self.assertRaises(ValidationError, lambda: t.from_python(True, validate=True))
        
    def test_ip(self):
        t = Ip()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(t.to_python('8.8.8.8'), '8.8.8.8')
        self.assertEqual(t.from_python('8.8.8.8'), '8.8.8.8')
        self.assertEqual(t.from_python('8.8.8.'), '8.8.8.')
        self.assertEqual(t.from_python(8888), 8888)
        self.assertEqual(t.from_python('8.8.8.8', validate=True), '8.8.8.8')
        self.assertRaises(ValidationError, lambda: t.from_python('8.8.8.', validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(8888, validate=True))
        
    def test_object(self):
        t = Object(DynamicDocument)
        self.assertIs(t.to_python(None), None)
        doc = t.to_python({'name': 'Test', 'status': 1})
        self.assertEqual(doc.name, 'Test')
        self.assertEqual(doc.status, 1)
        
    def test_list(self):
        t = List(Integer)
        self.assertRaises(ValueError, lambda: t.to_python('test'))
        self.assertRaises(ValueError, lambda: t.to_python(['test']))
        self.assertEqual(t.to_python(123), [123])
        self.assertEqual(t.to_python('123'), [123])
        self.assertEqual(t.to_python([1, '2']), [1, 2])
        self.assertEqual(t.to_python_single([1, '2']), 1)
        self.assertEqual(t.from_python('test'), ['test'])
        self.assertRaises(ValidationError, lambda: t.from_python('test', validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(['test'], validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(1 << 31, validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python([1 << 31], validate=True))
        
    def test_geo_poing(self):
        t = GeoPoint()
        self.assertIs(t.to_python(None), None)
        self.assertEqual(t.to_python('41.12,-71.34'), {'lat': 41.12, 'lon': -71.34})
        p = t.to_python('drm3btev3e86')
        self.assertAlmostEqual(p['lat'], 41.12)
        self.assertAlmostEqual(p['lon'], -71.3400001)
        self.assertEqual(t.to_python([-71.34, 41.12]), {'lat': 41.12, 'lon': -71.34})
        self.assertEqual(
            t.to_python({'lat': 41.12, 'lon': -71.32}),
            {'lat': 41.12, 'lon': -71.32}
        )
        self.assertEqual(
            t.from_python({'lon': -71.34, 'lat': 41.12}),
            {'lon': -71.34, 'lat': 41.12}
        )
        self.assertEqual(t.from_python('drm3btev3e86'), 'drm3btev3e86')
        self.assertEqual(t.from_python('41.12,-71.34'), '41.12,-71.34')
        self.assertEqual(t.from_python([-71.34, 41.12]), [-71.34, 41.12])
        self.assertRaises(
            ValidationError,
            lambda: t.from_python({'lon': -71.34, 'lat': 41.12, 'description': 'Test'}, validate=True)
        )
        self.assertRaises(ValidationError, lambda: t.from_python('drm3btev3e86', validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python([-71.34], validate=True))
        self.assertRaises(ValidationError, lambda: t.from_python(['1test', '2test'], validate=True))
        
