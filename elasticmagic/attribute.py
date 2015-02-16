from .util import cached_property
from .expression import Expression, Field, FieldOperators
from .datastructures import OrderedAttributes


# really its factory factory
def _attributed_field_factory(attr_cls, doc_cls, dynamic_field, make_field_name=None):
    def _attr_field(name):
        field_name = make_field_name(name) if make_field_name else name
        return attr_cls(doc_cls, name, Field(field_name, dynamic_field.get_type()))
    return _attr_field


class AttributedField(Expression, FieldOperators):
    __visit_name__ = 'attributed_field'

    def __init__(self, parent, attr_name, field):
        self._parent = parent
        self._attr = attr_name
        self._field = field

        if self._field._type.doc_cls:
            doc_cls = self._field._type.doc_cls
            dynamic_defaults = {}
            for dyn_field_name, dyn_attr_field in doc_cls.dynamic_fields.items():
                dyn_field = dyn_attr_field._field
                default = _attributed_field_factory(AttributedField, self._parent, dyn_field, self._make_field_name)
                dynamic_defaults[dyn_field_name] = default

            self._sub_fields = OrderedAttributes(defaults=dynamic_defaults)
            for attr_name, attr_field in doc_cls.user_fields.items():
                field = attr_field._field
                self._sub_fields[attr_name] = AttributedField(
                    self._parent, attr_name, Field(self._make_field_name(field._name), field._type, fields=field._fields)
                )
        elif self._field._fields:
            self._sub_fields = OrderedAttributes()
            for field_attr, field in self._field._fields.items():
                self._sub_fields[field_attr] = AttributedField(
                    self, field_attr, Field(self._make_field_name(field_attr), field._type)
                )
        else:
            self._sub_fields = OrderedAttributes()

    def _make_field_name(self, name):
        return '{}.{}'.format(self._field._name, name)

    @property
    def fields(self):
        return self._sub_fields

    def __getattr__(self, name):
        return getattr(self.fields, name)

    def wildcard(self, name):
        return DynamicAttributedField(
            self._parent, name, Field(self._make_field_name(name))
        )

    def get_parent(self):
        return self._parent
    
    def get_attr(self):
        return self._attr
    
    def get_field(self):
        return self._field
    
    def get_type(self):
        return self._field.get_type()
    
    def __get__(self, obj, type=None):
        if obj is None:
            return self

        obj.__dict__[self._attr] = None
        return None

    def _collect_doc_classes(self):
        if isinstance(self._parent, AttributedField):
            return self._parent._collect_doc_classes()
        return [self._parent]

    def _to_python(self, value):
        return self._field._to_python(value)

    def _from_python(self, value):
        return self._field._from_python(value)


class DynamicAttributedField(AttributedField):
    def __getattr__(self, name):
        return getattr(
            self.fields,
            name,
            DynamicAttributedField(
                self._parent, name, Field(self._make_field_name(name))
            )
        )
