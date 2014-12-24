from .util import cached_property
from .expression import Expression, Field, FieldOperators
from .collections import OrderedAttributes


class AttributedField(Expression, FieldOperators):
    __visit_name__ = 'attributed_field'

    def __init__(self, parent, attr_name, field):
        self._parent = parent
        self._attr = attr_name
        self._field = field

        self._sub_fields = OrderedAttributes()
        sub_fields = list(self._field._type.sub_fields().items())
        if sub_fields:
            for attr_name, attr_field in sub_fields:
                field = attr_field._field
                self._sub_fields[attr_name] = AttributedField(
                    self._parent, attr_name, Field(self._make_field_name(field._name), field._type, fields=field._fields)
                )
        elif self._field._fields:
            for field_attr, field in self._field._fields.items():
                self._sub_fields[field_attr] = AttributedField(
                    self, field_attr, Field(self._make_field_name(field_attr), field._type)
                )

    def _make_field_name(self, name):
        return '{}.{}'.format(self._field._name, name)

    @property
    def fields(self):
        return self._sub_fields

    def __getattr__(self, name):
        if name not in self.fields:
            raise AttributeError('No that field: %s' % name)
        return self.fields[name]

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

        dict_ = obj.__dict__
        if self._attr in obj.__dict__:
            return dict_[self._attr]
        dict_[self._attr] = None
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
    def __init__(self, parent, attr_name, field):
        super(DynamicAttributedField, self).__init__(parent, attr_name, field)

    def __getattr__(self, name):
        if name not in self.fields:
            return DynamicAttributedField(
                self._parent, name, Field(self._make_field_name(name))
            )
        return self.fields[name]
