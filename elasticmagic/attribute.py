from .expression import Expression, Field, FieldOperators
from .datastructures import OrderedAttributes


# really its factory factory
def _attributed_field_factory(
        attr_cls, doc_cls, dynamic_field, make_field_name=None
):
    def _attr_field(name):
        field = dynamic_field.clone()
        field._name = make_field_name(name) if make_field_name else name
        return attr_cls(doc_cls, name, field)
    return _attr_field


class AttributedField(Expression, FieldOperators):
    __visit_name__ = 'attributed_field'

    def __init__(self, parent, attr_name, field):
        self._parent = parent
        self._attr_name = attr_name
        self._field = field

        self._dynamic_fields = OrderedAttributes()

        if self._field._type.doc_cls:
            doc_cls = self._field._type.doc_cls
            dynamic_defaults = {}
            for dyn_field_name, dyn_attr_field in \
                    doc_cls.dynamic_fields.items():
                dyn_field = dyn_attr_field._field.clone()
                dyn_field._name = self._make_field_name(dyn_field._name)
                self._dynamic_fields[dyn_field_name] = AttributedField(
                    self._parent, dyn_field_name, dyn_field
                )
                default = _attributed_field_factory(
                    AttributedField, self._parent, dyn_field,
                    self._make_field_name
                )
                dynamic_defaults[dyn_field_name] = default

            self._sub_fields = OrderedAttributes(defaults=dynamic_defaults)
            for attr_name, attr_field in doc_cls.user_fields.items():
                field = attr_field._field.clone()
                field._name = self._make_field_name(field._name)
                self._sub_fields[attr_name] = AttributedField(
                    self._parent, attr_name, field
                )
        elif self._field._fields:
            self._sub_fields = OrderedAttributes()
            for field_attr, field in self._field._fields.items():
                field = field.clone()
                field._name = self._make_field_name(field_attr)
                self._sub_fields[field_attr] = AttributedField(
                    self, field_attr, field
                )
        else:
            self._sub_fields = OrderedAttributes()

    def _make_field_name(self, name):
        return '{}.{}'.format(self._field._name, name)

    @property
    def fields(self):
        return self._sub_fields

    @property
    def dynamic_fields(self):
        return self._dynamic_fields

    def __getattr__(self, name):
        return getattr(self.fields, name)

    def wildcard(self, name):
        return DynamicAttributedField(
            self._parent, name, Field(self._make_field_name(name))
        )

    def get_parent(self):
        return self._parent

    def get_attr_name(self):
        return self._attr_name

    def get_field(self):
        return self._field

    def get_field_name(self):
        return self._field._name

    def get_type(self):
        return self._field.get_type()

    def __get__(self, obj, type=None):
        if obj is None:
            return self

        obj.__dict__[self._attr_name] = None
        return None

    def _collect_doc_classes(self):
        if isinstance(self._parent, AttributedField):
            return self._parent._collect_doc_classes()
        return {self._parent}

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
