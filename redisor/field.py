import json


class Field:

    def __init__(self, name, column_type, index, default, required=False, jsonify=False):
        self.name = name
        self.column_type = column_type
        self.index = index
        self.default = default
        self.required = required
        self.model_class = None

    def add_to_class(self, model_class, name):
        self.model_class = model_class
        self.name = name

    def redis_value(self, value):
        if self.column_type and callable(self.column_type):
            return self.column_type(value)
        return value

    def python_value(self, value):
        if self.column_type and callable(self.column_type):
            return self.column_type(value)
        return value

    def __repr__(self):
        return '<%s, %s:%s>' % (
            self.__class__.__name__,
            self.column_type,
            self.name
        )


class StringField(Field):

    def __init__(self, name=None, index=False, default=None):
        super().__init__(name, str, index, default)


class JsonField(Field):

    def __init__(self, name=None, index=False, default=None):
        if default is None:
            default = {}
        super().__init__(name, 'json', index, default)

    def python_value(self, value):
        return json.loads(value)

    def redis_value(self, value):
        return json.dumps(value)


class IntegerField(Field):

    def __init__(self, name=None, index=False, default=None):
        super().__init__(name, int, index, default)


class AutoIncrementField(IntegerField):

    def __init__(self, *args, **kwargs):
        kwargs['index'] = True
        super(AutoIncrementField, self).__init__(*args, **kwargs)

    def _gen_key(self):
        if not hasattr(self, "key"):
            key = '%s:%s:_sequence' % (
                self.model_class.__name__,
                self.name
            )
            seq = self.model_class.__database__.incr(key)
            print("gen key %s" % seq)
            setattr(self, "key", seq)
        else:
            seq = getattr(self, "key")
        return seq

    def redis_value(self, value):
        return self._gen_key()

    def python_value(self, value):
        return self._gen_key()
