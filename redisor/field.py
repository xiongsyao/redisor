import json

from .structure import List


class Field:

    def __init__(self, name, column_type, default, required=False):
        self.name = name
        self.column_type = column_type
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


class ExtField:

    def __init__(self, name, default, required=False):
        self.name = name
        self.default = default
        self.required = required
        self.model_class = None

    def add_to_class(self, model_class, name):
        self.model_class = model_class
        self.name = name

    def save(self, pipe, key, value):
        """save ext_field
        """
        raise NotImplementedError

    def load(self, key):
        """load ext_field
        """
        raise NotImplementedError

    # def __repr__(self):
    #     return '<%s:%s> ' % (
    #         self.__class__.__name__,
    #         self.name
    #     )



class StringField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, str, default)


class JsonField(Field):

    def __init__(self, name=None, default=None):
        if default is None:
            default = {}
        super().__init__(name, 'json', default)

    def python_value(self, value):
        return json.loads(value)

    def redis_value(self, value):
        return json.dumps(value)


class IntegerField(Field):

    def __init__(self, name=None,  default=None):
        super().__init__(name, int, default)


class AutoIncrementField(IntegerField):

    def __init__(self, *args, **kwargs):
        super(AutoIncrementField, self).__init__(*args, **kwargs)

    def _gen_key(self):
        if not hasattr(self, "key"):
            key = '%s:%s:_sequence' % (
                self.model_class.__name__,
                self.name
            )
            seq = self.model_class.__database__.incr(key)
            setattr(self, "key", seq)
        else:
            seq = getattr(self, "key")
        return seq

    def redis_value(self, value):
        return self._gen_key()

    def python_value(self, value):
        return self._gen_key()


class ListField(ExtField):

    def __init__(self, name, default=None):
        self.name = name
        self.default = default if default is not None else []
        super(ListField, self).__init__(self, name, default)

    def save(self, pipe, key, value):
        pipe.rpush(key, *value)

    def load(self, key):
        return self.model_class.__database__.lrange(key, 0, -1)


class HashField(ExtField):

    def __init__(self, name, default=None):
        self.name = name
        self.default = default if default is not None else {}
        super(HashField, self).__init__(self, name, default)

    def save(self, pipe, key, value):
        pipe.hmset(key, value)

    def load(self, key):
        return self.model_class.__database__.hgetall(key)


__all__ = ['Field', 'ExtField', 'StringField', 'AutoIncrementField',
           'IntegerField', 'ListField', 'HashField', 'JsonField']
