from redisor import get_client

class Field:

    def __init__(self, name, column_type, primary_key, default,
                 required=False, unique=False):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
        self.required = required
        self.unique = unique

    def __repr__(self):
        return '<%s, %s:%s>' % (
            self.__class__.__name__,
            self.column_type,
            self.name
        )


class StringField(Field):

    def __init__(self, name=None, primary_key=None, default=None):
        super().__init__(name, str, primary_key, default)
