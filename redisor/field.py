class Field:

    def __init__(self, name, column_type, primary_key, default,
                 required=False, unique=False):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
        self.required = required
        self.unique = unique
        self.model_class = None

    def add_to_class(self, model_class, name):
        self.model_class = model_class
        self.name = name

    def __repr__(self):
        return '<%s, %s:%s>' % (
            self.__class__.__name__,
            self.column_type,
            self.name
        )


class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None):
        super().__init__(name, str, primary_key, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=None):
        super().__init__(name, int, primary_key, default)


class AutoIncrementField(IntegerField):

    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        super(AutoIncrementField, self).__init__(*args, **kwargs)

    def _gen_key(self):
        key = '%s:%s:_sequence' % (
            self.model_class.__name__,
            self.name
        )
        return self.model_class.database.incr(key)

