from copy import deepcopy
from . import get_client
from .logcenter import logger
from .field import Field, AutoIncrementField
from .structure import Hash


class BaseModelMeta(type):
    def __new__(cls, name, bases, attrs):
        if name == "Model":
            return type.__new__(cls, name, bases, attrs)

        primary_key = None
        fields = []
        defaults = dict()

        for k, v in attrs.items():
            if not isinstance(v, Field):
                continue
            logger.info(' found mapping: %s ==> %s' % (k, v))
            if v.primary_key:
                if primary_key:
                    raise RuntimeError('Duplicate primary key for field: %s' % k)
                primary_key = k
            else:
                fields.append(k)
            if v.default:
                defaults[k] = v.default

        if not primary_key:
            attrs['_id'] = AutoIncrementField()
            primary_key = '_id'

        model_class = super(BaseModelMeta, cls).__new__(cls, name, bases, attrs)
        model_class._fields = fields
        model_class._defaults = defaults
        model_class._primary_key = primary_key
        model_class._data = None

        for key, value in attrs.items():
            if isinstance(value, Field):
                value.add_to_class(model_class, key)
        return model_class


class Model(object, metaclass=BaseModelMeta):
    database = None
    namespace = None

    def __init__(self, *args, **kwargs):
        self._data = {}
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def primary_key(self):
        return getattr(self, self._primary_key)

    @property
    def key(self):
        try:
            return getattr(self, '_key')
        except AttributeError:
            _key = ":".join(map(str, [self.__class__.__name__, self._primary_key, self.primary_key._gen_key()]))
            setattr(self, "_key", _key)
            return _key

    def save(self):
        _kw = dict()
        _self = Hash(key=self.key, db=get_client())
        for k in self._fields:
            _kw[k] = getattr(self, k)
        _self.update(**_kw)

    def update(self, *args, **kwargs):
        kwargs.update(*args)
        _kw = dict()
        for k, v in kwargs.items():
            if k in self._fields:
                self.k = v
                _kw[k] = v
        _self = Hash(key=self.key, db=get_client())
        _self.update(**_kw)
