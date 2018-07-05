from redis import Redis
from . import get_client
from .logcenter import logger
from .field import *
from .structure import *
from .query import *


class Database(Redis):
    def __init__(self, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        self.__mapping__ = {
            'list': self.List,
            'hash': self.Hash,
            'set': self.Set,
            'zset': self.ZSet
        }

    def List(self, key):
        return List(self, key)

    def Hash(self, key):
        return Hash(self, key)

    def Set(self, key):
        return Set(self, key)

    def ZSet(self, key):
        return SortedSet(self, key)


class BaseModelMeta(type):
    def __new__(mcs, name, bases, attrs):
        if name == "Model":
            return type.__new__(mcs, name, bases, attrs)

        ext_fields = dict()
        fields = dict()
        defaults = dict()

        for k, v in attrs.items():
            if not isinstance(v, (Field, ExtField)):
                continue
            logger.info(' found mapping: %s ==> %s' % (k, v))
            if isinstance(v, Field):
                fields[k] = v
            else:
                ext_fields[k] = v
            if v.default is not None:
                defaults[k] = v.default

        model_class = super(BaseModelMeta, mcs).__new__(mcs, name, bases, attrs)
        model_class._fields = fields
        model_class._ext_fields = ext_fields
        model_class._defaults = defaults
        model_class._key = Key(name)
        # Add Queryset for model_class
        model_class.objects = Query(model_class)

        for key, value in attrs.items():
            if isinstance(value, (Field, ExtField)):
                value.add_to_class(model_class, key)
        return model_class


class Model(object, metaclass=BaseModelMeta):

    __database__ = None
    __namespace__ = None

    def __init__(self, **kwargs):
        self._load_default_dict()
        for k, v in kwargs.items():
            setattr(self, k, v)

    def _load_default_dict(self):
        """获取字段的默认值
        假如默认值是可调用的函数，例如，default=time.now,则获取当前时间
        获取默认值应该仅在初始化字段的值之前调用
        """
        for field_name, default in self._defaults.items():
            if callable(default):
                default = default()
            logger.info("Set default %s ==> %s" % (field_name, default))
            setattr(self, field_name, default)

    @property
    def db(cls):
        return cls.__database__

    @property
    def fields(self):
        return dict(self._fields)

    def key(self):
        return self._key[self.id]

    @property
    def id(self):
        return getattr(self, '_id')

    @id.setter
    def id(self, val):
        setattr(self, '_id', str(val))

    def save(self):
        """Use pipeline to ensure atomicity

        """
        if self.is_new():
            self._init_id()
        pipe = self.db.pipeline()
        self._create_membership(pipe)
        h = self._save_ext_fields(pipe)
        h[id] = self.id
        for k, v in self.fields.items():
            logger.info("%s ==> %s" % (k, v))
            print("%s == > %s" % (k, getattr(self, k)))
            h[k] = v.redis_value(getattr(self, k))
            setattr(self, k, v.python_value(h[k]))
        pipe.hmset(self.key(), h)
        pipe.execute()
        return True

    def delete(self):
        if self.is_new():
            raise RuntimeError("No such data")
        pipe = self.db.pipeline()
        self._delete_membership(pipe)
        ext_keys = [self.key()[e] for e in self._ext_fields.keys()]
        pipe.delete(*ext_keys)
        print(self.key())
        pipe.delete(self.key())
        pipe.execute()
        return True

    def update(self, *args, **kwargs):
        kwargs.update(*args)
        _kw = dict()
        for k, v in kwargs.items():
            if k in self._fields:
                setattr(self, k, v)
                _kw[k] = v
        _self = Hash(key=self.key, db=get_client())
        _self.update(**_kw)

    def is_new(self):
        return not hasattr(self, '_id')

    def _init_id(self):
        setattr(self, 'id', str(self.db.incr(self._key['id']['_sequence'])))

    def _index_key_for(self, field, value=None):
        if value is None:
            value = getattr(self, field)
            if isinstance(value, Field):
                value = value.python_value(getattr(self, field))
            if callable(value):
                value = str(value())
        return self._key[field][value]

    def _create_membership(self, pipe=None):
        pipe.sadd(self._key['all'], self.key())

    def _delete_membership(self, pipe=None):
        pipe.srem(self._key['all'], self.key())

    def _save_ext_fields(self, pipe=None):
        ext_h = {}
        for name, field in self._ext_fields.items():
            logger.info("%s ==> %s" % (name, field))
            key = self.key()[name]
            val = getattr(self, name)
            field.save(pipe, key, val)
            ext_h[name] = self.key()[name]
        return ext_h
