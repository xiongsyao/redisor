from redis import Redis
from . import get_client
from .logcenter import logger
from .field import Field, AutoIncrementField
from .structure import Hash, List, Set, SortedSet


class Key(str):
    def __getitem__(self, key):
        return Key("%s:%s" % (self, key,))


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


class Query:

    def __init__(self, model_class):
        self.model_class = model_class
        self._filters = {}

    def get_model_queryset(self):
        return Queryset(self.model_class, filters=self._filters)

    def all(self):
        return self.get_model_queryset()

    def __getitem__(self, index):
        return self.get_model_queryset()[index]

    def create(self, **kwargs):
        instance = self.model_class(**kwargs)
        instance.save()
        return instance

    def filter(self, **kwargs):
        self._filters.update(kwargs)
        return self.get_model_queryset()

    def get(self, id):
        return self.get_model_queryset()._get_item_with_id(id)


class Queryset:

    def __init__(self, model_class, filters=None):
        self.model_class = model_class
        self.db = model_class.__database__
        self.key = model_class._key['all']
        self._filters = filters

    def __getitem__(self, index):
        l = sorted(list(self.set))
        try:
            return self._get_item_with_id(l[int(index)])
        except IndexError:
            return None

    def _get_item_with_id(self, id):
        """Query data from redis by primary_key, and return a Model instance.
        :param primary_key:
        :return:
        """
        raw_data = self.db.hgetall(Key(self.model_class.__name__)[id])
        if not raw_data:
            raise Exception('%s `id` %s  doest`t exist.' % (self.model_class.__name__, id))
        data = {}
        for name, field in self.model_class._fields.items():
            if name not in raw_data:
                data[name] = None
            else:
                data[name] = field.python_value(raw_data[name])
        instance = self.model_class(**data)
        instance._id = str(id)
        return instance

    @property
    def set(self):
        s = Set(self.db, self.key)
        print("filter s %s" % s)
        if self._filters:
            indices = []
            for k, v in self._filters.items():
                index  =self._build_key_from_filter_item(k, v)
                if k not in self.model_class._indices:
                    raise AttributeError("%s is not indexed in %s clas." % (k, self.model_class.__name__))
                indices.append(index)
            new_set_key = "~%s" % ("+".join([self.key] + indices), )
            logger.info("Add new set key `%s`" % new_set_key)
            s.intersection(new_set_key, *[Set(self.db, n) for n in indices])
            s = Set(self.db, new_set_key)
        return s

    def filter(self, **kwargs):
        if not self._filters:
            self._filters = {}
        self._filters.update(kwargs)
        return self

    def _build_key_from_filter_item(self, index, value):
        return self.model_class._key[index][value]

    @property
    def members(self):
        return set(map(lambda id: self._get_item_with_id(id), self.set.all()))

    def __iter__(self):
        print("do search in redis")
        print(list(self.set))
        return iter(self.set)


class BaseModelMeta(type):
    def __new__(cls, name, bases, attrs):
        if name == "Model":
            return type.__new__(cls, name, bases, attrs)

        fields = dict()
        defaults = dict()

        for k, v in attrs.items():
            if not isinstance(v, Field):
                continue
            logger.info(' found mapping: %s ==> %s' % (k, v))
            fields[k] = v
            if v.default is not None:
                defaults[k] = v.default

        model_class = super(BaseModelMeta, cls).__new__(cls, name, bases, attrs)
        model_class._fields = fields
        model_class._defaults = defaults
        model_class._key = Key(name)
        # Add Queryset for model_class
        model_class.objects = Query(model_class)

        for key, value in attrs.items():
            if isinstance(value, Field):
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
    def fields(cls):
        return dict(cls._fields)

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
        pipe = self.db.pipeline()
        if self.is_new():
            self._init_id(pipe)
        self._create_membership(pipe)
        h = {}
        for k, v in self.fields.items():
            logger.info("%s ==> %s" % (k, v))
            print("%s == > %s" % (k, getattr(self, k)))
            h[k] = v.redis_value(getattr(self, k))
            setattr(self, k, v.python_value(h[k]))
        pipe.hmset(self.key(), h)
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

    def _init_id(self, pipe=None):
        setattr(self, 'id', str(pipe.incr(self._key['id']['_sequence'])))

    def _index_key_for(self, field, value=None):
        if value is None:
            value = getattr(self, field)
            if isinstance(value, Field):
                value = value.python_value(getattr(self, field))
            if callable(value):
                value = str(value())
        return self._key[field][value]

    def _create_membership(self, pipe=None):
        pipe.sadd(self._key['all'], self.key)

    def _delete_membership(self, pipe=None):
        pipe.srem(self._key['all'], self.key)
