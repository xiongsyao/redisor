from .logcenter import logger
from .structure import *


class Key(str):
    def __getitem__(self, key):
        return Key("%s:%s" % (self, key,))


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

    def get(self, id):
        return self.get_model_queryset()._get_item_with_id(id)

    def create(self, **kwargs):
        instance = self.model_class(**kwargs)
        instance.save()
        return instance

    def filter(self, **kwargs):
        self._filters.update(kwargs)
        return self.get_model_queryset()


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
        """Query data from redis by id, and return a Model instance.
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
        for name, field in self.model_class._ext_fields.items():
            data[name] = field.load(Key(self.model_class.__name__)[id][name])
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
                index = self._build_key_from_filter_item(k, v)
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
        return set(map(lambda _id: self._get_item_with_id(_id), self.set.all()))

    def count(self):
        return len(self.set)

    def __iter__(self):
        print("do search in redis")
        yield from self.set


__all__ = ['Queryset', 'Query', 'Key']
