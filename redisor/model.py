class BaseModelMeta(type):
    pass


class Model(dict, metaclass=BaseModelMeta):

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.k = v
        super(Model, self).__init__()

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(r"'Model' object has no aatribute '%s'" % item)

    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key, default=None):
        return getattr(self, key, default)
