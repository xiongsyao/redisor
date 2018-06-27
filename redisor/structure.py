class List:

    def __init__(self, db, key):
        self.db = db
        self.key = key

    def all(self):
        return self.db.lrange(self.key, 0, -1)

    def append(self, value):
        return self.db.rpush(self.key, value)

    def pop(self):
        return self.db.rpop(self.key)

    def shift(self):
        return self.db.lpop(self.key)

    def unshift(self, value):
        return self.db.lpush(self.key, value)

    def remove(self, item):
        return self.db.lrem(self.key, 1, item)

    def __len__(self):
        return self.db.llen(self.key)

    def __getitem__(self, index):
        if isinstance(index, slice):
            start = index.start or 0
            stop = (index.stop or 0) - 1
            return self.db.lrange(self.key, start, stop)
        return self.db.lindex(self.key, index)

    def __setitem__(self, index, value):
        return self.db.lset(self.key, index, value)

    def __contains__(self, item):
        return str(item) in self.all()

    def __iter__(self):
        yield from self.all()

    def __repr__(self):
        length = len(self)
        show_items = min(length, 5)
        return '<List(key=%s, value=[%s%s])>' % (
            self.key,
            ", ".join(self[:show_items]),
            '...' if show_items < length else ''
        )


class Set:

    def __init__(self, db, key):
        self.db = db
        self.key = key

    def all(self):
        return self.db.smembers(self.key)

    def add(self, item):
        return self.db.sadd(self.key, item)

    def remove(self, item):
        if not self.db.srem(self.key, item):
            raise KeyError(item)

    def discard(self, item):
        self.db.srem(self.key, item)

    def __delitem__(self, item):
        return self.remove(item)

    def __len__(self):
        return self.db.scard(self.key)

    def __iter__(self):
        yield from self.all()

    def __contains__(self, item):
        return self.db.sismembers(self.key, item)

    def __and__(self, other):
        """intersection"""
        if isinstance(other, Set):
            return self.db.sinter(self.key, other.key)
        else:
            raise SyntaxError("`And` operation expect a Set type, but get %s" % type(other))

    def __or__(self, other):
        """union"""
        if isinstance(other, Set):
            return self.db.sunion(self.key, other.key)
        else:
            raise SyntaxError("`Or` operation expect a Set type, but get %s" % type(other))

    def __sub__(self, other):
        """difference"""
        if isinstance(other, Set):
            return self.db.sdiff(self.key, other.key)
        else:
            raise SyntaxError("`Sub` operation expect a Set type, but get %s" % type(other))

    def __repr__(self):
        length = len(self)
        show_items = min(length, 5)
        return '<Set(key=%s, value={%s%s}>' % (
            self.key,
            ", ".join(list(self.all())),
            '...' if length > show_items else ''
        )


class SortedSet:

    def __init__(self, db, key):
        self.db = db
        self.key = key

    def all(self):
        return self.db.zrange(self.key, 0, -1)

    def add(self, member, score):
        self.db.zadd(self.key, member, score)

    def remove(self, member: str):
        self.db.zrem(self.key, member)

    def rank(self, member, desc=False):
        """Return the rank of the given member"""
        if desc:
            return self.db.zrevrank(self.key, member)
        else:
            return self.db.zrank(self.key, member)

    def count(self, low, high=None):
        if high is None:
            high = low
        return self.db.zcount(self.key, low, high)

    def incr_by(self, member, increment):
        return self.db.zincrby(self.key, member, increment)

    def __getitem__(self, index):
        if isinstance(index, slice):
            start = slice.start or 0
            stop = (slice.stop or 0) - 1
            return self.db.zrange(self.key, start, stop)
        return self.db.zrange(self.key, index, index)

    def __iter__(self):
        yield from self.all()

    def __len__(self):
        return self.db.zcard(self.key)


class Hash:

    def __init__(self, db, key):
        self.db = db
        self.key = key
