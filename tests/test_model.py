import time
import unittest
from redisor import get_client

from redisor import model


class Person(model.Model):

    __database__ = get_client()

    rank = model.AutoIncrementField(name='rank')
    name = model.StringField(name='name')
    address = model.StringField(name='address', default='1998')
    create_at = model.IntegerField(name='create_at', default=time.time)
    friend = model.ListField(name='friend')
    more_info = model.HashField(name='more_info')
    others = model.JsonField(name="others")

    def to_dict(self):
        return {
            "id": self.id,
            "rank": self.rank,
            "name": self.name,
            "address": self.address,
            "create_at": self.create_at,
            "friend": self.friend,
            "more_info": self.more_info,
            "others": self.others
        }


class ModelTestCase(unittest.TestCase):

    def setUp(self):
        self.client = get_client()
        self.client.flushdb()

    def tearDown(self):
        self.client.flushdb()

    def test_key(self):
        self.assertEqual('Person', Person._key)

    def test_is_new_and_save(self):
        liming = Person(name="Liming", address="China", friend=["HanMeimei", "ZhangTiezhu"],
                   more_info={"age": 13}, others={"family": ["papa", "mama"]})
        self.assertTrue(liming.is_new())
        liming.save()
        self.assertFalse(liming.is_new())
        self.assertEqual("1", liming.id)

    def test_query(self):
        liming = Person(name="Liming", address="China", friend=["HanMeimei", "ZhangTiezhu"],
                   more_info={"age": 13}, others={"family": ["papa", "mama"]})
        liming.save()
        p = Person.objects.get(1)
        print(p.to_dict())
        self.assertEqual(8, len(p.to_dict()))
        p2 = Person.objects.all()[0]
        # self.assertEqual(p, p2)


if __name__ == "__main__":
    unittest.main()
