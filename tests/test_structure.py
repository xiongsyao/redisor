import unittest

from redisor import get_client, setup
from redisor.structure import List, Set, Hash


class BaseTestMixin(unittest.TestCase):
    def setUp(self):
        setup(db=12)
        self.db = get_client()
        self.db.flushdb()
        super().setUp()

    def tearDown(self):
        self.db.flushdb()
        super().tearDown()


class ListTestCase(BaseTestMixin, unittest.TestCase):

    def test_operation(self):
        self.lst = List(db=self.db, key="test_list")
        self.assertEqual(self.lst.unshift("a"), 1)
        self.assertEqual(len(self.lst), 1)
        self.assertEqual(self.lst.append("b"), 2)
        self.assertEqual(self.lst.pop(), "b")
        self.assertFalse("b" in self.lst)
        self.assertEqual(list(self.lst), ["a"])


class SetTestCase(BaseTestMixin, unittest.TestCase):

    def test_base_operation(self):
        self.set_a = Set(db=self.db, key="test_set_a")
        self.set_a.add('one')
        self.assertEqual(len(self.set_a), 1)
        self.set_a.remove("one")
        self.set_a.add("two")
        self.assertEqual(list(self.set_a), ["two"])

    def test_tow_set_operation(self):
        self.set_b = Set(db=self.db, key="test_set_b")
        self.set_c = Set(db=self.db, key="test_set_c")
        self.set_b.add("one")
        self.set_b.add("two")
        self.set_c.add("two")
        self.set_c.add("three")
        self.assertEqual(self.set_b & self.set_c, {"two"})
        self.assertEqual(self.set_b | self.set_c, {"one", "two", "three"})
        self.assertEqual(self.set_b - self.set_c, {"one"})
        self.assertEqual(self.set_c - self.set_b, {"three"})


class HashTestCase(BaseTestMixin, unittest.TestCase):

    def test_base_operation(self):
        self.hash_a = Hash(db=self.db, key="test_hash_a")
        self.hash_a.update({"a": 3}, b=4)
        self.assertEqual(self.hash_a.all(), {"a": '3', "b": '4'})
        del self.hash_a["a"]
        self.assertEqual(self.hash_a.all(), {"b": "4"})
        self.hash_a.update({"1":1,"2":2,"3":3,"4":4,"5":5,"6":6})
        print(self.hash_a)


if __name__ == "__main__":
    unittest.main()
