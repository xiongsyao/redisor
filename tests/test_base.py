import unittest
from redisor import get_client, setup


class BasicsTestCase(unittest.TestCase):
    def setUp(self):
        setup(db=12)
        self.db = get_client()
        self.db.flushdb()

    def tearDown(self):
        self.db.flushdb()

    def test_connect_db(self):
        self.db.set("ping", "pong")
        self.assertEqual(self.db.get("ping"), "pong")


if __name__ == "__main__":
    unittest.main()
