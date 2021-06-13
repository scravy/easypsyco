import unittest

import easypsyco


# noinspection SqlNoDataSourceInspection
class MockTestCase(unittest.TestCase):
    def test_empty_mock(self):
        q = easypsyco.Queryable.mock()
        count = 0
        with q.select("SELECT * FROM table") as rows:
            for _ in rows:
                count += 1
        self.assertEqual(0, count)

    def test_some_mock(self):
        q = easypsyco.Queryable.mock({
            "SELECT .* FROM table": [['foo', 'bar']]
        })
        results = []
        with q.select("SELECT * FROM table") as rows:
            for a, b in rows:
                results.append(f"{a} {b}")
        self.assertEqual(["foo bar"], results)


if __name__ == '__main__':
    unittest.main()
