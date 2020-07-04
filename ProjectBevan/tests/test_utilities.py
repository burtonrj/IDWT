from utilities import parse_datetime
import unittest


class TestUtilities(unittest.TestCase):

    def test_parse_datetime(self):
        t1 = parse_datetime("15/03/2020")
        t2 = parse_datetime("1/30/2020")
        t3 = parse_datetime("15/3/2020 15:00:00")
        t4 = parse_datetime("15/3/2020 15:35")
        t5 = parse_datetime("12/01/2020 00:00:00")
        t6 = parse_datetime("12/01/2020 2pm")
        t7 = parse_datetime("12/01/2020 2:30pm")

        self.assertEqual(t1.get("date"), "15/3/2020")
        self.assertIsNone(t1.get("time"))

        self.assertIsNone(t2.get("date"))

        self.assertEqual(t3.get("date"), "15/3/2020")
        self.assertEqual(t3.get("time"), 15*60)

        self.assertEqual(t4.get("date"), "15/3/2020")
        self.assertEqual(t4.get("time"), (15 * 60)+35)

        self.assertEqual(t5.get("date"), "12/1/2020")
        self.assertEqual(t5.get("time"), 0)

        self.assertEqual(t6.get("date"), "12/1/2020")
        self.assertEqual(t6.get("time"), 14*60)

        self.assertEqual(t7.get("date"), "12/1/2020")
        self.assertEqual(t7.get("time"), (14 * 60)+30)
