from config import GlobalConfig
from datetime import datetime
import unittest
import os


class TestGlobalConfig(unittest.TestCase):

    def test_type_assertation(self):
        def _test(obj, type_):
            try:
                config._type_assertation(str, str)
            except AssertionError:
                return True
            return False

        config = GlobalConfig()
        self.assertFalse(_test(str, str))
        self.assertFalse(_test(float, float))
        self.assertFalse(_test(int, int))
        self.assertTrue(_test(float, int))
        self.assertTrue(_test(str, int))

    def test_set_log_path(self):
        config = GlobalConfig()
        error = False
        for x in [123, "aaaaaaaaabbbbbbbbbccccccccccc/aaaaaaaaaabbbbbbbccccccc/test.txt"]:
            try:
                config.set_log_path(x)
            except AssertionError:
                error = True
        self.assertTrue(error)
        try:
            config.set_log_path(f"{os.getcwd()}/test.txt")
            error = False
        except AssertionError:
            error = True
        self.assertFalse(error)

    def test_write_to_log(self):
        config = GlobalConfig()
        config.set_log_path(f"{os.getcwd()}/test.txt")
        config.write_to_log("TESTING")
        with open(f"{os.getcwd()}/test.txt", "r") as f:
            self.assertEqual(f.readline(), f"{datetime.now()}: TESTING \n")
        os.remove(f"{os.getcwd()}/test.txt")
