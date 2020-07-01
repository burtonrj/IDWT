from config import GlobalConfig
import unittest


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

