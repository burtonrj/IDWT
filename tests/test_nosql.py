from ..nosql.patient import Comorbidity
import unittest


class TestComorbidity(unittest.TestCase):

    def test_similarity(self):
        example = Comorbidity(comorbidName="Cancer")
        self.assertEqual(2, example.similarity("Cancer"))
        self.assertEqual(1, example.similarity("cancer", 1))
        self.assertEqual(0, example.similarity("hypertension", 1))
        self.assertEqual(0, example.similarity("hypertension", 2))
        self.assertEqual(0, example.similarity("hypertension", 3))
        self.assertEqual(1, example.similarity("Can-cer", 1))
        self.assertEqual(1, example.similarity("cancerous", 3))

