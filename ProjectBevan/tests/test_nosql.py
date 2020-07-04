from ProjectBevan.nosql.patient import Comorbidity
from mongoengine import connect, disconnect
import unittest


class TestComorbidity(unittest.TestCase):

    def test_similarity(self):
        example = Comorbidity(comorbidName="Cancer")
        self.assertEqual(1, example.similarity("Cancer"))
        self.assertEqual(1, example.similarity("cancer", 1))
        self.assertEqual(0, example.similarity("hypertension", 1))
        self.assertEqual(0, example.similarity("hypertension", 2))
        self.assertEqual(0, example.similarity("hypertension", 3))
        self.assertEqual(1, example.similarity("Can-cer", 1))
        self.assertEqual(1, example.similarity("cancerous", 3))


class TestPerson(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls):
       disconnect()

    def test_thing(self):
        pers = Person(name='John')
        pers.save()

        fresh_pers = Person.objects().first()
        assert fresh_pers.name ==  'John'

