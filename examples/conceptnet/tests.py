from unittest import TestCase


from helpers import trigrams
from helpers import concept_to_trigrams


def pk(*args):
    print args
    return args[-1]


class ConceptNetTestCase(TestCase):

    def test_trigrams(self):
        self.assertEqual(trigrams('word'), ['$wo', 'wor', 'ord', 'rd$'])

    def test_trigramsz(self):
        expected = ['$lo', 'loo', 'ook', 'ok$', '$fo', 'for', 'or$']
        self.assertEqual(concept_to_trigrams('/c/en/look_for'), expected)
