from collections import Counter
from itertools import chain

from ajgudb import *  # noqa


def sanitize(name):
    return name[len('/c/en/'):].split('_')

def trigrams(word):
    out = list()
    token = '$' + word + '$'
    for c in range(len(token) - 2):
        out.append((token[c] + token[c+1] + token[c+2]))
    return out

def words_to_trigrams(words):
    out = list()
    for word in words:
        out += trigrams(word)
    return out


def search(graph, words):
    trigrams = words_to_trigrams(words)
    counter = Counter()
    for trigram in trigrams:
        query = gremlin(
            FROM(trigram=trigram),
            group_count,
        )
        counter += query(graph)
    for k, v in sorted(counter.items(), key=lambda x: x[1], reverse=True):
        print k, v
