from msgpack import Unpacker

from ajgudb import Edge
from ajgudb import Vertex
from ajgudb import AjguDB


example = {
    u'id': u'/e/ff75ac33f095b9b2ee53ea6630e1a80be7e0a1b0',
    u'start': u'/c/af/afghanistan',
    u'rel': u'/r/Antonym',
    u'surfaceText': None,
    u'end': u'/c/af/pakistan',
    u'context': u'/ctx/all',

    u'source_uri': u'/and/[/s/rule/synonym_section/,/s/web/de.wiktionary.org/wiki/Afghanistan/]',
    u'uri': u'/a/[/r/Antonym/,/c/af/afghanistan/,/c/af/pakistan/]',
    u'dataset': u'/d/wiktionary/de/af',
    u'license': u'/l/CC/By-SA',

    u'weight': 1.0,
    u'sources': [
        u'/s/rule/synonym_section',
        u'/s/web/de.wiktionary.org/wiki/Afghanistan'
    ],
    u'features': [
        u'/c/af/afghanistan /r/Antonym -',
        u'/c/af/afghanistan - /c/af/pakistan',
        u'- /r/Antonym /c/af/pakistan'
    ]
}


def load():
    db = AjguDB('/tmp/ajgudb')
    relations = set()
    for index in range(1):
        name = 'data/conceptnet/part_0{}.msgpack'.format(index)
        with open(name, 'rb') as stream:
            print(name)
            unpacker = Unpacker(stream, encoding='utf-8')
            for value in unpacker:
                start = value.pop('start')
                end = value.pop('end')
                if start.startswith('/c/en') and end.startswith('/c/en'):
                    relation = value.pop('rel')
                    relations.add(relation)
                    start = Vertex(name=start)
                    end = Vertex(name=end)
                    relation = start.link(end, relation=relation)
                    db.save(relation)
                    print '.',
    for relation in relations:
        print relation
    db.close()


if __name__ == '__main__':
    load()
