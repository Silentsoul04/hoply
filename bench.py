import string
from pathlib import Path

import hoply as h
from hoply.wiredtiger import WiredTigerConnexion as Cnx


def TripleStoreDB(cnx):
    out = h.open(cnx, "hoply-test", ("subject", "predicate", "object"))
    return out


filenames = (Path(__file__).parent / "data" / "wikipedia-vital-articles-level-3/").glob(
    "*/*/*"
)
filenames = list(filenames)


for index, filename in enumerate(filenames):
    print("{}/{}".format(index, len(filenames)), filename)
    with TripleStoreDB(Cnx("wt")) as db:
        with filename.open("r") as f:
            html = f.read()
        text = html.translate(str.maketrans(" ", " ", string.punctuation))

        @h.transactional
        def query(tr):
            tr.add(str(filename), "text", text)
            for word in text.split():
                tr.add(str(filename), "word", word)

        query(db)
