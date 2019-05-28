import base64
import string
from pathlib import Path
from fdb import tuple

filenames = Path(__file__).parent / "data" / "wikipedia-vital-articles-level-3/"
filenames = filenames.glob("*/*/*")


def pack(v):
    return base64.b64encode(tuple.pack(v)).decode("ascii") + "\n"


with open("out.log", "w") as out:
    for index, filename in enumerate(filenames):
        print(index, filename)
        out.write("# BEGIN TRANSACTION\n")
        with filename.open("r") as f:
            html = f.read()
        text = html.translate(str.maketrans(" ", " ", string.punctuation))

        out.write(pack((1, str(filename), "text", text)))
        out.write(pack((2, "text", text, str(filename))))
        out.write(pack((3, text, str(filename), "text")))

        for word in text.split():
            out.write(pack((1, str(filename), "word", word)))
            out.write(pack((2, "word", word, str(filename))))
            out.write(pack((3, word, str(filename), "word")))
