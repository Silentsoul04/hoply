import sys

import hoply as h
from hoply.okvs.wiredtiger import WiredTiger
from fuzzyhash import simhash
from hoply.tuple import pack
from hoply.tuple import unpack


def humanize(concept):
    out = concept.split("/")
    return out[2], out[3].replace("_", " ").lower()


english = set()


with open(sys.argv[1]) as f:
    for index, line in enumerate(f):
        if index % 10000 == 0:
            print(index, file=sys.stderr)
        values = line.split("\t")
        source_lang, source = humanize(values[2])
        target_lang, target = humanize(values[3])
        if source_lang == "en" and source not in english:
            english.add(source)
            print(source)
        if target_lang == "en" and target not in english:
            english.add(target)
            print(target)
