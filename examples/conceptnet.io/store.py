import sys

import hoply as h
from hoply.okvs.wiredtiger import WiredTiger
from fuzzyhash import simhash
from hoply.tuple import pack
from hoply.tuple import unpack


def humanize(concept):
    return concept.split('/')[3].replace('_', ' ')


with WiredTiger(sys.argv[1]) as storage:
    with open(sys.argv[2]) as f:
        for index, line in enumerate(f):
            if index % 10_000 == 0:
                print(index)
            values = line.split('\t')
            source = humanize(values[2])
            target = humanize(values[3])
            with h.transaction(storage) as tr:
                tr.add(pack((simhash(source), source)), b'')
                tr.add(pack((simhash(target), target)), b'')
