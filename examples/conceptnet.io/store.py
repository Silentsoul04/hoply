import sys

import hoply as h
from hoply.okvs.wiredtiger import WiredTiger
from fuzzyhash import simhash
from hoply.tuple import pack
from hoply.tuple import unpack


with WiredTiger(sys.argv[1]) as storage:
    with open(sys.argv[2]) as f:
        for index, line in enumerate(f):
            if index % 10000 == 0:
                print(index)
            concept = line.strip()
            with h.transaction(storage) as tr:
                tr.add(pack((concept,)), b"")
