import time
import sys
from collections import Counter

import hoply as h
from hoply.okvs.wiredtiger import WiredTiger
from fuzzyhash import simhash
import fuzzyhash
from hoply.tuple import pack
from hoply.tuple import unpack
from Levenshtein import distance as levenshtein


path = sys.argv[1]
query = sys.argv[2]

candidates = set()

LIMIT = int(sys.argv[3])

start = 0
with WiredTiger(path) as storage:
    start = time.time()
    with h.transaction(storage) as tr:
        for count in range(len(query), 0, -1):
            prefix = query[0:count]
            prefix = pack((prefix,))
            # strip the very last \x00 byte
            prefix = prefix[0 : len(prefix) - 1]
            for key, _ in tr.prefix(prefix):
                concept, = unpack(key)
                candidates.add(concept)
            if len(candidates) > (LIMIT * 10):
                break


distances = Counter()
for concept in candidates:
    distance = fuzzyhash.distance(
        fuzzyhash.fuzzyhash(query), fuzzyhash.fuzzyhash(concept)
    )
    distances[concept] = -distance

concepts = [c for (c, s) in distances.items()]

concepts.sort(key=lambda x: levenshtein(x, query))
concepts = concepts[0:LIMIT]

end = time.time()

for concept in concepts:
    print(concept)


print("\n\nTime spent: ", end - start)
