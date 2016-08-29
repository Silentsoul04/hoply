from msgpack import Unpacker

from ajgudb import *


with open('relations.txt') as f:
    relations = f.read().split('\n')


graph = AjguDB('/tmp/ajgudb')
for relation in relations:
    # skip list
    if relation in ('/r/InstanceOf', '/r/IsA'):
        continue
    print '* ', relation
    # compute stats
    query = gremlin(
        FROM(relation=relation),
        end,
        outgoings,
        scatter,
        key('relation'),
        group_count
    )
    counter = query(graph)
    for k, v in sorted(counter.items(), key=lambda x: x[1], reverse=True)[:5]:
        print relation, '-> ... ->', k, '(count=%s)' % v

graph.close()
