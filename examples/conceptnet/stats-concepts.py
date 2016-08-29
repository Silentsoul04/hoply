from msgpack import Unpacker

from ajgudb import *


with open('concepts.txt') as f:
    relations = f.read().split('\n')


graph = AjguDB('db')


concepts = gremlin(FROM(label='concept'), get)(graph)

top = list()

# create top 100 of concept with the most relations
for concept in concepts:
    out = gremlin(outgoings, scatter, count)(graph, concept)
    inc = gremlin(incomings, scatter, count)(graph, concept)
    total = out + inc
    # add to top
    top.append((concept['concept'], total))
    top.sort(key=lambda x: x[1], reverse=True)
    top = top[:100]


for concept, total in top:
    print '*', concept, total
    out = gremlin(FROM(concept=concept), outgoings, scatter, key('relation'), group_count)(graph)
    out = out.most_common(5)
    for relation, count in out:
        print '**', concept, '-->', relation, count
    inc = gremlin(FROM(concept=concept), incomings, scatter, key('relation'), group_count)(graph)
    inc = inc.most_common(5)
    for relation, count in inc:
        print '**', concept, '<--', relation, count
        


graph.close()
