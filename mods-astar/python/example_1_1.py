
from pymantic.rdf import *
from pymantic.parsers import turtle_parser
from pymantic.sparql import SPARQLServer
from urllib.request import urlopen

server = SPARQLServer('http://172.17.0.1:9999/blazegraph/sparql')

u='''
clear all;

load <file:///home/gserdyuk/epddp/database-astr/mods-astar/data/triples_dump.n3>;
'''

ans1=server.update(u)

print(ans1)

print('----------------------')

q='''
# example 1
  PREFIX gas: <http://www.bigdata.com/rdf/gas#>
  SELECT ?depth ?predecessor ?linkType ?out {
  SERVICE gas:service {
     gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.ASTAR" .
     gas:program gas:in <http://sunflower.test/asn_cidr/85.142.164.0%2F22> . # one or more times, specifies the initial frontier.
     gas:program gas:target <http://sunflower.test/phone/%2B7+812+670+00+00> . # only retain vertices along paths to these target vertices.
     gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
     gas:program gas:out1 ?depth . # exactly once - will be bound to the depth of the visited vertices.
     gas:program gas:out2 ?predecessor . # exactly once - will be bound to the predecessor.
     gas:program gas:maxIterations 10 . # optional limit on breadth first expansion.
     gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
  }
  ?predecessor ?linkType ?out . # figure out what link type(s) connect a vertex with a predecessor
} 
order by desc(?depth)
limit 100
'''
ans=server.query(q)

print(ans)

for iter in ans['results']['bindings']:
    print ( iter['out']['value'] + " <--(" + iter['linkType']['value'] +")--<< "+ iter['predecessor']['value'])

