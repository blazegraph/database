import json
from pymantic.rdf import *
from pymantic.parsers import turtle_parser
from pymantic.sparql import SPARQLServer
from urllib.request import urlopen

server = SPARQLServer('http://172.17.0.1:9999/blazegraph/sparql')

u='''
clear all;

load <file:///home/gserdyuk/epddp/database-astr/mods-astar/data/triples_dump.ttl>;
'''

ans1=server.update(u)

print(json.dumps(ans1, indent=4, default=str))

print('----------------------')

q='''
# example 2
PREFIX gas: <http://www.bigdata.com/rdf/gas#>
SELECT ?depth ?predecessor ?linkType ?out{
  SERVICE gas:service {
     gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.ASTAR" .
     gas:program gas:in <http://sunflower.test/handle/IS111-RIPE> . # one or more times, specifies the initial frontier.
     gas:program gas:target <http://sunflower.test/asn_cidr/87.117.128.0%2F12>. # only retain vertices along paths to these target vertices.
     # gas:program gas:target <http://sunflower.test/handle/> . # only retain vertices along paths to these target vertices.
     gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
     gas:program gas:out1 ?depth . # exactly once - will be bound to the depth of the visited vertices.
     gas:program gas:out2 ?predecessor . # exactly once - will be bound to the predecessor.
     gas:program gas:maxIteration 10 . # optional limit on breadth first expansion.
     gas:program gas:maxVisited 500 . # optional limit on the #of visited vertices.
     gas:program gas:traversalDirection 'Undirected' . # optional limit on the #of visited vertices.
     gas:program gas:nthreads 1 .
     gas:program gas:epv <rdf:type> .  # disallow type (like Handle)
     # gas:program gas:epv <http://sunflower.test/is_admin_c_of> .  # disallow another type

  } 
  #?predecessor ?linkType ?out
} 
order by ?depth
'''
ans=server.query(q)

print(json.dumps(ans, indent=4, default=str))

for iter in ans['results']['bindings']:
    print ( iter['out']['value'] + " <--(" + iter.get('linkType',{'value':' '})['value'] + ")--<< "+ iter.get('predecessor',{'value':' '})['value'])

