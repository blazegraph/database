
from pymantic.rdf import *
from pymantic.parsers import turtle_parser
from urllib.request import urlopen
Resource.prefixes['foaf'] = Prefix('http://xmlns.com/foaf/0.1/')

graph = turtle_parser.parse(open('../data/foaf-bond.ttl').read())
bond_james = Resource(graph, 'http://example.org/stuff/Bond')
print ("%s knows:" % (bond_james.get_scalar('foaf:name'),))
for person in bond_james['foaf:knows']:
    print (person.get_scalar('foaf:name'))

for person in bond_james['foaf:knows']:
    print (person)
