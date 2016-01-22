Here is the summary by Cambridge Semantics on how the rdf:type data were
generated. The various queries referenced below are found in this directory.

  ----

BaseURI=http://www.rdfabout.com/rdf/usgov/congress/people
InputFile=./people.rdf OutputFile=./trig/people.trig BaseURI=http://www.rdfabout.com/rdf/usgov/congress/people
InputFile=./people.roles.rdf OutputFile=./trig/people.roles.trig
BaseURI=http://www.rdfabout.com/rdf/usgov/congress InputFile=./orgs.rdf OutputFile=./trig/trig/orgs.trig

For(1-111){
 BaseURI=http://www.rdfabout.com/rdf/usgov/congress/{n}/house
InputFile=./votes.{n}.h.rdf OutputFile=./trig/votes.{n}.h.trig
 BaseURI=http://www.rdfabout.com/rdf/usgov/congress/{n}/senate
InputFile=./votes.{n}.s.rdf OutputFile=./trig/votes.{n}.s.trig
 If(i>=99){
   BaseURI=http://www.rdfabout.com/rdf/usgov/congress/{n}/bills
InputFile=./bills.{n}.actions.rdf OutputFile=./trig/bills.{n}.actions.trig
   BaseURI=http://www.rdfabout.com/rdf/usgov/congress/{n}/bills
InputFile=./bills.{n}.cosponsors.rdf
OutputFile=./trig/bills.{n}.cosponsors.trig
   BaseURI=http://www.rdfabout.com/rdf/usgov/congress/{n}{/bills
InputFile=./bills.{n}.rdf OutputFile=./trig/bills.{n}.trig
   BaseURI=http://www.rdfabout.com/rdf/usgov/congress/{n}/bills
InputFile=./bills.{n}.terms.rdf OutputFile=./trig/bills.{n}.terms.trig
 }
}
