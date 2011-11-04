This directory contains queries for data published by GovTrack [1] in RDF [2]. 
These data are used with permission per the license [3].  A snapshot [4] of 
data was taken in March, 2010, and is available for download. In order to
provide stable basis for CI, the snapshot is used when running the queries
against these data.

The trig directory in the govtrack.zip file are those triples from [2], placed
into named graphs by Cambridge Semantics.  The additions directory are additional
quads inferred by Cambridge Semantics in order to have more complete rdf:type 
statements.  See the background directory for more information on this process.

The data are approximately 33 million statements (32,789,993 in the base data
with a total of 33,038,985 statements when the additional data is loaded). The
queries depend on both the base data and the additional data.

The context position in these data represents the original govtrack file
breakdown, which I believe is the a name-graph for each chamber's congressional 
sessions, for both bills and votes, plus a graph for the people and organizations.

[1] http://www.govtrack.us/
[2] http://www.govtrack.us/data/rdf/
[3] http://www.govtrack.us/developers/license.xpd
[4] http://www.bigdata.com/govtrack.zip
