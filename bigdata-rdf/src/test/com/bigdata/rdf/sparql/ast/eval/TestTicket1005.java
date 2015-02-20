/**

Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sparql.ast.eval;


/**
 * Test suite for an issue where ill designed patterns consisting of a
 * combination of UNION and nested OPTIONAL cause problems.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1087">
 * Named subquery results not referenced within query (bottom-up evaluation)</a>
 */
public class TestTicket1005 extends AbstractDataDrivenSPARQLTestCase {

    public TestTicket1005() {
    }

    public TestTicket1005(String name) {
        super(name);
    }

    /**
     * TEST CASES (TO BE WRITTEN): correct acceptance and rejectance of the
     * following queries:

Use Case from ticket (WITH ….)

—

PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns: <http://example.org/ns#>
INSERT DATA
{ GRAPH <http://example/bookStore> { <http://example/book1>  ns:price  42 } }

—

PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns: <http://example.org/ns#>
INSERT 
{ <http://example/book1>  ns:price  42 }
WHERE
{
  GRAPH <http://test> { <http://a> <http://a> <http://a> }
}

—

PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns: <http://example.org/ns#>
INSERT 
{ <http://example/book1>  ns:price  42 }
WHERE
{
  GRAPH <http://test> { <http://a> <http://a> <http://a> }
}

—

PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ns: <http://example.org/ns#>
DELETE 
{ <http://example/book1>  ns:price  42 }
WHERE
{
  GRAPH <http://test> { <http://a> <http://a> <http://a> }
}

— 
SELECT * 
FROM NAMED <http://example.org/alice>
WHERE { ?s ?p ?o }

— 

SELECT * 
WHERE { GRAPH ?g { ?s ?p ?o } }

—

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

INSERT {?x rdfs:label ?y . } USING ex:graph1 WHERE {?x foaf:name ?y } 



     */
// TODO: hook in at right positions
    
    /*
    public void test_ticket_1087_1() throws Exception {
        
        new TestHelper(
                "ticket_1087_1", // testURI,
                "ticket_1087_1.rq",// queryFileURL
                "ticket_1087.trig",// dataFileURL
                "ticket_1087_1.srx"// resultFileURL
                ).runTest();

    }
    */
}
