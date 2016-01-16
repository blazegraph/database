/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Tests concerning:
 * <pre>
SELECT ?A
WHERE {
    ?A rdf:type  / rdfs:subClassOf * <os:ClassA> ;
       rdf:value ?B .
    ?B rdf:type  / rdfs:subClassOf *  <os:ClassB> .
}
</pre>
There is a work-around which is to replace a * with a UNION of a zero and a +

<pre>
    { 
      { ?B rdf:type  <os:ClassB> }
      UNION
      {  ?B rdf:type  / rdfs:subClassOf + <os:ClassB> 
      }
    }
</pre>    
In property-path-734-B-none.rq, this is taken as the following variant:
<pre>
    { 
      { ?A rdf:type  / rdfs:subClassOf  ? <os:ClassA> }
      UNION
      {  ?A rdf:type  /  rdfs:subClassOf / rdfs:subClassOf + <os:ClassA> 
      }
    }
</pre>   
and in property-path-734-B-workaround2.rq it is taken to (the broken):
<pre>
    ?A rdf:type  / ( rdfs:subClassOf ? | ( rdfs:subClassOf + / rdfs:subClassOf  ) )
            <os:ClassA> .
</pre>            
            
and in property-path-734-B-workaround3.rq it is taken to (the working):
<pre>
    ?A ( ( rdf:type  / rdfs:subClassOf ? ) | ( rdf:type  / rdfs:subClassOf + / rdfs:subClassOf  ) )
            <os:ClassA> .
</pre>      
            
 */
public class TestTwoPropertyPaths734 extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestTwoPropertyPaths734() {
    }

    /**
     * @param name
     */
    public TestTwoPropertyPaths734(String name) {
        super(name);
    }

    private void property_path_test(String name) throws Exception {

        new TestHelper(
                "property-path-734-" + name,         // testURI,
                "property-path-734-" + name + ".rq", // queryFileURL
                "property-path-734.ttl",             // dataFileURL
                "property-path-734.srx"              // resultFileURL,
                ).runTest();
    }

    private void property_path_using_workaround_test(String name) throws Exception {

        new TestHelper(
                "property-path-734-B-" + name,         // testURI,
                "property-path-734-B-" + name + ".rq", // queryFileURL
                "property-path-734-B.ttl",             // dataFileURL
                "property-path-734-B.srx"              // resultFileURL,
                ).runTest();
    }
    public void test_no_property_paths() throws Exception {
        property_path_test("none");
    }
    public void test_first_property_path() throws Exception {
        property_path_test("first");
    }
    public void test_second_property_path() throws Exception {
        property_path_test("second");
    }
    public void test_both_property_paths() throws Exception {
        property_path_test("both");
    }
    public void test_no_using_workaround_property_paths() throws Exception {
        property_path_using_workaround_test("none");
    }
    public void test_first_using_workaround_property_path() throws Exception {
        property_path_using_workaround_test("first");
    }
    public void test_second_using_workaround_property_path() throws Exception {
        property_path_using_workaround_test("second");
    }
    public void test_both_using_workaround_property_paths() throws Exception {
        property_path_using_workaround_test("both");
    }
    public void test_both_using_workaround2_property_paths() throws Exception {
        property_path_using_workaround_test("workaround2");
    }
    public void test_both_using_workaround3_property_paths() throws Exception {
        property_path_using_workaround_test("workaround3");
    }
    public void test_both_using_workaround4_property_paths() throws Exception {
        property_path_using_workaround_test("workaround4");
    }
    

    public void test_minimal_star_734() throws Exception {

        new TestHelper(
                "property-path-734-C",         // testURI,
                "property-path-734-C-star.rq", // queryFileURL
                "property-path-734-C.ttl",             // dataFileURL
                "property-path-734-C.srx"              // resultFileURL,
                ).runTest();
    } 
    public void test_minimal_opt_734() throws Exception {

        new TestHelper(
                "property-path-734-C",         // testURI,
                "property-path-734-C-opt.rq", // queryFileURL
                "property-path-734-C.ttl",             // dataFileURL
                "property-path-734-C.srx"              // resultFileURL,
                ).runTest();
    } 
    public void test_minimal_plus_734() throws Exception {

        new TestHelper(
                "property-path-734-C",         // testURI,
                "property-path-734-C-plus.rq", // queryFileURL
                "property-path-734-C.ttl",             // dataFileURL
                "property-path-734-C-plus.srx"              // resultFileURL,
                ).runTest();
    }
}
