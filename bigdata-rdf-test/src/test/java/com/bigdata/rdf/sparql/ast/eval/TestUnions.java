/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

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
/*
 * Created on Oct 24, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for UNION.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestUnions extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestUnions() {
    }

    /**
     * @param name
     */
    public TestUnions(String name) {
        super(name);
    }

    /**
     * <pre>
     * SELECT ?p ?o
     * WHERE { 
     *     {
     *         <http://www4.wiwiss.fu-berlin.de/dailymed/resource/drugs/1080> ?p ?o . 
     *     } UNION { 
     *         <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB01254> ?p ?o . 
     *     } UNION { 
     *         <http://www4.wiwiss.fu-berlin.de/sider/resource/drugs/3062316> ?p ?o . 
     *     }
     * }
     * </pre>
     */
    public void test_union_01() throws Exception {

        new TestHelper("test_union_01").runTest();
        
    }

    /**
     * <pre>
     * select distinct ?s
     * where { {
     *   ?s a foaf:Person .
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Mike")
     * } union { 
     *   ?s a foaf:Person . 
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Jane")
     * } }
     * </pre>
     * 
     * Note: This is a port of TestBigdataEvaluationStrategyImpl#test_union().
     * 
     * @throws Exception
     */
    public void test_union_02() throws Exception {

        new TestHelper("union_02").runTest();
        
    }
    
    /**
     * <pre>
     * select distinct ?s
     * where { {
     *   ?s a foaf:Person .
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Mike")
     * } union { 
     *   ?s a foaf:Person . 
     *   ?s rdfs:label ?label .
     * } }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_union_03() throws Exception {

        new TestHelper("union_03").runTest();
        
    }

    /**
     * <pre>
     * select distinct ?s
     * where { {
     *   ?s a foaf:Person .
     *   ?s rdfs:label ?label .
     * } union { 
     *   ?s a foaf:Person . 
     *   ?s rdfs:label ?label .
     *   FILTER (?label = "Jane")
     * } }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_union_04() throws Exception {

        new TestHelper("union_04").runTest();
        
    }    
    
    /**
     * <pre>
     *  SELECT ?p ?o ?s
     *  WHERE { 
     *      {
     *          <http://example/foo> ?p ?o . 
     *      } UNION { 
     *          ?s ?p <http://example/foo> . 
     *      }
     *  }
     * </pre>
     */
    public void test_union_05() throws Exception {

        new TestHelper("union_05").runTest();
        
    }

    /*
     * ported from com.bigdata.rdf.sail.TestNestedUnions
     */
    
    // Note: was testSimplestNestedUnion().
    public void test_union_06() throws Exception {
        
        new TestHelper("union_06").runTest();
        
    }

    // Note: was testNestedUnionWithOptionals().
    public void test_union_07() throws Exception {
        
        new TestHelper("union_07").runTest();
        
    }

    // Note: was testForumBug() which used union.ttl for its data.
    public void test_union_08() throws Exception {
        
        new TestHelper(
                "union_08", // testURI,
                "union_08.rq",// queryFileURL
                "union_08.ttl",// dataFileURL
                "union_08.srx"// resultFileURL
                ).runTest();
        
    }

    /**
     * <pre>
     * PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
     * PREFIX :     <http://example.org/book/> 
     * PREFIX ns:   <http://example.org/ns#> 
     * 
     * SELECT ?book
     * {
     *    {}
     *    UNION
     *    { ?book dc:title ?title }
     * }
     * </pre>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/504">
     *      UNION with Empty Group Pattern </a>
     */
    public void test_union_with_empty() throws Exception {
        
        new TestHelper(
                "union_with_empty", // testURI,
                "union_with_empty.rq",// queryFileURL
                "union_with_empty.ttl",// dataFileURL
                "union_with_empty.srx"// resultFileURL
                ).runTest();
        
    }    

    /**
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/831">Union with FILTER
     *      issue</a>
     */
    public void test_union_ticket_831() throws Exception {
        
        new TestHelper(
                "ticket_831", // testURI,
                "ticket_831.rq",// queryFileURL
                "ticket_831.ttl",// dataFileURL
                "ticket_831.srx"// resultFileURL
                ).runTest();
        
    }    

    /**
     * The original query.
     * @see <a href="http://trac.blazegraph.com/ticket/874">FILTER not applied when
     *      there is UNION in the same join group</a>
     */
    public void test_union_ticket_874() throws Exception {
        
        new TestHelper(
                "ticket_874", // testURI,
                "ticket_874.rq",// queryFileURL
                "ticket_874.ttl",// dataFileURL
                "ticket_874.srx"// resultFileURL
                ).runTest();
        
    }    

    /**
     * A rewrite of the original query that works.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/874">FILTER not applied when
     *      there is UNION in the same join group</a>
     */
    public void test_union_ticket_874b() throws Exception {
        
        new TestHelper(
                "ticket_874b", // testURI,
                "ticket_874b.rq",// queryFileURL
                "ticket_874.ttl",// dataFileURL
                "ticket_874.srx"// resultFileURL
                ).runTest();
        
    }    

    /**
     * This is DAWG sparql11-subquery-05. It fails in scale-out.
     * 
     * <pre>
     * SELECT * 
     * WHERE { 
     *     {
     *       SELECT ?s 
     *       WHERE {?s ?p ?o}
     *       LIMIT 1 
     *     }
     *     {?s ?p ?o} 
     *     UNION 
     *     {} 
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_union_ticket_944() throws Exception {
        
        new TestHelper(
                "ticket_944", // testURI,
                "ticket_944.rq",// queryFileURL
                "ticket_944.trig",// dataFileURL
                "ticket_944.srx"// resultFileURL
                ).runTest();
        
    }
    
}
