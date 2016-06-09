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

package com.bigdata.bop.solutions;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase.TestHelper;

/** A collection of tests for various implementations of GroupByOp.
 *  Unlike the unit tests in {@link AbstractAggregationTestCase }, this test 
 *  suite provides higher coverage in terms of the covered feature combinations,
 *  but weaker focus because it simply parses and runs the test queries from 
 *  files, so interference from other modules is likely. Also, the choice of 
 *  the particular {@link GroupByOp} implementation is made automatically
 *  by the tested implementation rather than by test config.
 * 
 *  All the current tests were initially used as tests for ticket BLZG 1202.
 * 
 *  @author <a href="mailto:ariazanov@blazegraph.com">Alexandre Riazanov</a>
 *  @since Apr 19, 2016
 */
public class TestGroupByOp extends AbstractDataDrivenSPARQLTestCase {

    public TestGroupByOp() {
        super();
    }

    public TestGroupByOp(String name) {
        super(name);
    }

    /**     
     * SELECT ?w (SAMPLE(?v) AS ?S)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
    
    public void test_ticket_1202a() throws Exception {

        new TestHelper("ticket-1202-group03",// testURI,
                "ticket-1202-group03.rq",// queryFileURL
                "ticket-1202-group03.ttl",// dataFileURL
                "ticket-1202-group03.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT?s ?w
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?s ?w
     */
    
    public void test_ticket_1202b() throws Exception {

        new TestHelper("ticket-1202-group05",// testURI,
                "ticket-1202-group05.rq",// queryFileURL
                "ticket-1202-group05.ttl",// dataFileURL
                "ticket-1202-group05.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?w (COUNT(DISTINCT ?v) AS ?S)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
    public void test_ticket_1202c() throws Exception {

        new TestHelper("ticket-1202-group03-modified1",// testURI,
                "ticket-1202-group03-modified1.rq",// queryFileURL
                "ticket-1202-group03-modified1.ttl",// dataFileURL
                "ticket-1202-group03-modified1.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT?s ?w (COUNT(DISTINCT ?v) AS ?c)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?s ?w
     */
    public void test_ticket_1202d() throws Exception {

        new TestHelper("ticket-1202-group05-modified1",// testURI,
                "ticket-1202-group05-modified1.rq",// queryFileURL
                "ticket-1202-group05-modified1.ttl",// dataFileURL
                "ticket-1202-group05-modified1.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT?w (SAMPLE(?v) AS ?S) (STR(?w) AS ?wstr)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
    
    public void test_ticket_1202e() throws Exception {

        new TestHelper("ticket-1202-group03-modified2",// testURI,
                "ticket-1202-group03-modified2.rq",// queryFileURL
                "ticket-1202-group03-modified2.ttl",// dataFileURL
                "ticket-1202-group03-modified2.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?w (COUNT(DISTINCT ?v) AS ?S) (STR(?w) AS ?wstr)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
        
    public void test_ticket_1202f() throws Exception {

        new TestHelper("ticket-1202-group03-modified3",// testURI,
                "ticket-1202-group03-modified3.rq",// queryFileURL
                "ticket-1202-group03-modified3.ttl",// dataFileURL
                "ticket-1202-group03-modified3.srx"// resultFileURL
                ).runTest();
    }
    
    
        
    /**      
     * SELECT?s ?w (STR(?w) AS ?wstr)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?s ?w
     */
    public void test_ticket_1202g() throws Exception {

        new TestHelper("ticket-1202-group05-modified2",// testURI,
                "ticket-1202-group05-modified2.rq",// queryFileURL
                "ticket-1202-group05-modified2.ttl",// dataFileURL
                "ticket-1202-group05-modified2.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?s ?w (COUNT(DISTINCT ?v) AS ?c) (STR(?w) AS ?wstr)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?s ?w
     */
        
    public void test_ticket_1202h() throws Exception {

        new TestHelper("ticket-1202-group05-modified3",// testURI,
                "ticket-1202-group05-modified3.rq",// queryFileURL
                "ticket-1202-group05-modified3.ttl",// dataFileURL
                "ticket-1202-group05-modified3.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?w (SAMPLE(?v) AS ?S) (?w AS ?u)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
     
    public void test_ticket_1202i() throws Exception {

        new TestHelper("ticket-1202-group03-modified4",// testURI,
                "ticket-1202-group03-modified4.rq",// queryFileURL
                "ticket-1202-group03-modified4.ttl",// dataFileURL
                "ticket-1202-group03-modified4.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?w (COUNT(DISTINCT ?v) AS ?S) (?w AS ?u)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
        
    public void test_ticket_1202j() throws Exception {

        new TestHelper("ticket-1202-group03-modified5",// testURI,
                "ticket-1202-group03-modified5.rq",// queryFileURL
                "ticket-1202-group03-modified5.ttl",// dataFileURL
                "ticket-1202-group03-modified5.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT?s ?w (?w AS ?u)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?s ?w
     */
    
    public void test_ticket_1202k() throws Exception {

        new TestHelper("ticket-1202-group05-modified4",// testURI,
                "ticket-1202-group05-modified4.rq",// queryFileURL
                "ticket-1202-group05-modified4.ttl",// dataFileURL
                "ticket-1202-group05-modified4.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT?s ?w (COUNT(DISTINCT ?v) AS ?c) (?w AS ?u)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?s ?w
     */
    public void test_ticket_1202l() throws Exception {

        new TestHelper("ticket-1202-group05-modified5",// testURI,
                "ticket-1202-group05-modified5.rq",// queryFileURL
                "ticket-1202-group05-modified5.ttl",// dataFileURL
                "ticket-1202-group05-modified5.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT(COUNT(?w) AS ?wcnt) (SAMPLE(?v) AS ?S)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     */

    public void test_ticket_1202m() throws Exception {

        new TestHelper("ticket-1202-additional1",// testURI,
                "ticket-1202-additional1.rq",// queryFileURL
                "ticket-1202-additional1.ttl",// dataFileURL
                "ticket-1202-additional1.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**      
     * SELECT(COUNT(?w) AS ?wcnt)  (COUNT(DISTINCT ?v) AS ?S)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     */
    public void test_ticket_1202n() throws Exception {

        new TestHelper("ticket-1202-additional2",// testURI,
                "ticket-1202-additional2.rq",// queryFileURL
                "ticket-1202-additional2.ttl",// dataFileURL
                "ticket-1202-additional2.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?w (COUNT(*) AS ?c)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
        
    public void test_ticket_1202o() throws Exception {

        new TestHelper("ticket-1202-additional3",// testURI,
                "ticket-1202-additional3.rq",// queryFileURL
                "ticket-1202-additional3.ttl",// dataFileURL
                "ticket-1202-additional3.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT?w (COUNT(DISTINCT *) AS ?c)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     * GROUP BY ?w
     */
    
    public void test_ticket_1202p() throws Exception {

        new TestHelper("ticket-1202-additional4",// testURI,
                "ticket-1202-additional4.rq",// queryFileURL
                "ticket-1202-additional4.ttl",// dataFileURL
                "ticket-1202-additional4.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT(COUNT(?w) AS ?u) (COUNT(*) AS ?c)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     */
    public void test_ticket_1202q() throws Exception {

        new TestHelper("ticket-1202-additional5",// testURI,
                "ticket-1202-additional5.rq",// queryFileURL
                "ticket-1202-additional5.ttl",// dataFileURL
                "ticket-1202-additional5.srx"// resultFileURL
                ).runTest();
    }
    
    /**      
     * SELECT(COUNT(?w) AS ?u) (COUNT(DISTINCT *) AS ?c)
     * {
     *  ?s :p ?v .
     *   OPTIONAL { ?s :q ?w }
     * }
     */
    
    public void test_ticket_1202r() throws Exception {

        new TestHelper("ticket-1202-additional6",// testURI,
                "ticket-1202-additional6.rq",// queryFileURL
                "ticket-1202-additional6.ttl",// dataFileURL
                "ticket-1202-additional6.srx"// resultFileURL
                ).runTest();
    }
    
        
   
} // class TestGroupByOp
