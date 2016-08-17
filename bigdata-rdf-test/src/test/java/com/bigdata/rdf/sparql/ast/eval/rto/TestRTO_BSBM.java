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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.rto;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.QueryHints;

/**
 * Data driven test suite for the Runtime Query Optimizer (RTO) using BSBM data
 * and queries based on BSBM.
 * <p>
 * Note: BSBM is parameterized. We can generate more queries against the pc100
 * data set easily enough. In priciple, those queries might exhibit different
 * correlations. However, the pc100 data set may be too small for any
 * interesting correlations. In fact, it may be too small since the vertex
 * estimates and cutoff joins may be exact before the RTO is run running. If so,
 * then we need to go back and use a larger data set. However, the specific
 * parameterized queries will remain valid against larger data sets since BSBM
 * only adds more data when generating a larger data set. Of course, the number
 * of solutions for the queries may change.
 * <p>
 * Note: BSBM uses a lot of filters, subgroups, and sub-selects. As we build up
 * coverage for those constructions in the RTO, it will handle more of the
 * query. As a result, the observed join orders (and even the #of joins that are
 * considered) are likely to change.
 * <p>
 * Note: Q6 is no longer run in BSBM (the query was dropped).
 * <p>
 * Note: Q9 is a simple DESCRIBE (too simple for the RTO). Sample query is:
 * 
 * <pre>
 * PREFIX rev: <http://purl.org/stuff/rev#>
 * 
 * DESCRIBE ?x
 * WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review576> rev:reviewer ?x }
 * </pre>
 * 
 * Note: Q12 is a UNION (too simple for the RTO). Sample query is:
 * 
 * <pre>
 * PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
 * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
 * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * 
 * SELECT ?property ?hasValue ?isValueOf
 * WHERE {
 *   { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer134> ?property ?hasValue }
 *   UNION
 *   { ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer134> }
 * }
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6440 2012-08-14 17:57:33Z thompsonbry $
 */
public class TestRTO_BSBM extends AbstractRTOTestCase {

//    private final static Logger log = Logger.getLogger(TestRTO_LUBM.class);
    
    /**
     * 
     */
    public TestRTO_BSBM() {
    }

    /**
     * @param name
     */
    public TestRTO_BSBM(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(BigdataSail.Options.TRIPLES_MODE, "true");

        properties.setProperty(BigdataSail.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        return properties;
        
    }

    /**
     * Test of BSBM Q1 against an empty data set. There are no solutions in the
     * data. 
     */
    public void test_BSBM_Q1_noSolutions() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q1", // testURI,
                "rto/BSBM-Q1.rq",// queryFileURL
                new String[]{},// data files.
                "rto/BSBM-Q1-noSolutions.srx"// resultFileURL
        );
        
        /*
         * TODO In fact, the RTO should not be running for a group of required
         * joins in which some vertex has a zero cardinality or when any join
         * can provably produce ZERO results when fed solutions from a fully
         * materialized vertex.
         */

        assertSameJoinOrder(new int[] { 2, 1, 3, 4, 5 }, helper);
      
    }

    /**
     * BSBM Q1 against pc100.
     */
    public void test_BSBM_Q1_pc100() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q1", // testURI,
                "rto/BSBM-Q1.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q1.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         * 
         * FIXME There are two different solutions that I see for this query
         * depending on whether or not AST2BOpRTO.runAllJoinsAsComplexJoins is
         * true or false. I have modified the test to allow either join ordering
         * for now, but we should chase down the root cause for this difference
         * in how the simple and complex cutoff join evaluation code paths
         * compute the join hit ratios and estimated cardinality. It is probably
         * an off by one fencepost....
         */

        try {
            assertSameJoinOrder(new int[] { 2, 4, 1, 3, 5 }, helper);
        } catch (AssertionFailedError er) {
            log.warn(er);
        }
        assertSameJoinOrder(new int[] { 3, 2, 4, 1, 5 }, helper);
      
    }
    
    /**
     * BSBM Q2 against pc100.
     */
    public void test_BSBM_Q2_pc100() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q2", // testURI,
                "rto/BSBM-Q2.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q2.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 3, 4, 5, 1, 2, 6, 7, 8, 9, 10, 11, 12 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q3 against pc100.
     */
    public void test_BSBM_Q3_pc100() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q3", // testURI,
                "rto/BSBM-Q3.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q3.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 2, 5, 1, 3, 4 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q4 against pc100.
     * <p>
     * Note: This query has TWO join groups that are sufficiently complex to run
     * the RTO. However, only one of the join groups is marked for RTO
     * optimization in order to keep the test harness simple. The test harness
     * assumes that there is a single JOIN group that is optimized by the RTO
     * and then verifies the join ordering within that join group. The test
     * harness breaks if there is more than one join group optimized by the RTO.
     */
    public void test_BSBM_Q4_pc100() throws Exception {
       
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q4", // testURI,
                "rto/BSBM-Q4.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q4.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 9, 6, 7, 8, 10, 11 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q5 on the pc100 data set.
     * 
     * FIXME FAILS if we disallow out of order evaluation when doing cutoff
     * joins.
     */
    public void test_BSBM_Q5_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q5", // testURI,
                "rto/BSBM-Q5.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q5.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final List<int[]> expectedOrders = new LinkedList<int[]>();

        expectedOrders.add(new int[] { 1, 3, 2, 5, 4, 7, 6 });
        expectedOrders.add(new int[] { 1, 3, 5, 4, 2, 7, 6 });
        
        assertSameJoinOrder(expectedOrders, helper);
        
    }

    /**
     * BSBM Q7 on the pc100 data set.
     */
    public void test_BSBM_Q7_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q7", // testURI,
                "rto/BSBM-Q7.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q7.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         * 
         * The join IDs depend on whether the pipelined hash join is enabled
         * by default. Given that the pipelined hash join unites the hash index
         * construction and join in one operator, there is one less operator
         * in the plan, so the indices decrease by one.
         */
        final List<int[]> expectedOrders = new LinkedList<int[]>();
        
        expectedOrders.add( QueryHints.DEFAULT_PIPELINED_HASH_JOIN ?
            new int[] { 7, 6, 8, 4, 5, 9, 10 } :
            new int[] { 8, 7, 9, 5, 6, 10, 11 }
                );
        
        expectedOrders.add(new int[] { 8, 7, 5, 9, 6, 10, 11 });
        
        assertSameJoinOrder(expectedOrders, helper);

    }
    
    /**
     * A modified version BSBM Q7 on pc100 which is the simplest form of the
     * query that causes the RTO to fail with the "No stats" assertion error.
     * This is basically just an OPTIONAL {} join group.
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX rev: <http://purl.org/stuff/rev#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * 
     * SELECT (COUNT(*) as ?count)
     * WHERE { 
     * 
     *   # Control all RTO parameters for repeatable behavior.
     *   hint:Query hint:RTO-sampleType "DENSE".
     *   hint:Query hint:RTO-limit "100".
     *   hint:Query hint:RTO-nedges "1".
     * 
     *    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product7> rdfs:label ?productLabel .
     *     OPTIONAL {
     *    
     *    # Enable the RTO inside of the OPTIONAL join group.
     *    hint:Group hint:optimizer "Runtime".
     *    
     *    ?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product7> .
     *    ?review rev:reviewer ?reviewer .
     *    ?reviewer foaf:name ?revName .
     *    ?review dc:title ?revTitle .
     *    
     *    }
     * }
     * </pre>
     */
    public void test_BSBM_Q7b_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q7b", // testURI,
                "rto/BSBM-Q7b.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q7b.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         * 
         * The join IDs depend on whether the pipelined hash join is enabled
         * by default. Given that the pipelined hash join unites the hash index
         * construction and join in one operator, there is one less operator
         * in the plan, so the indices decrease by one.
         */
        final int[] expected = QueryHints.DEFAULT_PIPELINED_HASH_JOIN ?
            new int[] { 4, 5, 6, 7 } :
            new int[] { 5, 6, 7, 8 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q8 on the pc100 data set.
     */
    public void test_BSBM_Q8_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q8", // testURI,
                "rto/BSBM-Q8.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q8.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 1, 3, 2, 4, 5, 6 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * BSBM Q10 on pc100.
     */
    public void test_BSBM_Q10_pc100() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/BSBM-Q10", // testURI,
                "rto/BSBM-Q10.rq",// queryFileURL
                "src/test/resources/data/bsbm/dataset_pc100.nt",// dataFileURL
                "rto/BSBM-Q10.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 1, 7, 5, 2, 3, 4, 6 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /*
     * larger runs -- OOM on laptop when running under Eclipse.
     */
    
//    /**
//     * BSBM Q1 against pc1000 (OOM on laptop).
//     */
//    public void _test_BSBM_Q1_pc1000() throws Exception {
//
//        final TestHelper helper = new TestHelper(//
//                "rto/BSBM-Q1", // testURI,
//                "rto/BSBM-Q1.rq",// queryFileURL
//                "src/test/resources/data/bsbm/dataset_pc1000.nt.gz",// dataFileURL
//                "rto/BSBM-Q1.srx"// resultFileURL
//        );
//        
//        /*
//         * Verify that the runtime optimizer produced the expected join path.
//         */
//
//        final int[] expected = new int[] { 3, 2, 4, 1, 5 };
//
//        assertSameJoinOrder(expected, helper);
//        
//    }    
    
}
