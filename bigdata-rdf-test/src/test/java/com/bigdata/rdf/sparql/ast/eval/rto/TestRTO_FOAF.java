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

import java.util.Properties;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;

/**
 * Data driven test suite for the Runtime Query Optimizer (RTO) using quads-mode
 * FOAF data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6440 2012-08-14 17:57:33Z thompsonbry $
 * 
 * @deprecated None of these test queries are complicated enough to trigger the
 *             RTO. The class and its queries should just be dropped.
 */
public class TestRTO_FOAF extends AbstractRTOTestCase {

//    private final static Logger log = Logger.getLogger(TestRTO_LUBM.class);
    
    /**
     * 
     */
    public TestRTO_FOAF() {
    }

    /**
     * @param name
     */
    public TestRTO_FOAF(String name) {
        super(name);
    }

    /**
     * Data files for 3-degrees of separation starting with a crawl of TBLs foaf
     * card.
     */
    private static final String[] dataFiles = new String[] { // data files
            "src/test/resources/data/foaf/data-0.nq.gz",//
            "src/test/resources/data/foaf/data-1.nq.gz",//
            "src/test/resources/data/foaf/data-2.nq.gz",//
    };//
    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(BigdataSail.Options.QUADS_MODE, "true");

        properties.setProperty(BigdataSail.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        return properties;
        
    }

    /**
     * Find all friends of a friend.
     * 
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT ?x ?z (count(?y) as ?connectionCount) 
     *     (sample(?xname2) as ?xname) 
     *     (sample(?zname2) as ?zname)
     *  WHERE {
     *     ?x foaf:knows ?y .
     *     ?y foaf:knows ?z .
     *     FILTER NOT EXISTS { ?x foaf:knows ?z } . 
     *     FILTER ( !sameTerm(?x,?z)) . 
     *     OPTIONAL { ?x rdfs:label ?xname2 } .
     *     OPTIONAL { ?z rdfs:label ?zname2 } .
     *     }
     * GROUP BY ?x ?z
     * </pre>
     * 
     * FIXME This example is not complex enough to run through the RTO. This may
     * change when we begin to handle OPTIONALs. However, the FILTER NOT EXISTS
     * would also need to be handled to make this work since otherwise the query
     * remain 2 required SPs with a simple FILTER, a sub-SELECTs (for the FILTER
     * NOT EXISTS) and then two simple OPTIONALs.
     */
    public void test_FOAF_Q1() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/FOAF-Q1", // testURI,
                "rto/FOAF-Q1.rq",// queryFileURL
                dataFiles,//
                "rto/FOAF-Q1.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 2, 4, 1, 3, 5 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * Find all friends of a friend having at least N indirect connections.
     * 
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT ?x ?z (count(?y) as ?connectionCount) 
     *     (sample(?xname2) as ?xname) 
     *     (sample(?zname2) as ?zname)
     *  WHERE {
     *     ?x foaf:knows ?y .
     *     ?y foaf:knows ?z .
     *     FILTER NOT EXISTS { ?x foaf:knows ?z } . 
     *     FILTER ( !sameTerm(?x,?z)) . 
     *     OPTIONAL { ?x rdfs:label ?xname2 } .
     *     OPTIONAL { ?z rdfs:label ?zname2 } .
     *     }
     * GROUP BY ?x ?z
     * HAVING (?connectionCount > 1)
     * </pre>
     * 
     * FIXME This example is not complex enough to run through the RTO. This may
     * change when we begin to handle OPTIONALs. However, the FILTER NOT EXISTS
     * would also need to be handled to make this work since otherwise the query
     * remain 2 required SPs with a simple FILTER, a sub-SELECTs (for the FILTER
     * NOT EXISTS) and then two simple OPTIONALs.
     */
    public void test_FOAF_Q2() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/FOAF-Q2", // testURI,
                "rto/FOAF-Q2.rq",// queryFileURL
                dataFiles,//
                "rto/FOAF-Q2.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 2, 4, 1, 3, 5 };

        assertSameJoinOrder(expected, helper);
        
    }
    
    /**
     * Find all direct friends and extract their names (when available).
     * 
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * CONSTRUCT {
     *   ?u a foaf:Person .
     *   ?u foaf:knows ?v .
     *   ?u rdfs:label ?name .
     *   }
     * WHERE {
     * 
     *   # Control all RTO parameters for repeatable behavior.
     *   hint:Query hint:optimizer "Runtime".
     *   hint:Query hint:RTO-sampleType "DENSE".
     *   hint:Query hint:RTO-limit "100".
     *   hint:Query hint:RTO-nedges "1".
     * 
     *   ?u a foaf:Person .
     *   ?u foaf:knows ?v . 
     *   OPTIONAL { ?u rdfs:label ?name } . 
     * } 
     * LIMIT 100
     * </pre>
     * 
     * FIXME This example is not complex enough to run through the RTO. This
     * might change when we handle the OPTIONAL join inside of the RTO, however
     * it would remain 2 required JOINS and an OPTIONAL join and there is no
     * reason to run that query through the RTO. The query plan will always be
     * the most selective vertex, then the other vertex, then the OPTIONAL JOIN.
     * This is fully deterministic based on inspection on the query and the
     * range counts. The RTO is not required.
     */
    public void test_FOAF_Q10() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/FOAF-Q10", // testURI,
                "rto/FOAF-Q10.rq",// queryFileURL
                dataFiles,//
                "rto/FOAF-Q10.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 2, 4, 1, 3, 5 };

        assertSameJoinOrder(expected, helper);
        
    }

}
