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
 * Data driven test suite for the Runtime Query Optimizer (RTO) using LUBM data
 * and queries based on LUBM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6440 2012-08-14 17:57:33Z thompsonbry $
 */
public class TestRTO_LUBM extends AbstractRTOTestCase {

//    private final static Logger log = Logger.getLogger(TestRTO_LUBM.class);
    
    /**
     * 
     */
    public TestRTO_LUBM() {
    }

    /**
     * @param name
     */
    public TestRTO_LUBM(String name) {
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
     * LUBM Q2 on the U1 data set.
     * <p>
     * Note: There are no solutions for this query against U1. The optimizer is
     * only providing the fastest path to prove that. In practice, this amounts
     * to proving that one of the joins fails to produce any results.
     * <p>
     * Note: For U50 there are 130 solutions and it discovers the join order
     * <code>[3, 5, 6, 4, 1, 2]</code>.
     */
    public void test_LUBM_Q2() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/LUBM-Q2", // testURI,
                "rto/LUBM-Q2.rq",// queryFileURL
                "src/test/resources/data/lehigh/LUBM-U1.rdf.gz",// dataFileURL
                "rto/LUBM-Q2.srx"// resultFileURL
        );

        /*
         * Verify the expected join ordering.
         */
        final int[] expected = new int[] { 5, 6, 1, 2, 3, 4 };

        assertSameJoinOrder(expected, helper);

    }

    /**
     * LUBM Q8 on the U1 data set.
     * <p>
     * Note: For U50 there are 6463 solutions and it discovers the join order:
     * <code>[4,1,3,2,5]</code>.
     */
    public void test_LUBM_Q8() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/LUBM-Q8", // testURI,
                "rto/LUBM-Q8.rq",// queryFileURL
                "src/test/resources/data/lehigh/LUBM-U1.rdf.gz",// dataFileURL
                "rto/LUBM-Q8.srx"// resultFileURL
        );

        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 1, 4, 3, 2, 5 };

        assertSameJoinOrder(expected, helper);

    }
    
    /**
     * LUBM Q9 on the U1 data set.
     * <p>
     * Note: For U50 there are 8627 solutions and it discovers the join order:
     * <code>[2,5,6,4,1,3]</code>.
     * <p>
     * Note: When random sampling is used, several join plans are possible
     * including:
     * 
     * <pre>
     * {4, 2, 3, 5, 0, 1}
     * {3, 0, 5, 4, 1, 2}
     * {1, 4, 3, 5, 0, 2} - this solution has the best performance on U50.
     * </pre>
     * 
     * These all appear to be good join plans even though the last one is is
     * definitely better.
     */
    public void test_LUBM_Q9() throws Exception {
        
        final TestHelper helper = new TestHelper(//
                "rto/LUBM-Q9", // testURI,
                "rto/LUBM-Q9.rq",// queryFileURL
                "src/test/resources/data/lehigh/LUBM-U1.rdf.gz",// dataFileURL
                "rto/LUBM-Q9.srx"// resultFileURL
        );
        
        /*
         * Verify that the runtime optimizer produced the expected join path.
         */

        final int[] expected = new int[] { 5, 3, 4, 6, 1, 2 };

        assertSameJoinOrder(expected, helper);
        
    }

}
