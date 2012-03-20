/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval.update;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTest;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTxTest;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME We need an extensive test suite for INSERT DATA, DELETE DATA,
 *          and LOAD DATA which cover the different database modes (triples,
 *          triples+inference, sids, sids+inference, quads) and also explore
 *          plans which batch updates, doing TM across the update set and plans
 *          which do not update TM or which do database at once closure.
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("SPARQL Update Evaluation");

        /*
         * Boot strapped test suite for core UPDATE functionality.
         */

        suite.addTestSuite(TestUpdateBootstrap.class);

        /*
         * Data driven tests.
         * 
         * TODO Openrdf does not have tests for the LOAD operation in this test
         * suite (or add to the NSS test suite). That test suite should cover
         * triples, triples+inference, sids, sids+inference, and quads modes).
         * Also test with and w/o the full text index and subject centric text
         * index (which has its own truth maintenance).
         */
//        suite.addTestSuite(TestUpdate.class);
       
        /*
         * The openrdf SPARQL UPDATE test suite.
         * 
         * Note: This test suite is for quads mode only.
         * 
         * FIXME We also need to run this against a cluster.
         */

        // Unisolated operations.
        suite.addTestSuite(BigdataSPARQLUpdateTest.class);

        // Fully isolated read/write operations.
        suite.addTestSuite(BigdataSPARQLUpdateTxTest.class);

        return suite;
        
    }
    
}
