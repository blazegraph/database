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
package com.bigdata.rdf.sail.tck;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTest;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTest2;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTest2DiskRW;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTest2DiskWORM;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTxTest;
import com.bigdata.rdf.sail.tck.BigdataSPARQLUpdateTxTest2;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
         * The openrdf SPARQL UPDATE test suite.
         * 
         * Note: This test suite is for quads mode only. SPARQL UPDATE support
         * is also tested by the NSS test suite.
         */

        // Unisolated operations.
        suite.addTestSuite(BigdataSPARQLUpdateTest.class);

        // Fully isolated read/write operations.
        suite.addTestSuite(BigdataSPARQLUpdateTxTest.class);

        /**
         * The bigdata extensions to SPARQL UPDATE to support solution sets as
         * well as graphs.
         * 
         * Note: We need to run a few different IRawStore backends to confirm
         * support for the IStreamStore interface and to confirm that the store
         * correctly supports SPARQL UPDATE on NAMED SOLUTION SETS using that
         * IStreamStore interface.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531">
         *      SPARQL UPDATE Extensions (Trac) </a>
         * @see <a
         *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
         *      SPARQL Update Extensions (Wiki) </a>
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/555" >
         *      Support PSOutputStream/InputStream at IRawStore </a>
         */
        {

            // Unisolated operations
            suite.addTestSuite(BigdataSPARQLUpdateTest2.class); // MemStore.
            suite.addTestSuite(BigdataSPARQLUpdateTest2DiskRW.class);
            suite.addTestSuite(BigdataSPARQLUpdateTest2DiskWORM.class);

            // Fully isolated read/write operations.
            suite.addTestSuite(BigdataSPARQLUpdateTxTest2.class); // MemStore

        }

        return suite;

    }

}
