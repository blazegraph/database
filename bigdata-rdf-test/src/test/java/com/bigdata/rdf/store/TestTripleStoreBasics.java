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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestSuite;

/**
 * Aggregates test that are run for each {@link ITripleStore} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreBasics extends TestCase2 {

    /**
     * Aggregates the test suites into something approximating increasing
     * dependency. This is designed to run as a <em>proxy test suite</em> in
     * which all tests are run using a common configuration and a delegation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite() {

        final TestSuite suite = new TestSuite("Triple store basics");

        /*
         * Bootstrap test suites.
         */
        
        // make sure that the db can find the relations and they their container
        suite.addTestSuite(TestRelationLocator.class);

        // test suite for the LexiconRelation.
        suite.addTest(com.bigdata.rdf.lexicon.TestAll.suite());

        // test suite for the SPORelation.
        suite.addTest(com.bigdata.rdf.spo.TestAll.suite());

        /*
         * Tests at the RDF Statement level, requiring use of both the
         * LexiconRelation and the SPORelation.
         */
        
        // test adding terms and statements.
        suite.addTestSuite(TestTripleStore.class);

        // test the ISPO#isModified() API (low-level API).
        suite.addTestSuite(TestIsModified.class);

        // test adding terms and statements is restart safe.
        suite.addTestSuite(TestRestartSafe.class);

        // a stress test based on an issue observed for centos.
        suite.addTestSuite(StressTestCentos.class);
        
        // somewhat dated test of sustained insert rate on synthetic data.
        suite.addTestSuite(TestInsertRate.class);

        // test of the statement identifier semantics.
        suite.addTestSuite(TestStatementIdentifiers.class);

        // test suite for bulk filter of statements absent/present in the kb.
        suite.addTestSuite(TestBulkFilter.class);

        // test suite for temp stores sharing the same lexicon.
        suite.addTestSuite(TestSharedLexiconTempStore.class);
        
        /*
         * test suite for the rio parser and data loading integration, including
         * support for statement identifiers and handling of blank nodes when
         * statement identifiers are NOT enabled.
         */
        suite.addTest(com.bigdata.rdf.rio.TestAll.suite());

        // the DataLoader utility.
        suite.addTestSuite(TestDataLoader.class);
        
		/**
		 * Test suite for configuration of the BLOBS index support.
		 * 
		 * @see <a href="https://github.com/SYSTAP/bigdata-gpu/issues/25">
		 *      Disable BLOBS indexing completely for GPU </a>
		 */ 
        suite.addTestSuite(TestBlobsConfiguration.class);
        
//        // magic sets support (still under development).
//        suite.addTest(com.bigdata.rdf.magic.TestAll.suite());

        // integration test suite for inline URIs.
        // See BLZG-1507 (Implement support for DTE extension types for URIs)
        suite.addTestSuite(com.bigdata.rdf.store.TestInlineURIs.class);

        return suite;

    }

}
