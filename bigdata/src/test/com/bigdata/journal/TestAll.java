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
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.journal;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Runs all tests for all journal implementations.
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

        final TestSuite suite = new TestSuite("journal");

        // test ability of the platform to synchronize writes to disk.
        suite.addTestSuite( TestRandomAccessFileSynchronousWrites.class );
        
        // test the ability to (de-)serialize the root addreses.
        suite.addTestSuite( TestCommitRecordSerializer.class );
        
        // test the root block api.
        suite.addTestSuite( TestRootBlockView.class );

        // @todo tests of the index used map index names to indices.
        suite.addTestSuite( TestName2Addr.class );

        // tests of the index used to access historical commit records
        suite.addTestSuite( TestCommitRecordIndex.class );
        
        /*
         * Test a scalable temporary store (uses the transient and disk-only
         * buffer modes).
         */
        suite.addTest( TestTemporaryStore.suite() );
        
        /*
         * Test the different journal modes.
         * 
         * -DminimizeUnitTests="true" is used when building the project site to keep
         * down the nightly build demands.
         */
        
        if(Boolean.parseBoolean(System.getProperty("minimizeUnitTests","false"))) {

            suite.addTest( TestTransientJournal.suite() );

            suite.addTest( TestDirectJournal.suite() );

            /*
             * Note: The mapped journal is somewhat problematic and its tests are
             * disabled for the moment since (a) we have to pre-allocate large
             * extends; (b) it does not perform any better than other options; and
             * (c) we can not synchronously unmap or delete a mapped file which
             * makes cleanup of the test suites difficult and winds up spewing 200M
             * files all over your temp directory.
             */
            
//            suite.addTest( TestMappedJournal.suite() );

        }

        suite.addTest( TestDiskJournal.suite() );

        suite.addTest( com.bigdata.rwstore.TestAll.suite() );

        /*
         * FIXME enable this test suite once the journal mode is ready.
         */
//        suite.addTest( TestBufferedDiskJournal.suite() );

        return suite;

    }

}
