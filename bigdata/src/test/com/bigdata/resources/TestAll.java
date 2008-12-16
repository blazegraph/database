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

package com.bigdata.resources;


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

        TestSuite suite = new TestSuite("resources");

        /*
         * Test management of local resources.
         * 
         * @todo convert to a proxy test suite per the examples above so that we
         * can run any interesting conditions and gather the resource manager
         * unit tests together into that proxy test suite and its bootstrap
         * tests.
         * 
         * @todo write tests for deleting old resources when coordinating read
         * locks with a transaction manager.
         * 
         * @todo add tests for access to read-committed and fully isolated
         * indices.
         * 
         * Note: More extensive tests are performed in the services package
         * where we have access to the IMetadataService as well so that we can
         * validate the results of an index split, etc.
         */
        // bootstrap tests of the resource manager
        suite.addTestSuite(TestResourceManagerBootstrap.class);
        // test overflow handling.
        suite.addTestSuite(TestOverflow.class);
        // test of index segment build and full compacting merge tasks.
        suite.addTestSuite(TestBuildTask.class);
        // test merge/split of index partitions.
        suite.addTestSuite(TestSplitTask.class);

        // test release of old resources.
        suite.addTestSuite(TestAddDeleteResource.class);
//        suite.addTest(TestReleaseResources.suite());
//        suite.addTestSuite(TestReleaseResources.TestReleaseFree.class);
        suite.addTestSuite(TestReleaseResources.TestWithCopyNoRelease.class);
        suite.addTestSuite(TestReleaseResources.TestWithCopyImmediateRelease.class);
        suite.addTestSuite(TestReleaseResources.TestWithCopy_NonZeroMinReleaseAge.class);

        return suite;

    }

}
