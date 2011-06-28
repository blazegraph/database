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

package com.bigdata.btree.isolation;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increasing dependency order.
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
     * Aggregates test suites in increasing dependency order.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("B+Tree Isolation");

        // test isolated fused view (handles delete markers).
        suite.addTestSuite(TestIsolatedFusedView.class);

        // test for mixing full transactions with unisolated operations.
        suite.addTestSuite(TestMixedModeOperations.class);
        
        // test for state-based validation _concept_
        suite.addTestSuite(TestAccount.class);
        
        // tests of write-write conflict resolution.
        suite.addTestSuite(TestConflictResolution.class);

        // @todo write test suite for this and handle remove() (propagate the tuple revision timestamp).
        // suite.addTestSuite(TestIsolatedFusedViewCursors.class);

        return suite;
        
    }
    
}
