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

package com.bigdata.isolation;

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

        TestSuite suite = new TestSuite("isolation");

        // test for state-based validation _concept_
        suite.addTestSuite(TestAccount.class);
        
        // test version counter and delete marker implementation class.
        suite.addTestSuite(TestValue.class);

        // test serialization for Value class.
        suite.addTestSuite(TestValueSerializer.class);

        // @todo test various compression schemes for the Value[]s here.
        
        // @todo test btree with IValue values.
        suite.addTestSuite(TestUnisolatedBTree.class);
        
        // test fully isolated btree for use with transactions.
        suite.addTestSuite(TestIsolatedBTree.class);

        // @todo test conflict resolution, including on the Account problem and
        // on the TripleStore.

        // @todo test isolatable fused view (handles delete markers).
//        suite.addTestSuite(TestIsolatableFusedView.class);

        /*
         * @todo test index segment builder (must indicate whether full
         * compacting merge or not), or maybe that is use in the metadataindex
         */
        /*
         * test index merge code (merge rule must recognize and handle
         * deletion markers).
         */
//        suite.addTestSuite(TestIndexSegmentMerger.class); 
        
        // @todo test isolatable partitioned index.
//        suite.addTestSuite(TestIsolatablePartitionedIndex.class);

        return suite;
        
    }
    
}
