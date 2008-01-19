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
 * Created on Feb 4, 2007
 */

package com.bigdata.repo;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegment;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increase dependency order.
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
     * Aggregates the tests in increasing dependency order.
     * 
     * @todo add test suite covering (a) CRUD operations; (b) atomic append; (c)
     *       range scan and range delete operations; (d) performance tests under
     *       simulated load; and (e) overflow handling of the metadata and data
     *       indices, especially with an eye to the index split points and if we
     *       optimize the chunk storage for the {@link BTree} to use raw records
     *       on the journal (thereby requiring an extension of the overflow
     *       semantics to get the data inline on the {@link IndexSegment}).
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("Bigdata Scale-Out Repository");
 
        // test atomic append operations on the file data index.
        suite.addTestSuite( TestAtomicAppend.class );

        /*
         * test various operations on the file metadata index, including those
         * that write on the data index.
         */
        suite.addTestSuite( TestMetadataIndex.class );
 
        // @todo test document metadata range scan and range delete.
        
        // @todo test full text indexing and search.
//        suite.addTestSuite( TestSearch.class );
        
        return suite;
        
    }

}
