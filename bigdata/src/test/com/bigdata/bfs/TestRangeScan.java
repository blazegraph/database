/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 15, 2008
 */

package com.bigdata.bfs;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.sparse.SparseRowStore;

/**
 * Test suite for {@link BigdataFileSystem#getDocumentHeaders(String, String)}.
 * <p>
 * Note: The correct behavior of this method depends on the
 * {@link SparseRowStore}'s logical row iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRangeScan extends AbstractRepositoryTestCase {

    /**
     * 
     */
    public TestRangeScan() {
    }

    /**
     * @param arg0
     */
    public TestRangeScan(String arg0) {
        super(arg0);
    }

    public void test_rangeScan() {
        
        fail("write test");
        
    }
    
}
