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
 * Created on Apr 21, 2006
 */
package com.bigdata.cache;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates unit tests into dependency order.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    public static junit.framework.Test suite() {
        
        TestSuite retval = new TestSuite("cache");
        
        retval.addTestSuite(TestHardReferenceCache.class);
        
//        // Test all ICacheEntry implementations.
//        retval.addTestSuite( TestCacheEntry.class );

        // Test LRU semantics.
        retval.addTestSuite( TestLRUCache.class );
        
        // Test cache semantics with weak/soft reference values.
        retval.addTestSuite( TestWeakValueCache.class );
        
//        // Generic test of cache policy.
//        retval.addTestSuite( TestCachePolicy.class );

        return retval;
    }

}
