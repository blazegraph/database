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
 * Created on May 17, 2007
 */

package com.bigdata.btree;

/**
 * Test suite for the {@link IIndex#getCounter()} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexCounter extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIndexCounter() {
    }

    /**
     * @param name
     */
    public TestIndexCounter(String name) {
        super(name);
    }

    public void test_counter() {
        
        BTree btree = getBTree(3);
        
        ICounter counter = btree.getCounter();
        
        // initial value is zero for an unpartitioned index
        assertEquals(0,counter.get());
        
        // get() does not have a side-effect on the counter.
        assertEquals(0,counter.get());
        
        // inc() increments the value and _then_ returns the counter.
        assertEquals(1,counter.incrementAndGet());
        assertEquals(1,counter.get());
        assertEquals(2,counter.incrementAndGet());

    }
    
}
