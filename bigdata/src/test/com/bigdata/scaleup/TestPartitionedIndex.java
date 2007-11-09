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
 * Created on Feb 7, 2007
 */

package com.bigdata.scaleup;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentMerger;
import com.bigdata.isolation.UnisolatedBTree;

/**
 * Test suite for {@link PartitionedIndexView}.
 * 
 * @todo This should be refactored to test when using {@link BTree} as well as
 *       {@link UnisolatedBTree}. The latter is more important for database
 *       operations as it supports isolation and column stores.  Changing over
 *       from {@link BTree} to {@link UnisolatedBTree} highlights many assumptions
 *       in {@link IndexSegmentBuilder}, {@link IndexSegmentMerger}, and the
 *       code that handles {@link MasterJournal#overflow()}.
 * 
 * @todo where possible, it would be nice to leverage the unit tests for the
 *       mutable btree. note that rangeCount does not have the same behavior for
 *       a view since it reports the sum of the counts across the sources rather
 *       than computing the exact #of keys in the range. However, most other
 *       aspects of the view are true to the btree api.
 * 
 * @todo also test restart safety.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPartitionedIndex extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestPartitionedIndex() {
    }

    /**
     * @param name
     */
    public TestPartitionedIndex(String name) {
        super(name);
    }

    public void test_onePartition_noSegments() {
        
        fail("write test");
        
    }
    
    public void test_onePartition_evictSegment() {

        fail("write test");
        
    }
    
    public void test_onePartition_evictSegments() {

        fail("write test");
        
    }
    
    public void test_multiplePartitions_noSegments() {

        fail("write test");
        
    }
    
    public void test_multiplePartitions_evictSegments() {

        fail("write test");
        
    }
    
}
