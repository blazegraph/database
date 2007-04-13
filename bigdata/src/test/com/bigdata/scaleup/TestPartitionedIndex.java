/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 7, 2007
 */

package com.bigdata.scaleup;

import com.bigdata.btree.AbstractBTreeTestCase;
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
