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
 * Created on Jan 5, 2007
 */

package com.bigdata.btree;

import java.io.IOException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILeafData;
import com.bigdata.btree.IndexSegmentMerger;
import com.bigdata.btree.IndexSegmentMerger.MergedLeafIterator;

/**
 * Test suite for compacting merge of B+-Trees.
 * 
 * @todo write tests of a key range merge (feature not supported yet, but
 *       required for partitioned index segment builds).
 * 
 * @todo test N-way merge.
 * 
 * @todo write tests where the keys do not overlap (done).
 * 
 * @todo write tests where the keys overlap but there are no conflicts.
 * 
 * @todo write tests: merging a tree with itself, merging trees w/o deletion
 *       markers, merging trees w/ deletion markers, merging trees w/ age-based
 *       version expiration, merging trees with count-based version expiration.
 * 
 * @todo write tests where there are keys that conflict. conflicts need to be
 *       resolved according to some policy, and there are a variety of policies
 *       that could make sense for different applications, e.g., treat one of
 *       the btrees as more recent so that it overwrites the entry for the same
 *       key in the other btree or track timestamps and retain either the N most
 *       recent or all entries of more more than a given age. yet another
 *       approach is to resolve the conflict using some state-based mechanism
 *       and "merge" the values. conflict of necessity interacts with isolation
 *       since what we are looking at are write-write conflicts.
 * 
 * @todo another twist on the concept of a conflict is a "deleted" entry. this
 *       interacts a bit with the isolation mechanism, just like the resolution
 *       of conflicts interactions with isolation (delete is just another kind
 *       of write, so this is still a write-write conflict).
 * 
 * @todo write tests where the merged data exceed the target size constraints
 *       for the index partition and work through a split of the parition. once
 *       again, this interacts with transactional isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see TestMetadataIndex, which also tests {@link IndexSegmentMerger} in the
 *      context of validating index partition management.
 */
public class TestIndexSegmentMerger extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIndexSegmentMerger() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentMerger(String name) {
        super(name);
    }
    
    /**
     * Test builds two btrees, populates them with disjoint key ranges (1-10
     * and 11-20), and then merges their entries.
     * 
     * @throws IOException
     */
    public void test_merge01() throws IOException {
        
        BTree btree1 = getBTree(3);

        BTree btree2 = getBTree(3);

        for (int i = 1; i <= 10; i++) {

            btree1.insert(i, new SimpleEntry(i));

        }

        for (int i = 11; i <= 20; i++) {

            btree2.insert(i, new SimpleEntry(i));

        }
        
        // branching factor used by leaves emitted by the merge process.
        final int m = 3;
        
        IndexSegmentMerger merger = new IndexSegmentMerger(m, btree1, btree2);

        MergedLeafIterator itr = merger.merge();
        
        assertEquals("nentries",20,merger.nentries);

        int entryIndex = 0;
        
        while(itr.hasNext()){
            
            ILeafData leaf = itr.next();
            
//            System.err.println("leaf: nkeys="+leaf.getEntryCount());
            
            for( int i=0; i<leaf.getEntryCount(); i++ ) {
        
                byte[] key = leaf.getKeys().getKey(i);
                
                Object val = (SimpleEntry)leaf.getValues()[i];
                
                assertEquals("key",keyBuilder.reset().append(entryIndex+1).getKey(),key);
                
                assertEquals("val",new SimpleEntry(entryIndex+1),val);
                
                entryIndex++;
                
            }
            
        }

        assertEquals(20,entryIndex);
        
        /*
         * Verify that the tmpStore was closed. The backing file (if any) is
         * deleted when the tmpStore is closed.
         */
        assertFalse(itr.tmpStore.isOpen());

    }

    /**
     * test builds two btrees with overlapping but disjoint keys (one has the
     * odd numbers beween 1 and 20, inclusive, while the other has the even
     * numbers between 1 and 20, inclusive).  The trees are merged and the
     * merged entries are validated.
     */
    public void test_merge02() throws IOException {
        
        BTree btree1 = getBTree(3);

        BTree btree2 = getBTree(3);

        for (int i = 1; i <= 20; i++) {

            if((i&1)==1) {
                
                btree1.insert(i, new SimpleEntry(i));
                
            } else {
                
                btree2.insert(i, new SimpleEntry(i));
                
            }

        }

        assertNotNull(btree1.lookup(1)); // odd keys
        assertNotNull(btree2.lookup(2)); // even keys.

        assertEquals("btree1.nentries", 10, btree1.getEntryCount());
        assertEquals("btree2.nentries", 10, btree2.getEntryCount());
        
        // branching factor used by leaves emitted by the merge process.
        final int m = 3;
        
        IndexSegmentMerger merger = new IndexSegmentMerger(m, btree1, btree2);

        MergedLeafIterator itr = merger.merge();
        
        assertEquals("nentries",20,merger.nentries);

        int entryIndex = 0;
        
        while(itr.hasNext()){
            
            ILeafData leaf = itr.next();
            
//            System.err.println("leaf: nkeys="+leaf.getEntryCount());
            
            for( int i=0; i<leaf.getEntryCount(); i++ ) {
        
                byte[] key = leaf.getKeys().getKey(i);
                
                Object val = (SimpleEntry)leaf.getValues()[i];
                
                assertEquals("key",keyBuilder.reset().append(entryIndex+1).getKey(),key);
                
                assertEquals("val",new SimpleEntry(entryIndex+1),val);
                
                entryIndex++;
                
            }
            
        }

        assertEquals(20,entryIndex);
        
        /*
         * Verify that the tmpStore was closed. The backing file (if any) is
         * deleted when the tmpStore is closed.
         */
        assertFalse(itr.tmpStore.isOpen());
        
    }
    
}
