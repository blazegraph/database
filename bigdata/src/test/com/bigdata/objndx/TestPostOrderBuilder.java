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
 * Created on Dec 14, 2006
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;

import junit.framework.AssertionFailedError;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.objndx.IndexSegment.FileStore;


/**
 * Test suite for efficient post-order rebuild of an index in an external index
 * segment.
 * 
 * @todo try building large indices, exporting them into index segments, and
 *       then verifying that the index segments have the correct data.
 * 
 * @todo The notion of merging multiple index segments requires a notion of
 *       which index segments are more recent or alternatively which values are
 *       more recent so that we can reconcile values for the same key. this is
 *       linked to how we will handle transactional isolation.
 * 
 * @todo Handle "delete" markers. For full transactional isolation we need to
 *       keep delete markers around until there are no more live transactions
 *       that could read the index entry. This suggests that we probably want to
 *       use the transaction timestamp rather than a version counter. Consider
 *       that a read by tx1 needs to check the index on the journal and then
 *       each index segment in turn in reverse historical order until an entry
 *       (potentially a delete marker) is found that is equal to or less than
 *       the timestamp of the committed state from which tx1 was born. This
 *       means that an index miss must test the journal and all live segments
 *       for that index (hence the use of bloom filters to filter out index
 *       misses). It also suggests that we should keep the timestamp as part of
 *       the key, except in the ground state index on the journal where the
 *       timestamp is the timestamp of the last commit of the journal. This
 *       probably will also address VLR TX that would span a freeze of the
 *       journal. We expunge the isolated index into a segment and do a merge
 *       when the transaction finally commits. We wind up doing the same
 *       validation and merge steps as when the isolation occurs within a more
 *       limited buffer, but in more of a batch fashion. This might work nicely
 *       if we buffer the isolatation index out to a certain size in memory and
 *       then start to spill it onto the journal. If fact, the hard reference
 *       queue already does this so we can just test to see if (a) anything has
 *       been written out from the isolation index; and (b) whether or not the
 *       journal was frozen since the isolation index was created.
 * 
 * Should the merge down should impose the transaction commit timestamp on the
 * items in the index?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPostOrderBuilder extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestPostOrderBuilder() {
    }

    /**
     * @param name
     */
    public TestPostOrderBuilder(String name) {
        super(name);
    }

    public void test_minimumHeight() {
        
        assertEquals( 0, PostOrderBuilder.getMinimumHeight(3, 1));
        assertEquals( 1, PostOrderBuilder.getMinimumHeight(3, 2));
        assertEquals( 1, PostOrderBuilder.getMinimumHeight(3, 3));
        assertEquals( 2, PostOrderBuilder.getMinimumHeight(3, 4));
        assertEquals( 2, PostOrderBuilder.getMinimumHeight(3, 5));
        assertEquals( 2, PostOrderBuilder.getMinimumHeight(3, 6));
        assertEquals( 2, PostOrderBuilder.getMinimumHeight(3, 7));
        assertEquals( 2, PostOrderBuilder.getMinimumHeight(3, 8));
        assertEquals( 2, PostOrderBuilder.getMinimumHeight(3, 9));
        assertEquals( 3, PostOrderBuilder.getMinimumHeight(3, 10));
        
    }

    /**
     * Test suite based on a small btree with known keys and values.
     * 
     * @see src/architecture/btree.xls, which has the detailed examples.
     *  
     * @todo test it all again with other interesting output branching
     * factors.
     * 
     * @todo write another test that builds a tree of at least height 3
     * (four levels).
     * 
     * @todo write a stress test that builds a tree with at least 1M
     * entries, exports it as an index segment, and then validates the index
     * segment.
     */
    public static class TestSmallTree extends AbstractBTreeTestCase {

        BTree btree;
        File outFile;

        public TestSmallTree() {}
        
        public TestSmallTree(String name) {super(name);}
        
        /**
         * Sets up the {@link #btree} and ensures that the {@link #outFile} on
         * which the {@link IndexSegment} will be written does not exist.
         */
        public void setUp() {

            btree = getBTree(3);

            for (int i = 1; i <= 10; i++) {

                btree.insert(i, new SimpleEntry(i));

            }

            outFile = new File(getName() + ".seg");

            if (outFile.exists() && !outFile.delete()) {

                throw new RuntimeException("Could not delete file: " + outFile);

            }

        }

        public void tearDown() {

            if (outFile != null && outFile.exists() && !outFile.delete()) {

                System.err
                        .println("Warning: Could not delete file: " + outFile);

            }

        }
        
        /**
         * Test ability to build an index segment from a {@link BTree}.
         * 
         * @todo verify interals in builder.
         * @todo verify post-conditions for files.
         */
        public void test_buildOrder3() throws IOException {
            
            new PostOrderBuilder(outFile,null,btree,3);

             /*
              * Verify can load the index file and that the metadata
              * associated with the index file is correct (we are only
              * checking those aspects that are easily defined by the test
              * case and not, for example, those aspects that depend on the
              * specifics of the length of serialized nodes or leaves).
              */
            final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                    // setup reference queue to hold all leaves and nodes.
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            7, 7),
                    // take the other parameters from the btree.
                    btree.NEGINF, btree.comparator,
                    btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer);
            assertEquals(3,seg.getBranchingFactor());
            assertEquals(2,seg.getHeight());
            assertEquals(ArrayType.INT,seg.getKeyType());
            assertEquals(4,seg.getLeafCount());
            assertEquals(3,seg.getNodeCount());
            assertEquals(10,seg.size());
            
            /*
             * @todo verify the right keys in the right leaves.
             */
            
            /*
             * Verify the total index order.
             */
            assertSameBTree(btree, seg);
            
        }
        
        public void test_buildOrder10() throws IOException {
            
            new PostOrderBuilder(outFile,null,btree,10);

             /*
              * Verify can load the index file and that the metadata
              * associated with the index file is correct (we are only
              * checking those aspects that are easily defined by the test
              * case and not, for example, those aspects that depend on the
              * specifics of the length of serialized nodes or leaves).
              */
            final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                    // setup reference queue to hold all leaves and nodes.
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            1, 1),
                    // take the other parameters from the btree.
                    btree.NEGINF, btree.comparator,
                    btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer
                    );
            assertEquals(10,seg.getBranchingFactor());
            assertEquals(0,seg.getHeight());
            assertEquals(ArrayType.INT,seg.getKeyType());
            assertEquals(1,seg.getLeafCount());
            assertEquals(0,seg.getNodeCount());
            assertEquals(10,seg.size());
            
            /*
             * @todo verify the right keys in the right leaves.
             */
            
            /*
             * Verify the total index order.
             */
            assertSameBTree(btree, seg);
            
        }
        
        /**
         * Assert #of entries, key type, and the same keys and values. The
         * height, branching factor, #of nodes and #of leaves will differ if the
         * {@link IndexSegment} was built with a different branching factor from
         * the original {@link BTree}.
         * 
         * @param expected
         *            The ground truth btree.
         * @param actual
         *            The index segment that is being validated.
         */
        public void assertSameBTree(BTree expected, IndexSegment actual) {

            assert expected != null;
            
            assert actual != null;
            
            // The #of entries must agree.
            assertEquals(expected.size(), actual.size());
            
            // The key type must agree.
            assertEquals(expected.getKeyType(), actual.getKeyType());
            
            KeyValueIterator expectedItr = btree.entryIterator();
            
            KeyValueIterator actualItr = actual.entryIterator();
            
            int index = 0;
            
            while( expectedItr.hasNext() ) {
                
                if( ! actualItr.hasNext() ) {
                    
                    fail("The iterator is not willing to visit enough entries");
                    
                }
                
                Object expectedVal = expectedItr.next();
                
                Object actualVal = actualItr.next();

                Object expectedKey = expectedItr.getKey();
                
                Object actualKey = expectedItr.getKey();

//                System.err.println("index="+index+", key="+actualKey+", val="+actualVal);
                
                try {
                    
                    assertEquals(expectedKey, actualKey);
                    
                } catch (AssertionFailedError ex) {
                    
                    /*
                     * Lazily generate message.
                     */
                    fail("index=" + index, ex);
                    
                }

                try {

                    assertEquals(expectedVal, actualVal);
                    
                } catch (AssertionFailedError ex) {
                    /*
                     * Lazily generate message.
                     */
                    fail("index=" + index + ", key=" + expectedKey, ex);
                    
                }
                
                index++;
                
            }
            
            if( actualItr.hasNext() ) {
                
                fail("The iterator is willing to visit too many entries");
                
            }
            
        }
        
    }
    
}
