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
 * Created on Dec 7, 2006
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;

import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.objndx.IndexSegmentMerger.MergedLeafIterator;
import com.bigdata.objndx.IndexSegmentMerger.MergedEntryIterator;
import com.bigdata.objndx.MetadataIndex.PartitionMetadata;
import com.bigdata.objndx.MetadataIndex.SegmentMetadata;
import com.bigdata.objndx.MetadataIndex.StateEnum;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore2;

/**
 * A test suite for managing a partitioned index.
 * 
 * @todo debug repeated merges with one partition.
 * 
 * @todo write tests for repeated merges with multiple partitions.
 * 
 * @todo refactor the control logic until we can use it to support partitioned
 *       indices transparently.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMetadataIndex extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestMetadataIndex() {
    }

    /**
     * @param name
     */
    public TestMetadataIndex(String name) {
        
        super(name);
        
    }

    /**
     * Test the ability to create and update the metadata for a partition.
     */
    public void test_crud() {
        
        // setup a store to hold the metadata index.
        IRawStore store = new SimpleMemoryRawStore2();
        
        // create the metadata index.
        MetadataIndex md = new MetadataIndex(store,3);

        /*
         * initially there are no entries in the metadata index. if this is a
         * partitioned but not a distributed index then we can presume that the
         * application knows the location of the journal on which writes are
         * absorbed for the partitioned index.  in this case we only store the
         * file names of the evicted index segments in the metadata index rather
         * than, e.g., locators for the journal and index segments for a given
         * partition.
         */
        
        /*
         * the key for the first partition. this will accept any key iff there
         * is only one partition, but you can create paritions with keys that
         * order greater than an empty byte[] and they will be choosen
         * correctly once they exist.
         */
        final byte[] key0 = new byte[] {};

        final int partId0 = 0;
        
        PartitionMetadata part1 = new PartitionMetadata(partId0);

        assertEquals(null,md.put(key0, part1));

        assertEquals(part1,md.get(key0));
        
        PartitionMetadata part2 = new PartitionMetadata(partId0,
                new SegmentMetadata[] { new SegmentMetadata("a", 10L,StateEnum.LIVE) });

        assertEquals(part1,md.put(key0, part2));

        assertEquals(part2,md.get(key0));
        
        PartitionMetadata part3 = new PartitionMetadata(partId0,
                new SegmentMetadata[] { new SegmentMetadata("a", 10L,StateEnum.LIVE),
                        new SegmentMetadata("b", 20L,StateEnum.LIVE) });

        assertEquals(part2,md.put(key0, part3));

        assertEquals(part3,md.get(key0));

    }
    
    /**
     * Verifies (de-)serialization and re-start safety for a metadata index with
     * three partitions.
     * 
     * @throws IOException
     */
    public void test_restartSafe() throws IOException {
        
        Properties properties = new Properties();
        
        File file = new File(getName()+".jnl");

        if(file.exists() && !file.delete() ) {
        
            fail("Could not delete file: "+file.getAbsoluteFile());
            
        }
        
        properties.setProperty(Options.FILE,file.toString());

        properties.setProperty(Options.SEGMENT,"0");

        // setup a store to hold the metadata index.
        Journal store = new Journal(properties);
        
        // create the metadata index.
        MetadataIndex md = new MetadataIndex(store,3);

        /*
         * initially there are no entries in the metadata index. if this is a
         * partitioned but not a distributed index then we can presume that the
         * application knows the location of the journal on which writes are
         * absorbed for the partitioned index.  in this case we only store the
         * file names of the evicted index segments in the metadata index rather
         * than, e.g., locators for the journal and index segments for a given
         * partition.
         */
        
        /*
         * the separator key for the first partition (index := 0). this will
         * accept any key iff there is only one partition, but you can create
         * paritions with keys that order greater than an empty byte[] and they
         * will be choosen correctly once they exist.
         */
        final byte[] key0 = new byte[] {};
        /*
         * separator key for the 2nd partition (index := 1).
         */
        final byte[] key1 = new byte[] {1,2,3};
        /*
         * separator key for the 3nd partition (index := 2).
         */
        final byte[] key2 = new byte[] {4,5,6};

        /*
         * create three partitions.
         */
        final int partId0 = 0;
        PartitionMetadata part0 = new PartitionMetadata(partId0);
        assertEquals(null,md.put(key0, part0));
        assertEquals(part0,md.get(key0));
        
        final int partId1 = 1;
        PartitionMetadata part1 = new PartitionMetadata(partId1,
                new SegmentMetadata[] { new SegmentMetadata("a", 10L,StateEnum.LIVE) });
        assertEquals(null,md.put(key1, part1));
        assertEquals(part1,md.get(key1));
        
        final int partId2 = 2;
        PartitionMetadata part2 = new PartitionMetadata(partId2,
                new SegmentMetadata[] { new SegmentMetadata("a", 10L,StateEnum.LIVE),
                        new SegmentMetadata("b", 20L,StateEnum.LIVE) });
        assertEquals(null, md.put(key2, part2));
        assertEquals(part2, md.get(key2));

        assertEquals("#entries",3,md.getEntryCount());

        // restart address.
        long addr = md.write();
        
        // commit store.
        store.commit();
        
        // close store.
        store.close();
        
        // re-open the store.
        store = new Journal(properties);
        
        // re-load the index.
        md = new MetadataIndex(store,addr);
        
        assertEquals("#entries",3,md.getEntryCount());

        assertEquals(part0,md.get(key0));
        assertEquals(part1,md.get(key1));
        assertEquals(part2,md.get(key2));

    }
    
    /**
     * The leaf search rule for the index partitions is the first entry having a
     * key GTE the search key. This search rule lets us locate the corresponding
     * partition much as the node search rule lets us direct search to correct
     * node or leaf in the the next level down of the btree.
     * 
     * @see MetadataIndex#find(byte[])
     */
    public void test_leafSearchRule() {

        // setup a store to hold the metadata index.
        IRawStore store = new SimpleMemoryRawStore2();
        
        // create the metadata index.
        MetadataIndex md = new MetadataIndex(store,3);

        /*
         * the separator key for the first partition (index := 0). this will
         * accept any key iff there is only one partition, but you can create
         * paritions with keys that order greater than an empty byte[] and they
         * will be choosen correctly once they exist.
         */
        final byte[] key0 = new byte[] {};
        /*
         * separator key for the 2nd partition (index := 1).
         */
        final byte[] key1 = new byte[] {1,2,3};
        /*
         * separator key for the 3nd partition (index := 2).
         */
        final byte[] key2 = new byte[] {4,5,6};

        /*
         * verify null return when there are no partitions defined.
         */
        assertNull(md.find(key0));
        assertNull(md.find(key1));
        assertNull(md.find(key2));

        /*
         * create three partitions.
         */
        final int partId0 = 0;
        PartitionMetadata part0 = new PartitionMetadata(partId0);
        assertEquals(null,md.put(key0, part0));
        assertEquals(part0,md.get(key0));
        
        final int partId1 = 1;
        PartitionMetadata part1 = new PartitionMetadata(partId1,
                new SegmentMetadata[] { new SegmentMetadata("a", 10L,StateEnum.LIVE) });
        assertEquals(null,md.put(key1, part1));
        assertEquals(part1,md.get(key1));
        
        final int partId2 = 2;
        PartitionMetadata part2 = new PartitionMetadata(partId2,
                new SegmentMetadata[] { new SegmentMetadata("a", 10L,StateEnum.LIVE),
                        new SegmentMetadata("b", 20L,StateEnum.LIVE) });
        assertEquals(null, md.put(key2, part2));
        assertEquals(part2,md.get(key2));

        /*
         * verify that keys greater than or equal to the key for the partition
         * and less than the next partition will select that partition.
         * 
         * Note: indexOf which is the most direct means for verifying the leaf
         * search rule.
         */
        
        /*
         * the is the smallest possible key for any btree. it always selects the
         * first partition.
         */
        assertEquals(part0,md.find(new byte[]{}));
        
        /*
         * a key less than the 2nd partition always selects the first partition.
         */
        assertEquals(part0,md.find(new byte[]{1,2,2}));

        /*
         * a key equal to the 2nd partition selects that partition.
         */
        assertEquals(part1,md.find(new byte[]{1,2,3}));

        /*
         * a key greater than the 2nd partition and less than the 3rd partition
         * selects the 2nd partition.  
         */
        assertEquals(part1,md.find(new byte[]{1,2,4}));

        /*
         * a key equal to the 3nd partition selects that partition.
         */
        assertEquals(part2,md.find(new byte[]{4,5,6}));

        /*
         * a key greater than any partition always selects the last parition.
         */
        assertEquals(part2,md.find(new byte[]{9,9,9}));

    }

    /**
     * Test ability to evict a data index from a journal into a single partition
     * without any pre-existing index segments.
     */
    public void test_evictSegment_onePartition() throws IOException {

        Properties properties = new Properties();
        
        File file = new File(getName()+".jnl");

        if(file.exists() && !file.delete() ) {
        
            fail("Could not delete file: "+file.getAbsoluteFile());
            
        }
        
        properties.setProperty(Options.FILE,file.toString());

        properties.setProperty(Options.SEGMENT,"0");

        Journal store = new Journal(properties);
        
        // partition metadata index.
        MetadataIndex md = new MetadataIndex(store, 3);
        
        // define a single partition with no segments.
        md.put(new byte[]{}, new PartitionMetadata(0));
        
        // btree to be filled with data.
        BTree btree = new BTree(store, 3, SimpleEntry.Serializer.INSTANCE);
        
        /*
         * populate the btree with some data.
         */
        byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[]{v5,v6,v7,v8,v3,v4,v2,v1};

        btree.insert(values.length, keys, values);
        
        assertTrue(btree.dump(Level.DEBUG,System.err));

        assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                btree.entryIterator());

        /*
         * evict the btree into an index segment.
         */

        File outFile = new File(getName()+"-part0.seg");

        outFile.deleteOnExit();
        
        assertTrue(!outFile.exists() || outFile.delete());
        
        new IndexSegmentBuilder(outFile,null,btree,100,0d);

        /*
         * update the metadata index for this partition.
         */
        md.put(new byte[] {}, new PartitionMetadata(0,
                new SegmentMetadata[] { new SegmentMetadata("" + outFile,
                        outFile.length(),StateEnum.LIVE) }));

        /*
         * open and verify the index segment against the btree data.
         */
        IndexSegment seg = new IndexSegment(new IndexSegmentFileStore(outFile),
                btree.nodeSer.valueSerializer);

        assertSameBTree(btree,seg);
        
        /*
         * verify the fused view is the same as the data already on the btree.
         */
        FusedView view = new FusedView(new AbstractBTree[]{btree,seg});
        
        assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                view.rangeIterator(null,null));

        /*
         * close the index segment, discard files.
         */
        seg.close();
        
        outFile.delete();
        
    }

    /**
     * Test the ability to evict a data index multiple times for a single
     * partition where the secondary index segments are merged down each time
     * into a single index segment and a fused view is required only to read
     * across the journal and the single index segment.
     * 
     * @throws IOException
     */
    public void test_evictSegments_onePartition_withMerge() throws IOException {

        Properties properties = new Properties();
        
        File file = new File(getName()+".jnl");

        if(file.exists() && !file.delete() ) {
        
            fail("Could not delete file: "+file.getAbsoluteFile());
            
        }
        
        properties.setProperty(Options.FILE,file.toString());

        properties.setProperty(Options.SEGMENT,"0");

        Journal store = new Journal(properties);
        
        // partition metadata index.
        MetadataIndex md = new MetadataIndex(store, 3);
        
        // define a single partition with no segments.
        md.put(new byte[]{}, new PartitionMetadata(0));
        
        // btree to be filled with data.
        BTree btree = new BTree(store, 3, SimpleEntry.Serializer.INSTANCE);
        
        /*
         * populate the btree with some data.
         */
        byte[][] keys1 = new byte[][] { new byte[] { 1 },
                new byte[] { 3 }, new byte[] { 5 }, new byte[] { 7 } };

        byte[][] keys2 = new byte[][] { new byte[] { 2 }, new byte[] { 4 },
                new byte[] { 6 }, new byte[] { 8 } };

        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values1 = new Object[] { v1, v3, v5, v7 }; // Note: modified by insert!
        Object[] values2 = new Object[] { v2, v4, v6, v8 }; // Note: modified by insert!
        
        btree.insert(values1.length, keys1, values1);
        
        assertTrue(btree.dump(Level.DEBUG,System.err));

        assertSameIterator(new Object[]{v1,v3,v5,v7}, btree.entryIterator());
        
        /*
         * evict the btree into an index segment.
         */

        File outFile01 = new File(getName()+"-part0.01.seg");

        outFile01.deleteOnExit();
        
        assertTrue(!outFile01.exists() || outFile01.delete());
        
        new IndexSegmentBuilder(outFile01,null,btree,100,0d);

        /*
         * update the metadata index for this partition.
         */
        md.put(new byte[] {}, new PartitionMetadata(0,
                new SegmentMetadata[] { new SegmentMetadata("" + outFile01,
                        outFile01.length(),StateEnum.LIVE) }));

        /*
         * open and verify the index segment against the btree data.
         */
        IndexSegment seg01 = new IndexSegment(new IndexSegmentFileStore(outFile01),
                btree.nodeSer.valueSerializer);

        assertSameBTree(btree,seg01);
        
        /*
         * verify the fused view is the same as the data already on the btree.
         */
        assertSameIterator(new Object[] { v1, v3, v5, v7 }, new FusedView(
                new AbstractBTree[] { btree, seg01 }).rangeIterator(null, null));

        /*
         * create a new btree and insert the other keys/values into this btree.
         */
        btree = new BTree(store,3,SimpleEntry.Serializer.INSTANCE);
        
        btree.insert(values2.length, keys2, values2);
        
        assertTrue(btree.dump(Level.DEBUG,System.err));

        assertSameIterator(new Object[]{v2,v4,v6,v8}, btree.entryIterator());

        /*
         * evict the merge of the 2nd btree and the existing index segment into
         * another index segment
         */

        File outFile02_tmp = new File(getName() + "-part0.02.tmp");

        outFile02_tmp.deleteOnExit();

        File outFile02 = new File(getName() + "-part0.02.seg");

        outFile02.deleteOnExit();

        assertTrue(!outFile02_tmp.exists() || outFile02_tmp.delete());
        assertTrue(!outFile02.exists() || outFile02.delete());

        MergedLeafIterator mergeItr = new IndexSegmentMerger(outFile02_tmp,
                100, btree, seg01).merge();
        
        new IndexSegmentBuilder(outFile02, null, mergeItr.nentries, new MergedEntryIterator(mergeItr
                ), 100, btree.nodeSer.valueSerializer,
                true/* fullyBuffer */, false/* useChecksum */,
                null/*recordCompressor*/, 0d/*errorRate*/);

        /*
         * update the metadata index for this partition.
         * 
         * Note: We mark index segment 01 as "DEAD" for this partition since it
         * has been replaced by the merged result (index segment 02).
         */
        md.put(new byte[] {}, new PartitionMetadata(0, new SegmentMetadata[] {
                new SegmentMetadata("" + outFile01, outFile01.length(),StateEnum.DEAD),
                new SegmentMetadata("" + outFile02, outFile02.length(),StateEnum.LIVE) }));

        /*
         * open and verify the merged index segment against the total expected
         * data.
         */
        IndexSegment seg02 = new IndexSegment(new IndexSegmentFileStore(
                outFile02), btree.nodeSer.valueSerializer);

        assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                seg02.entryIterator());

        /*
         * close the index segments, discard files.
         */
        seg01.close();
        seg02.close();
        
        outFile01.delete();
        outFile02.delete();
                
    }
    
    /**
     * A stress test of the ability to evict a data index multiple times for a single
     * partition where the secondary index segments are merged down each time
     * into a single index segment and a fused view is required only to read
     * across the journal and the single index segment.
     * 
     * @throws IOException
     */
    public void test_evictSegments_onePartition_withMerge_stress() throws IOException {

        Random r = new Random();
        
        Properties properties = new Properties();
        
        File file = new File(getName()+".jnl");

        if(file.exists() && !file.delete() ) {
        
            fail("Could not delete file: "+file.getAbsoluteFile());
            
        }
        
        properties.setProperty(Options.FILE,file.toString());

        properties.setProperty(Options.SEGMENT,"0");

        Journal store = new Journal(properties);
        
        // partition metadata index.
        MetadataIndex md = new MetadataIndex(store, 3);
        
        // define a single partition with no segments.
        md.put(new byte[]{}, new PartitionMetadata(0));
        
        /*
         * In each trial we randomly modify the state of the tree.
         * 
         * We track ground truth in a distinct btree.  Every time we evict and
         * merge the test btree onto the index segment we compare the result
         * against the ground truth tree.  They must always agree.
         * 
         * Note: the fused view of the test btree and the current index segment
         * must also always agree with the ground truth btree.
         */
        // #of trials.
        final int ntrials = 2;
        
        // #of mutation operations per trial (insert, remove).
        final int nops = 10000;
        
        // @todo when true only insert operations are performed (does not test merge of deleted keys).
        final boolean insertOnly = true;

        // The branching factor used on the index segment.
        final int mseg = 100;
        
        // ground truth btree.
        BTree groundTruth = new BTree(store, 3, SimpleEntry.Serializer.INSTANCE);

        // the current index segment and null if there is none yet.
        IndexSegment seg = null;
        
        for(int trial=0; trial<ntrials; trial++) {
        
            // test data btree - new tree on each trial!
            BTree testData = new BTree(store, 3, SimpleEntry.Serializer.INSTANCE);

            /*
             * Insert / remove random key/values.
             * 
             * @todo periodically verify the groundTruth against the fused view
             * of the current testData btree and the last evicted index segment
             * (once one has been evicted).
             */
            for(int i=0;i<nops; i++) {
            
                if (insertOnly || groundTruth.nentries == 0 || r.nextInt(100) > 5) {

                    /*
                     * Note: The more bytes in the key the more likely you are
                     * to update an existing key. Likewise, the large the range
                     * for an [int] key the more likely you are to update that
                     * key twice.
                     */
                    byte[] key = new byte[3]; r.nextBytes(key);
//                    int key = r.nextInt(100000);
                    
                    // random value object.
                    Object val = new SimpleEntry(r.nextInt(1000));
                    
                    // insert into ground truth.
                    groundTruth.insert(key,val);
                    
                    // insert into test data.
                    testData.insert(key,val);

//                    System.err.println("trial="+trial+", op="+i+", insert( "+BytesUtil.toString(key)+", "+val+")");
//                    System.err.println("trial="+trial+", op="+i+", insert( "+key+", "+val+")");
                    
                } else {

                    // a valid index into the ground truth btree.
                    int entryIndex = r.nextInt(groundTruth.nentries);

                    // the key at that index.
                    byte[] tmp = groundTruth.keyAt(entryIndex); 

//                    System.err.println("trial="+trial+", op="+i+", remove("+BytesUtil.toString(key)+")");
                    System.err.println("trial="+trial+", op="+i+", remove("+tmp+")");

                    // remove the key from the ground truth
                    groundTruth.remove(tmp);

                    /* remove the key from the test data.
                     * 
                     * FIXME we need a marker for delete operations in case the
                     * index allows null values.  If we always interpret a null
                     * value as a delete marker then we will wind up deleting
                     * things like the statements in the statement index during
                     * a merge.
                     */
                    testData.insert(tmp,null);
                    
                }
            
            }
            
            if(testData.nentries==0) {
                
                /*
                 * Note: You can not build an index segment with an empty input.
                 */
                System.err.println("trial="+trial+" test data is empty.");
                
                continue;
                
            }

            if (seg == null) {

                /*
                 * evict the btree into an index segment. this control path is
                 * used when there is no pre-existing index segment for a
                 * partition and the only data is in the mutable btree on the
                 * journal.
                 */
                
                System.err.println("trial="+trial+", evicting first segment.");

                File outFile01 = new File(getName() + "-part0."+trial+".seg");

                outFile01.deleteOnExit();

                assertTrue(!outFile01.exists() || outFile01.delete());

                new IndexSegmentBuilder(outFile01, null, testData, mseg, 0d);

                /*
                 * update the metadata index for this partition.
                 */
                md.put(new byte[] {},
                        new PartitionMetadata(0,
                                new SegmentMetadata[] { new SegmentMetadata(""
                                        + outFile01, outFile01.length(),
                                        StateEnum.LIVE) }));

                /*
                 * open and verify the index segment against the btree data.
                 */
                seg = new IndexSegment(new IndexSegmentFileStore(outFile01),
                        testData.nodeSer.valueSerializer);

                assertSameBTree(testData, seg);

            } else {

                /*
                 * Evict the merge of the mutable btree on the journal and the
                 * existing index segment into another index segment.
                 */

                System.err.println("trial="+trial+", evicting and merging with existing segment.");

                // tmp file for the merge process.
                File outFile02_tmp = new File(getName() + "-part0."+trial+".tmp");

                outFile02_tmp.deleteOnExit();

                assertTrue(!outFile02_tmp.exists() || outFile02_tmp.delete());

                // output file for the merged segment.
                File outFile02 = new File(getName() + "-part0."+trial+".seg");

                outFile02.deleteOnExit();

                assertTrue(!outFile02.exists() || outFile02.delete());

                // merge the data from the btree on the journal and the index segment.
                MergedLeafIterator mergeItr = new IndexSegmentMerger(
                        outFile02_tmp, mseg, testData, seg).merge();
                
                // build the merged index segment.
                new IndexSegmentBuilder(outFile02, null, mergeItr.nentries,
                        new MergedEntryIterator(mergeItr), mseg,
                        testData.nodeSer.valueSerializer,
                        true/* fullyBuffer */, false/* useChecksum */,
                        null/* recordCompressor */, 0d/* errorRate */);

                /*
                 * update the metadata index for this partition.
                 * 
                 * @todo We could mark the earlier index segment as "DEAD" for
                 * this partition since it has been replaced by the merged
                 * result.  We could then delete the files for "DEAD" index
                 * segments after a suitable grace period. 
                 */
                PartitionMetadata oldpart = md.put(new byte[] {}, new PartitionMetadata(0,
                        new SegmentMetadata[] {
                                new SegmentMetadata("" + outFile02, outFile02
                                        .length(), StateEnum.LIVE) }));

                /*
                 * open and verify the merged index segment against the total
                 * expected data.
                 */
                IndexSegment seg02 = new IndexSegment(
                        new IndexSegmentFileStore(outFile02),
                        testData.nodeSer.valueSerializer);

                assertSameBTree(groundTruth,seg02);
                
                /*
                 * close and then delete the old index segment.
                 * 
                 * Note: it is a good idea to wait until you have opened the
                 * merged index segment, and even until it has begun to serve
                 * data, before deleting the old index segment that was an input
                 * to that merge!
                 */
                seg.close();
                
                new File(oldpart.segs[0].filename).delete();

                // this is now the current index segment.
                seg = seg02;

            }
                    
        }

        /*
         * close the last index segment and discard its file.
         */

        seg.close();

        new File(md.get(new byte[]{}).segs[0].filename).delete();

        System.err.println("End of stress test: ntrial="+ntrials+", nops="+nops);
        
    }
    
    /**
     * Test the ability to evict a data index multiple times for a single
     * partition where a new secondary index segment is created each time and a
     * fused view is required to read across any data on the journal plus the
     * sequence of created index segments.
     * 
     * @throws IOException
     * 
     * @todo test at #segments evicted (and in the fused view) > 2.
     */
    public void test_evictSegments_onePartition_noMerge() throws IOException {
        
        Properties properties = new Properties();
        
        File file = new File(getName()+".jnl");

        if(file.exists() && !file.delete() ) {
        
            fail("Could not delete file: "+file.getAbsoluteFile());
            
        }
        
        properties.setProperty(Options.FILE,file.toString());

        properties.setProperty(Options.SEGMENT,"0");

        Journal store = new Journal(properties);
        
        // partition metadata index.
        MetadataIndex md = new MetadataIndex(store, 3);
        
        // define a single partition with no segments.
        md.put(new byte[]{}, new PartitionMetadata(0));
        
        // btree to be filled with data.
        BTree btree = new BTree(store, 3, SimpleEntry.Serializer.INSTANCE);
        
        /*
         * populate the btree with some data.
         */
        byte[][] keys1 = new byte[][] { new byte[] { 1 },
                new byte[] { 3 }, new byte[] { 5 }, new byte[] { 7 } };

        byte[][] keys2 = new byte[][] { new byte[] { 2 }, new byte[] { 4 },
                new byte[] { 6 }, new byte[] { 8 } };

        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values1 = new Object[] { v1, v3, v5, v7 }; // Note: modified by insert!
        Object[] values2 = new Object[] { v2, v4, v6, v8 }; // Note: modified by insert!
        
        btree.insert(values1.length, keys1, values1);
        
        assertTrue(btree.dump(Level.DEBUG,System.err));

        assertSameIterator(new Object[]{v1,v3,v5,v7}, btree.entryIterator());
        
        /*
         * evict the btree into an index segment.
         */

        File outFile01 = new File(getName()+"-part0.01.seg");

        outFile01.deleteOnExit();
        
        assertTrue(!outFile01.exists() || outFile01.delete());
        
        new IndexSegmentBuilder(outFile01,null,btree,100,0d);

        /*
         * update the metadata index for this partition.
         */
        md.put(new byte[] {}, new PartitionMetadata(0,
                new SegmentMetadata[] { new SegmentMetadata("" + outFile01,
                        outFile01.length(),StateEnum.LIVE) }));

        /*
         * open and verify the index segment against the btree data.
         */
        IndexSegment seg01 = new IndexSegment(new IndexSegmentFileStore(outFile01),
                btree.nodeSer.valueSerializer);

        assertSameBTree(btree,seg01);
        
        /*
         * verify the fused view is the same as the data already on the btree.
         */
        assertSameIterator(new Object[] { v1, v3, v5, v7 }, new FusedView(
                new AbstractBTree[] { btree, seg01 }).rangeIterator(null, null));

        /*
         * create a new btree and insert the other keys/values into this btree.
         */
        btree = new BTree(store,3,SimpleEntry.Serializer.INSTANCE);
        
        btree.insert(values2.length, keys2, values2);
        
        assertTrue(btree.dump(Level.DEBUG,System.err));

        assertSameIterator(new Object[]{v2,v4,v6,v8}, btree.entryIterator());

        /*
         * evict the 2nd btree into another index segment.
         */

        File outFile02 = new File(getName() + "-part0.02.seg");

        outFile02.deleteOnExit();

        assertTrue(!outFile02.exists() || outFile02.delete());

        new IndexSegmentBuilder(outFile02, null, btree, 100, 0d);

        /*
         * update the metadata index for this partition.
         */
        md.put(new byte[] {}, new PartitionMetadata(0, new SegmentMetadata[] {
                new SegmentMetadata("" + outFile01, outFile01.length(),StateEnum.LIVE),
                new SegmentMetadata("" + outFile02, outFile02.length(),StateEnum.LIVE) }));

        /*
         * open and verify the index segment against the btree data.
         */
        IndexSegment seg02 = new IndexSegment(new IndexSegmentFileStore(
                outFile02), btree.nodeSer.valueSerializer);

        assertSameBTree(btree, seg02);

        /*
         * verify the fused view is the same as the data already on the btree.
         */
        assertSameIterator(new Object[] { v2, v4, v6, v8 }, new FusedView(
                new AbstractBTree[] { btree, seg02 }).rangeIterator(null, null));

        /*
         * verify the fused view of both segments agrees with the total data
         * inserted into the trees.
         */
        assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                new FusedView(new AbstractBTree[] { seg01, seg02 })
                        .rangeIterator(null, null));
        
        /*
         * close the index segments, discard files.
         */
        seg01.close();
        seg02.close();
        
        outFile01.delete();
        outFile02.delete();
        
    }

    /**
     * Sets up multiple partitions and verifies that a data index is evicted
     * onto all relevant partitions and that the metadata index is updated
     * appropriately.
     * 
     * @todo We need to track each partition touched during operations on the
     *       journal and then only check those partitions against the keys in
     *       the data index when performing an eviction. this makes more sense
     *       since we need to cache some runtime metadata anyway, e.g.,
     *       separatorKey:IndexSegment[].
     * 
     * @throws IOException
     */
    public void test_evictSegments_multiplePartitions_noMerge() throws IOException {

        fail("write test");
        
    }

    /**
     * @todo variants with merge of the evicted index segments.
     * 
     * @throws IOException
     */
    public void test_evictSegments_multiplePartitions_withMerge() throws IOException {

        fail("write test");
        
    }

    /**
     * The partitioned database needs to track which index segments are in use
     * and periodically merge them down into a single segment. When this occurs,
     * the old index segments for that partition must be deleted (either
     * immediately or eventually).
     * 
     * @todo do a variant where there are multiple partitions and we examine
     *       statistics on activity on the various partitions, build a list of
     *       partitions to be compacted, and then delegate compacting merge
     *       operations for those partitions.
     */
    public void test_compactingMerge_onePartition() {
        
        fail("write test");
        
    }
    
    /**
     * Test split of a large segment into two (segment overflow).
     *
     */
    public void test_splitSegment() {

        fail("write test");

    }

    /**
     * Test join of a small segment with its left or right sibling (segment
     * underflow).
     */
    public void test_joinSegments() {

        fail("write test");

    }

}
