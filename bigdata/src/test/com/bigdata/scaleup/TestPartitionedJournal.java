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

import java.io.File;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.journal.Journal;
import com.bigdata.objndx.AbstractBTreeTestCase;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BatchInsert;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.objndx.SimpleEntry;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.scaleup.PartitionedJournal.MergePolicy;
import com.bigdata.scaleup.PartitionedJournal.Options;

/**
 * @todo update how we create and get rid of the temporary files created by the
 * tests.
 * 
 * @todo rather than writing this test suite directly, we mostly want to apply
 * the existing proxy test suites for {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestPartitionedJournal extends TestCase2 {

    /**
     * 
     */
    public TestPartitionedJournal() {
    }

    /**
     * @param name
     */
    public TestPartitionedJournal(String name) {
        super(name);
    }

    public void setUp() throws Exception {

        super.setUp();

        deleteTestFiles();
        
    }
    
    public void tearDown() throws Exception {

        super.tearDown();

        deleteTestFiles();
        
    }
    
    protected void deleteTestFiles() {

        NameAndExtensionFilter filter = new NameAndExtensionFilter(getName(),Options.JNL);
        
        File[] files = filter.getFiles();

        for(int i=0; i<files.length; i++) {

            File file = files[i];
            
            if(file.exists() &&!file.delete()) {
                
                log.warn("Could not delete test file: "+file);
                
            }
            
        }

    }
    
    /**
     * Test the ability to register and use named index, including whether the
     * named index is restart safe.
     */
    public void test_registerAndUse() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
        
        properties.setProperty(Options.SEGMENT, "0");
        
        properties.setProperty(Options.BASENAME,getName());
        
        PartitionedJournal journal = new PartitionedJournal(properties);
        
        final String name = "abc";
        
        IIndex index = new BTree(journal, 3, SimpleEntry.Serializer.INSTANCE);
        
        assertNull(journal.getIndex(name));
        
        index = journal.registerIndex(name, index);
        
        assertTrue(journal.getIndex(name) instanceof PartitionedIndex);
        
        assertEquals("name", name, ((PartitionedIndex) journal.getIndex(name))
                .getName());
        
        MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);
        
        assertEquals("mdi.entryCount", 1, mdi.getEntryCount());
        
        final byte[] k0 = new byte[]{0};
        final Object v0 = new SimpleEntry(0);
        
        index.insert( k0, v0);

        /*
         * commit and close the journal
         */
        journal.commit();
        
        journal.close();
        
        if (journal.isStable()) {

            /*
             * re-open the journal and test restart safety.
             */
            journal = new PartitionedJournal(properties);

            index = (PartitionedIndex) journal.getIndex(name);

            assertNotNull("btree", index);
            assertEquals("entryCount", 1, ((PartitionedIndex)index).getBTree().getEntryCount());
            assertEquals(v0, index.lookup(k0));

            journal.dropIndex(name);
            
            journal.close();

        }

    }

    /**
     * test creates a partitioned index and triggers overflow then verifies
     * that the index appears on the journal after overflow.
     */
    public void test_overflow_zeroEntries() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
        
        properties.setProperty(Options.SEGMENT, "0");
        
        properties.setProperty(Options.BASENAME,getName());
        
        PartitionedJournal journal = new PartitionedJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new BTree(journal, 3,
                SimpleEntry.Serializer.INSTANCE)));
        
        journal.overflow();

        assertNotNull(journal.getIndex(name));

        journal.dropIndex(name);

        journal.close();

    }
    
    /**
     * Test registers a partitioned index, inserts some data into that index
     * and then triggers an overflow.  The #of records is choosen to be less
     * than the threshold for evicting the data to an index segment.  the test
     * verifies that the index appears on the journal after overflow and that
     * the records are on the mutable index on the journal and that no index
     * segments were created for that index.
     */
    public void test_overflow_underThreshold() {
    
        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
        
        properties.setProperty(Options.SEGMENT, "0");
        
        properties.setProperty(Options.BASENAME,getName());
        
        PartitionedJournal journal = new PartitionedJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new BTree(journal, 3,
                SimpleEntry.Serializer.INSTANCE)));

        final TestData data = new TestData(journal.migrationThreshold-1);
        
        data.insertInto(journal.getIndex(name));

        data.verify(journal.getIndex(name));

        journal.overflow();

        assertNotNull(journal.getIndex(name));
        
        /*
         * verify that there is only one partition and that no segments were
         * created.
         */
        MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);
        
        assertEquals("#partitions",1,mdi.getEntryCount());
        
        assertEquals("#segments",0,mdi.get(new byte[]{}).segs.length);
        
        /*
         * verify that the data are there.
         */
        data.verify(journal.getIndex(name));

        journal.dropIndex(name);

        journal.close();

    }
    
    public void test_overflow_overThreshold() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
        
        properties.setProperty(Options.SEGMENT, "0");
        
        properties.setProperty(Options.BASENAME,getName());

        properties.setProperty(Options.MERGE_POLICY,
                MergePolicy.CompactingMerge.toString());
        
        PartitionedJournal journal = new PartitionedJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new BTree(journal, 3,
                SimpleEntry.Serializer.INSTANCE)));

        final TestData data = new TestData(journal.migrationThreshold);
        
        data.insertInto(journal.getIndex(name));

        data.verify(journal.getIndex(name));

        journal.overflow();

        assertNotNull(journal.getIndex(name));
        
        /*
         * verify that there is only one partition and that one segment was
         * created.
         */
        MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);
        
        assertEquals("#partitions",1,mdi.getEntryCount());
        
        assertEquals("#segments",1,mdi.get(new byte[]{}).segs.length);
        
        /*
         * Verify that the data there.
         */
        data.verify(journal.getIndex(name));

        /*
         * Verify that the btree is empty.
         */
        assertEquals(0, ((PartitionedIndex) journal.getIndex(name)).getBTree()
                .getEntryCount());

        journal.dropIndex(name);

        journal.close();

    }

    /**
     * A test that forces repeated compacting merges for a single index.
     * 
     * @todo this does not test deletes since keys are asserted but never
     *       removed.
     * 
     * @todo this assumes that at most 2 segments exist for a partition - one
     *       dead and one live.
     */
    public void test_multipleOverflow_overThreshold_compactingMerges() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
        
        properties.setProperty(Options.SEGMENT, "0");
        
        properties.setProperty(Options.BASENAME,getName());

        properties.setProperty(Options.MERGE_POLICY,
                MergePolicy.CompactingMerge.toString());
        
        PartitionedJournal journal = new PartitionedJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new BTree(journal, 3,
                SimpleEntry.Serializer.INSTANCE)));

        final BTree groundTruth = new BTree(new SimpleMemoryRawStore(),
                BTree.DEFAULT_BRANCHING_FACTOR, SimpleEntry.Serializer.INSTANCE);

        final int ntrials = 10;

        for (int trial = 0; trial < ntrials; trial++) {
            
            final TestData data = new TestData(journal.migrationThreshold);

            data.insertInto(groundTruth);
            
            data.insertInto(journal.getIndex(name));

            journal.overflow();

            assertNotNull(journal.getIndex(name));

            /*
             * verify that there is only one partition and that one segment was
             * created.
             */
            MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);

            assertEquals("#partitions", 1, mdi.getEntryCount());

            final byte[] separatorKey = new byte[] {};
            
            PartitionMetadata pmd = mdi.get(separatorKey);
            
            assertEquals("partId",0,pmd.partId);
            
            assertEquals("nextSegId",trial+1,pmd.nextSegId);
            
            assertEquals("#segments", 1, pmd.getLiveCount());
            
            if(pmd.segs.length>1) {
                
                assertEquals("#segments",2,pmd.segs.length);
                
                assertEquals("state", IndexSegmentLifeCycleEnum.DEAD,
                        pmd.segs[0].state);
                
                assertEquals("state", IndexSegmentLifeCycleEnum.LIVE,
                        pmd.segs[1].state);
                
            }

            /*
             * Verify that the btree is empty.
             */
            assertEquals(0, ((PartitionedIndex) journal.getIndex(name))
                    .getBTree().getEntryCount());

            /* 
             * Verify the index
             */
            AbstractBTreeTestCase.doEntryIteratorTest(groundTruth, journal
                    .getIndex(name));

        }

//        /* 
//         * Verify the index
//         */
//        AbstractBTreeTestCase.doEntryIteratorTest(groundTruth, journal
//                .getIndex(name));
        
        journal.dropIndex(name);

        journal.close();

    }

    public void test_overflow_overThreshold_multiplePartitions() {
        fail("write test");
    }

    public void test_overflow_multipleIndices_overThreshold() {
        fail("write test");
    }

    public void test_overflow_multipleIndices_overThreshold_multiplePartitions() {
        fail("write test");
    }
    
    /**
     * Helper class used to insert and validate data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestData {
        
        public final int nrecords;
        
        public final byte[][] keys;
        public final SimpleEntry[] vals;
        
        /**
         * generates a set of N records with ascending keys and values that
         * correspond to the key value.
         * 
         * @param nrecords
         */
        public TestData(int nrecords) {
            
            assert nrecords>0;
            
            this.nrecords = nrecords;
            
            keys = new byte[nrecords][];
            
            vals = new SimpleEntry[nrecords];
            
            int key = 0;
            
            Random r = new Random();
            
            KeyBuilder keyBuilder = new KeyBuilder();
            
            for(int i=0; i<nrecords; i++) {
                
                key = key + r.nextInt(100) + 1;
                
                keys[i] = keyBuilder.reset().append(key).getKey();
                
                vals[i] = new SimpleEntry(key);
                
            }
            
        }

        public void insertInto(IIndex ndx) {
            
            // note: clones values to avoid side effect.
            ndx.insert(new BatchInsert(nrecords,keys,vals.clone()));
            
        }
        
        public void verify(IIndex ndx) {
            
            for(int i=0; i<nrecords; i++) {

                SimpleEntry actual = (SimpleEntry)ndx.lookup(keys[i]);
                
                assertEquals(vals[i],actual);
                
            }
            
        }
        
    }
    
}
