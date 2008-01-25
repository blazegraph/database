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

import java.io.File;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.ByteArrayValueSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.NameAndExtensionFilter;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.Journal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadata;
import com.bigdata.mdi.ResourceState;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.scaleup.MasterJournal.MergePolicy;
import com.bigdata.scaleup.MasterJournal.Options;

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
     * Test the ability to register and use a named index that does NOT support
     * transactional isolation, including whether the named index is restart
     * safe.
     */
    public void test_registerAndUse_noIsolation() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
                
        properties.setProperty(Options.BASENAME,getName());
        
        MasterJournal journal = new MasterJournal(properties);
        
        final String name = "abc";
        
        IIndex index = new BTree(journal, 3, UUID.randomUUID(), ByteArrayValueSerializer.INSTANCE);
        
        assertNull(journal.getIndex(name));
        
        index = journal.registerIndex(name, index);
        
        assertTrue(journal.getIndex(name) instanceof PartitionedIndexView);
        
        assertEquals("name", name, ((PartitionedIndexView) journal.getIndex(name))
                .getName());
        
        MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);
        
        assertEquals("mdi.entryCount", 1, mdi.getEntryCount());
        
        final byte[] k0 = new byte[]{0};
        final byte[] v0 = new byte[]{0};
        
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
            journal = new MasterJournal(properties);

            index = (PartitionedIndexView) journal.getIndex(name);

            assertNotNull("btree", index);
            assertEquals("entryCount", 1, ((PartitionedIndexView)index).getBTree().getEntryCount());
            assertEquals(v0, (byte[])index.lookup(k0));

            journal.dropIndex(name);
            
            journal.close();

        }

    }

    /**
     * Test the ability to register and use a named index that supports
     * transactional isolation, including whether the named index is restart
     * safe.
     */
    public void test_registerAndUse_isolation() {

        Properties properties = getProperties();

        properties.setProperty(Options.DELETE_ON_CLOSE, "false");
                
        properties.setProperty(Options.BASENAME,getName());
        
        MasterJournal journal = new MasterJournal(properties);
        
        final String name = "abc";
        
        IIndex index = new UnisolatedBTree(journal, UUID.randomUUID());
        
        assertNull(journal.getIndex(name));
        
        index = journal.registerIndex(name, index);
        
        assertTrue(journal.getIndex(name) instanceof PartitionedIndexView);
        
        assertEquals("name", name, ((PartitionedIndexView) journal.getIndex(name))
                .getName());
        
        MetadataIndex mdi = journal.getSlave().getMetadataIndex(name);
        
        assertEquals("mdi.entryCount", 1, mdi.getEntryCount());
        
        final byte[] k0 = new byte[]{0};
        final byte[] v0 = new byte[]{0};
        
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
            journal = new MasterJournal(properties);

            index = (PartitionedIndexView) journal.getIndex(name);

            assertNotNull("btree", index);
            assertEquals("entryCount", 1, ((PartitionedIndexView)index).getBTree().getEntryCount());
            assertEquals(v0, (byte[])index.lookup(k0));

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
        
        properties.setProperty(Options.BASENAME,getName());
        
        MasterJournal journal = new MasterJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new UnisolatedBTree(journal, UUID.randomUUID())));
        
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
        
        properties.setProperty(Options.BASENAME,getName());
        
        MasterJournal journal = new MasterJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new UnisolatedBTree(journal,
                3, UUID.randomUUID())));

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
        
        assertEquals("#segments",0,mdi.get(new byte[]{}).getResources().length);
        
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
        
        properties.setProperty(Options.BASENAME,getName());

        properties.setProperty(Options.MERGE_POLICY,
                MergePolicy.CompactingMerge.toString());
        
        MasterJournal journal = new MasterJournal(properties);
        
        final String name = "abc";
        
        assertNotNull(journal.registerIndex(name, new UnisolatedBTree(journal,
                UUID.randomUUID())));

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
        
        assertEquals("#segments",1,mdi.get(new byte[]{}).getResources().length);
        
        /*
         * Verify that the data there.
         */
        data.verify(journal.getIndex(name));

        /*
         * Verify that the btree is empty.
         */
        assertEquals(0, ((PartitionedIndexView) journal.getIndex(name)).getBTree()
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
                
        properties.setProperty(Options.BASENAME,getName());

        properties.setProperty(Options.MERGE_POLICY,
                MergePolicy.CompactingMerge.toString());
        
        MasterJournal journal = new MasterJournal(properties);
        
        final String name = "abc";
        
        final UUID indexUUID = UUID.randomUUID();
        
        assertNotNull(journal.registerIndex(name, new UnisolatedBTree(journal,
                indexUUID)));

        final UnisolatedBTree groundTruth = new UnisolatedBTree(
                new SimpleMemoryRawStore(), indexUUID);

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
            
            assertEquals("partId",0,pmd.getPartitionId());
            
            assertEquals("#segments", 1, pmd.getLiveCount());
            
            IResourceMetadata[] resources = pmd.getResources();
            
            if(resources.length>1) {
                
                assertEquals("#segments",2,resources.length);
                
                assertEquals("state", ResourceState.Dead,
                        resources[0].state());
                
                assertEquals("state", ResourceState.Live,
                        resources[1].state());
                
            }

            /*
             * Verify that the btree is empty.
             */
            assertEquals(0, ((PartitionedIndexView) journal.getIndex(name))
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
        public final byte[][] vals;
        
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
            
            vals = new byte[nrecords][];
            
            int key = 0;
            
            Random r = new Random();
            
            IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);
            
            for(int i=0; i<nrecords; i++) {
                
                key = key + r.nextInt(100) + 1;
                
                keys[i] = keyBuilder.reset().append(key).getKey();

                vals[i] = keys[i].clone();
                
            }
            
        }

        public void insertInto(IIndex ndx) {
            
            // note: clones values to avoid side effect.
            ndx.insert(new BatchInsert(nrecords,keys,vals.clone()));
            
        }
        
        public void verify(IIndex ndx) {
            
            for(int i=0; i<nrecords; i++) {

                byte[] actual = (byte[])ndx.lookup(keys[i]);
                
                assertEquals(vals[i],actual);
                
            }
            
        }
        
    }
    
}
