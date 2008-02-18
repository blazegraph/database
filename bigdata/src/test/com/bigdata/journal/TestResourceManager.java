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
 * Created on Feb 17, 2008
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.rawstore.Bytes;

/**
 * Test suite for the {@link ResourceManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestResourceManager extends TestCase2 {

    /**
     * 
     */
    public TestResourceManager() {
    }

    /**
     * @param name
     */
    public TestResourceManager(String name) {
        super(name);
    }

    /** The data directory. */
    File dataDir;
    /** The subdirectory containing the journal resources. */
    File journalsDir;
    /** The subdirectory spanning the index segment resources. */
    File segmentsDir;
    /** The temp directory. */
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));

    /**
     * Sets up the per-test data directory.
     */
    public void setUp() throws Exception {

        super.setUp();
        
        /*
         * Create a normal temporary file whose path is the path of the data
         * directory and then delete the temporary file.
         */

        dataDir = File.createTempFile(getName(), "", tmpDir).getCanonicalFile();
        
        assertTrue(dataDir.delete()); 

        assertFalse(dataDir.exists());

        journalsDir = new File(dataDir,"journals");

        segmentsDir = new File(dataDir,"segments");
        
    }

    /**
     * Removes the per-test data directory.
     */
    public void tearDown() throws Exception {
        
        if (dataDir != null) {

            recursiveDelete(dataDir);
            
        }
        
        super.tearDown();
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(File f) {
        
        if(f.isDirectory()) {
            
            File[] children = f.listFiles();
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        log.info("Removing: " + f);

        if (!f.delete()) {

            log.warn("Could not remove: " + f);
            
        }

    }
    
    /**
     * Test creation of a new {@link ResourceManager}. This verifies the
     * correct creation of the data directory, the various subdirectories, and
     * the initial journal in the appropriate subdirectory.
     * 
     * @throws IOException
     */
    public void test_create() throws IOException {
        
        Properties properties = new Properties();

        properties.setProperty(
                com.bigdata.journal.ResourceManager.Options.DATA_DIR, dataDir
                        .toString());
        
        ResourceManager rmgr = new ResourceManager(properties);

        assertTrue(dataDir.exists());
        assertTrue(dataDir.isDirectory());
        assertTrue(journalsDir.isDirectory());
        assertTrue(segmentsDir.isDirectory());

        // fetch the live journal.
        AbstractJournal journal = rmgr.getLiveJournal();
        
        assertNotNull(journal);
        
        // verify journal created in correct subdirectory.
        assertTrue(new File(journalsDir,journal.getFile().getName()).exists());

        // shutdown
        rmgr.shutdownNow();
        
    }

    /**
     * A test for restart of the {@link ResourceManager}. A directory is
     * created and pre-populated with two {@link Journal}s. The
     * {@link ResourceManager} is started and we verify that it locates the
     * various resources and opens the correct {@link Journal} as its "live"
     * journal.
     * 
     * @throws IOException
     */
    public void test_restartWithTwoJournals() throws IOException {
        
        // create the data directory.
        assertTrue(dataDir.mkdirs());
        assertTrue(journalsDir.mkdirs());
        assertTrue(segmentsDir.mkdirs());
        
        final IResourceMetadata journalMetadata1;
        {

            final File file = File.createTempFile("journal", Options.JNL,
                    journalsDir);

            file.delete(); // remove temp file - will be re-created below.
            
            Properties properties = new Properties();

            properties.setProperty(Options.FILE, file.toString());
         
            Journal journal = new Journal(properties);
            
            // commit the journal to assign [firstCommitTime].
            journal.commit();

            // wait for at least one more distinct timestamp to go by.
            journal.nextTimestamp();

            journalMetadata1 = journal.getResourceMetadata();
            
            journal.shutdownNow();
            
            assertTrue(journalMetadata1.getCommitTime() > 0L);
            
        }

        /*
         * Create the 2nd journal.
         */
        final IResourceMetadata journalMetadata2;
        {

            final File file = File.createTempFile("journal", Options.JNL,
                    journalsDir);

            file.delete(); // remove temp file - will be re-created below.

            Properties properties = new Properties();

            properties.setProperty(Options.FILE, file.toString());
         
            Journal journal = new Journal(properties);
            
            // commit the journal to assign [firstCommitTime].
            journal.commit();

            journalMetadata2 = journal.getResourceMetadata();
            
            journal.shutdownNow();
            
            assertTrue(journalMetadata1.getCommitTime() < journalMetadata2
                    .getCommitTime());
            
        }

        /*
         * Open the resource manager.
         */
        final ResourceManager rmgr;
        {
            
            Properties properties = new Properties();
            
            properties.setProperty(
                    com.bigdata.journal.ResourceManager.Options.DATA_DIR,
                    dataDir.toString());
            
            rmgr = new ResourceManager(properties);
            
        }
        
        // verify live journal was opened.
        assertNotNull(rmgr.getLiveJournal());
        
        // verify same reference each time we request the live journal. 
        assertTrue(rmgr.getLiveJournal()==rmgr.getLiveJournal());
        
        // verify #of journals discovered.
        assertEquals(2,rmgr.getJournalCount());
        
        // open one journal.
        assertNotNull(rmgr.openStore(journalMetadata1.getUUID()));

        // open the other journal.
        assertNotNull(rmgr.openStore(journalMetadata2.getUUID()));
        
        // verify correct journal has same reference as the live journal.
        assertTrue(rmgr.getLiveJournal()==rmgr.openStore(journalMetadata2.getUUID()));
        
        // shutdown.
        rmgr.shutdownNow();
        
    }
    
    /**
     * A test for restart of the {@link ResourceManager}. A directory is
     * created and pre-populated with a {@link Journal} and some
     * {@link IndexSegment} constructed from data on that {@link Journal}. The
     * {@link ResourceManager} is started and we verify that it locates the
     * various resources and opens the correct {@link Journal} as its "live"
     * journal.
     * 
     * @throws IOException
     */
    public void test_restartWithIndexSegments() throws IOException {
        
        // create the data directory.
        assertTrue(dataDir.mkdirs());
        assertTrue(journalsDir.mkdirs());
        assertTrue(segmentsDir.mkdirs());
        
        final int nsegments = 3;
        final IResourceMetadata journalMetadata;
        final UUID[] segmentUUIDs = new UUID[nsegments];
        {

            final File file = File.createTempFile("journal", Options.JNL,
                    journalsDir);

            file.delete(); // remove temp file - will be re-created below.
            
            Properties properties = new Properties();

            properties.setProperty(Options.FILE, file.toString());
         
            Journal journal = new Journal(properties);
            
            // commit the journal to assign [firstCommitTime].
            journal.commit();

            // wait for at least one more distinct timestamp to go by.
            journal.nextTimestamp();

            journalMetadata = journal.getResourceMetadata();
            
            /*
             * Create some index segments.
             */
            {

                for (int i = 0; i < nsegments; i++) {

                    // create btree.
                    BTree ndx = BTree.create(journal, new IndexMetadata("ndx#" + i, UUID.randomUUID()));

                    // populate with some data.
                    for (int j = 0; j < 100; j++) {

                        ndx.insert(KeyBuilder.asSortKey(j), SerializerUtil
                                .serialize(new Integer(j)));

                    }

                    final long commitTime = journal.commit();

                    final File outFile = new File(segmentsDir, "ndx" + i
                            + Options.SEG);

                    final int branchingFactor = 20;

                    final IndexSegmentBuilder builder = new IndexSegmentBuilder(
                            outFile, tmpDir, ndx.getEntryCount(), ndx
                                    .entryIterator(), branchingFactor, ndx
                                    .getIndexMetadata(), commitTime);

                    segmentUUIDs[i] = builder.segmentUUID;
                    
                }

            }
            
            journal.shutdownNow();
            
            assertTrue(journalMetadata.getCommitTime() > 0L);
            
        }

        /*
         * Open the resource manager.
         */
        final ResourceManager rmgr;
        {
            
            Properties properties = new Properties();
            
            properties.setProperty(
                    com.bigdata.journal.ResourceManager.Options.DATA_DIR,
                    dataDir.toString());
            
            rmgr = new ResourceManager(properties);
            
        }
        
        // verify #of journals discovered.
        assertEquals(1, rmgr.getJournalCount());

        // verify index segments discovered.
        for(int i=0; i<nsegments; i++) {
            
            IndexSegmentFileStore segStore = (IndexSegmentFileStore) rmgr
                    .openStore(segmentUUIDs[i]);

            // verify opened.
            assertNotNull(segStore);
            
            // verify same reference.
            assertTrue(segStore == rmgr.openStore(segmentUUIDs[i]));
            
        }
        
        // shutdown.
        rmgr.shutdownNow();
        
    }
    
    /**
     * A test for restart of the {@link ResourceManager}. A directory is
     * created and pre-populated with a {@link Journal}. An index is registered
     * on the journal and some data is written on the index. An
     * {@link IndexSegment} constructed from data on that index. The
     * {@link ResourceManager} is started and we verify that it locates the
     * various resources and opens the correct {@link Journal} as its "live"
     * journal.
     * 
     * @throws IOException
     */
    public void test_openIndexPartition() throws IOException {
        
        // create the data directory.
        assertTrue(dataDir.mkdirs());
        assertTrue(journalsDir.mkdirs());
        assertTrue(segmentsDir.mkdirs());
        
        final String indexName = "ndx";
        final int nentries = 100;
        final IResourceMetadata journalMetadata;
        final UUID indexUUID = UUID.randomUUID();
        final UUID segmentUUID;
        final IResourceMetadata segmentMetadata;
        {

            final File file = File.createTempFile("journal", Options.JNL,
                    journalsDir);

            file.delete(); // remove temp file - will be re-created below.
            
            Properties properties = new Properties();

            properties.setProperty(Options.FILE, file.toString());
         
            Journal journal = new Journal(properties);
            
            // commit the journal to assign [firstCommitTime].
            journal.commit();

            // wait for at least one more distinct timestamp to go by.
            journal.nextTimestamp();

            journalMetadata = journal.getResourceMetadata();
            
            /*
             * Create an index partition.
             */
            {

                IndexMetadata indexMetadata = new IndexMetadata(indexName,
                        indexUUID);
                
                // required for scale-out indices.
                indexMetadata.setDeleteMarkers(true);

                // create index and register on the journal.
                IIndex ndx = journal.registerIndex(indexName, BTree.create(journal, indexMetadata));
                
//                // commit journal so that it will notice when the index gets dirty. 
//                journal.commit();
                
                DataOutputBuffer buf = new DataOutputBuffer(Bytes.SIZEOF_INT);
                
                // populate with some data.
                for (int j = 0; j < nentries; j++) {

//                    ndx.insert(KeyBuilder.asSortKey(j), SerializerUtil
//                            .serialize(new Integer(j)));

                    // format the value.
                    buf.reset().putInt(j);
                    
                    ndx.insert(KeyBuilder.asSortKey(j), buf.toByteArray());

                }

                // commit data on the journal - this is the commitTime for the indexSegment!
                final long commitTime = journal.commit();

                // create index segment from btree on journal.
                final int partId = 0;
                {

                    // name the output file.
                    final File outFile = File.createTempFile(//
                            indexMetadata.getName()+"_"+partId, // prefix 
                            Options.SEG, // suffix
                            segmentsDir // directory
                            );

                    final int branchingFactor = 20;

                    final IndexSegmentBuilder builder = new IndexSegmentBuilder(
                            outFile, tmpDir, (int) ndx.rangeCount(null, null), ndx
                                    .rangeIterator(null, null),
                            branchingFactor, ndx.getIndexMetadata(), commitTime);

                    // assigned UUID for the index segment resource.
                    segmentUUID = builder.segmentUUID;

                    // the segment resource description.
                    segmentMetadata = builder.getSegmentMetadata();
                    
                }

                // clone before we start to modify the index metadata.
                indexMetadata = indexMetadata.clone();
                
                // describe the index partition.
                indexMetadata.setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                        partId,//
                        new UUID[]{//dataService UUIDs
                                UUID.randomUUID() // 
                        },//
                        new IResourceMetadata[]{// resource metadata[].
                                journal.getResourceMetadata(),//
                                segmentMetadata//
                        },//
                        new byte[]{}, // left separator (first valid key)
                        null // right separator (no upper bound)
                        ));

                /*
                 * Drop the index that we used to build up the data for the
                 * index segment.
                 */
                journal.dropIndex(indexMetadata.getName());
                
//                // commit changes on the journal.
//                journal.commit();

                /*
                 * Register a new (and empty) index with the same name but with
                 * an index partition definition that includes the index
                 * segment.
                 */
                journal.registerIndex(indexMetadata.getName(), BTree.create(
                        journal, indexMetadata));

                // commit changes on the journal.
                journal.commit();
            
            }
            
            journal.shutdownNow();
            
            assertTrue(journalMetadata.getCommitTime() > 0L);
            
        }

        /*
         * Open the resource manager.
         */
        final ResourceManager rmgr;
        {
            
            Properties properties = new Properties();
            
            properties.setProperty(
                    com.bigdata.journal.ResourceManager.Options.DATA_DIR,
                    dataDir.toString());
            
            rmgr = new ResourceManager(properties);
            
        }
        
        // verify journal discovered.
        assertEquals(1, rmgr.getJournalCount());
        
        // open the journal.
        IJournal journal = rmgr.getLiveJournal();

        // verify index exists on that journal.
        assertNotNull(journal.getIndex(indexName));

        // verify resource manager returns the same index object.
        assertEquals(journal.getIndex(indexName), rmgr.openIndex(indexName,
                0L/* timestamp */, journal));

        // verify index segment discovered.
        IndexSegmentFileStore segStore = (IndexSegmentFileStore) rmgr
                .openStore(segmentUUID);

        // verify opened.
        assertNotNull(segStore);

        // verify same reference.
        assertTrue(segStore == rmgr.openStore(segmentUUID));
        
        /*
         * @todo verify does not double-open an index segement from its store
         * file!
         */
        {
          
            AbstractBTree[] sources = rmgr
                    .openIndexPartition(indexName, 0L/* timestamp */);

            assertNotNull("sources", sources);

            assertEquals("#sources", 2, sources.length);

            // mutable btree on journal is empty.
            assertEquals(0, sources[0].getEntryCount());
            assertTrue(sources[0] instanceof BTree);

            // immutable index segment holds all of the data.
            assertEquals(nentries, sources[1].getEntryCount());
            assertTrue(sources[1] instanceof IndexSegment);

        }

        // shutdown.
        rmgr.shutdownNow();

    }

    /**
     * A test for overflow of the {@link ResourceManager}. We begin with a
     * blank slate, so the {@link ResourceManager} creates an initial
     * {@link Journal} for us and put its into play. The test then registers an
     * initial partition of scale-out index on that journal and some data is
     * written on that index. An overflow operation is executed, which causes a
     * new {@link Journal} to be created and brought into play. The index is
     * re-defined on the new journal such that its view includes the data on the
     * old journal as well.
     * 
     * @throws IOException 
     */
    public void test_overflow() throws IOException {

        Properties properties = new Properties();

        properties.setProperty(
                com.bigdata.journal.ResourceManager.Options.DATA_DIR, dataDir
                        .toString());
        
        ResourceManager rmgr = new ResourceManager(properties);

        /*
         * Define, register, and populate the initial partition of a named
         * scale-out index.
         */
        final String indexName = "testIndex";
        final int nentries = 100;
        {

            AbstractJournal journal = rmgr.getLiveJournal();

            IndexMetadata indexMetadata = new IndexMetadata(indexName, UUID
                    .randomUUID());

            // required for scale-out indices.
            indexMetadata.setDeleteMarkers(true);

            indexMetadata.setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(//
                    0, // partitionId
                    new UUID[]{UUID.randomUUID()},// dataService UUIDs.
                    new IResourceMetadata[]{
                            journal.getResourceMetadata()
                    },
                    new byte[]{}, // leftSeparator.
                    null // rightSeparator.
                    ));
            
            // create index and register on the journal.
            IIndex ndx = journal.registerIndex(indexName, BTree.create(journal,
                    indexMetadata));

            DataOutputBuffer buf = new DataOutputBuffer(Bytes.SIZEOF_INT);

            // populate with some data.
            for (int j = 0; j < nentries; j++) {

                // format the value.
                buf.reset().putInt(j);

                // insert values.
                ndx.insert(KeyBuilder.asSortKey(j), buf.toByteArray());

                // bump the counter. 
                ndx.getCounter().incrementAndGet();
                
            }

            // commit data on the journal
            journal.commit();

        }

        /*
         * Do overflow operation. This should create a new journal and migrate
         * the index definition to the new journal while re-defining the view to
         * include the data on the old journal.
         */
        {

            IJournal oldJ = rmgr.getLiveJournal();
            
            assertEquals(1, rmgr.getJournalCount());

            rmgr.overflow();

            assertEquals(2, rmgr.getJournalCount());

            // verify live journal is a different instance.
            assertTrue(oldJ != rmgr.getLiveJournal());

        }

        /*
         * Verify new view on the index partition.
         */
        {
            
            AbstractBTree[] sources = rmgr
                    .openIndexPartition(indexName, 0L/* timestamp */); 
            
            assertNotNull("sources",sources);
            
            assertEquals("#sources",2,sources.length);

            assertTrue(sources[0] != sources[1]);
            
            // entries are still on the old index.
            assertEquals(nentries,sources[1].getEntryCount());

            // verify counter on the old index is unchanged.
            assertEquals(nentries,sources[1].getCounter().get());

            // verify no entries yet on the new index.
            assertEquals(0,sources[0].getEntryCount());

            // verify counter was carried forward to the new index(!)
            assertEquals(nentries,sources[0].getCounter().get());
            
        }
        
    }

}
