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

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.IndexPartitionCause;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;

/**
 * Bootstrap test suite for the {@link ResourceManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestResourceManagerBootstrap extends AbstractResourceManagerBootstrapTestCase {

    /**
     * 
     */
    public TestResourceManagerBootstrap() {
    }

    /**
     * @param name
     */
    public TestResourceManagerBootstrap(String name) {
        super(name);
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
            
            final File[] children = f.listFiles();

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if(log.isInfoEnabled())
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

        /*
         * Setup the resource manager.
         */
        
        final Properties properties = getProperties();

        final ResourceManager resourceManager = new MyResourceManager(
                properties);

        final AbstractTransactionService txService = new MockTransactionService(
                properties).start();

        final AbstractLocalTransactionManager localTransactionManager = new MockLocalTransactionManager(
                txService);

        final ConcurrencyManager concurrencyManager = new ConcurrencyManager(
                properties, localTransactionManager, resourceManager);

        try {

            resourceManager.setConcurrencyManager(concurrencyManager);

            assertTrue(resourceManager.awaitRunning());

            /*
             * Do tests.
             */

            assertTrue(dataDir.exists());
            assertTrue(dataDir.isDirectory());
            assertTrue(journalsDir.isDirectory());
            assertTrue(segmentsDir.isDirectory());

            // fetch the live journal.
            AbstractJournal journal = resourceManager.getLiveJournal();

            assertNotNull(journal);

            // verify journal created in correct subdirectory.
            assertTrue(new File(journalsDir, journal.getFile().getName())
                    .exists());
        
        } finally {

            // shutdown
            resourceManager.shutdownNow();
            concurrencyManager.shutdownNow();
            localTransactionManager.shutdownNow();
            txService.destroy();

        }
        
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
            
            final Properties properties = new Properties();

            properties.setProperty(Options.FILE, file.toString());
         
            final Journal journal = new Journal(properties);

            // wait for at least one distinct timestamp to go by.
            journal.nextTimestamp();

            journalMetadata1 = journal.getResourceMetadata();

            // required to set the initial commitRecord before we can close out the journal for further writes.
            final long lastCommitTime = journal.commit();
            
            // close out for further writes.
            final long closeTime = journal.nextTimestamp();
            journal.closeForWrites(closeTime);
            
            assertTrue(journalMetadata1.getCreateTime() > 0L);
            
            // and close the journal.
            journal.close();
         
            // verify that we can re-open the journal.
            {
                
                properties.setProperty(Journal.Options.READ_ONLY,"true");
                
                final Journal tmp = new Journal(properties);

                // should be read only.
                assertTrue(tmp.isReadOnly());
                
                // verify last commit time. 
                assertEquals(lastCommitTime, tmp.getLastCommitTime());
                
                // verify journal closed for writes time.
                assertEquals(closeTime, tmp.getRootBlockView().getCloseTime());
                
                tmp.close();
                
            }
            
        }

        /*
         * Create the 2nd journal.
         */
        final IResourceMetadata journalMetadata2;
        {

            final File file = File.createTempFile("journal", Options.JNL,
                    journalsDir);

            file.delete(); // remove temp file - will be re-created below.

            final Properties properties = new Properties();

            properties.setProperty(Options.FILE, file.toString());
         
            final Journal journal = new Journal(properties);
            
            /*
             * Commit the journal - this causes the commitRecordIndex to become
             * restart safe and makes it possible to re-open the resulting
             * journal in read-only mode.
             */

            final long lastCommitTime = journal.commit();

            journalMetadata2 = journal.getResourceMetadata();
            
            journal.shutdownNow();
            
            assertTrue(journalMetadata1.getCreateTime() < journalMetadata2
                    .getCreateTime());
            
            // verify that we can re-open the journal.
            {
                
                final Journal tmp = new Journal(properties);
                
                // verify last commit time. 
                assertEquals(lastCommitTime, tmp.getLastCommitTime());
                
                tmp.close();
                
            }
            
        }

        /*
         * Setup the resource manager.
         */
        
        final Properties properties = getProperties();

        // disable so that our resources are not automatically purged.
        properties.setProperty(StoreManager.Options.PURGE_OLD_RESOURCES_DURING_STARTUP,"false");
        
        final ResourceManager resourceManager = new MyResourceManager(properties);

        final AbstractTransactionService txService = new MockTransactionService(
                properties).start();

        final AbstractLocalTransactionManager localTransactionManager = new MockLocalTransactionManager(
                txService);

        final ConcurrencyManager concurrencyManager = new ConcurrencyManager(
                properties, localTransactionManager, resourceManager);

        try {
        
        resourceManager.setConcurrencyManager(concurrencyManager);
        
        assertTrue( resourceManager.awaitRunning() );
        
        /*
         * Do tests.
         */
        
        // verify live journal was opened.
        assertNotNull(resourceManager.getLiveJournal());
        
        // verify same reference each time we request the live journal. 
        assertTrue(resourceManager.getLiveJournal()==resourceManager.getLiveJournal());
        
        // verify #of journals discovered.
        assertEquals(2,resourceManager.getManagedJournalCount());

        // verify no index segments discovered.
        assertEquals(0,resourceManager.getManagedSegmentCount());
        
        // open one journal.
        assertNotNull(resourceManager.openStore(journalMetadata1.getUUID()));

        // open the other journal.
        assertNotNull(resourceManager.openStore(journalMetadata2.getUUID()));
        
        // verify correct journal has same reference as the live journal.
        assertTrue(resourceManager.getLiveJournal()==resourceManager.openStore(journalMetadata2.getUUID()));
        
        } finally {
        
        // shutdown
        concurrencyManager.shutdownNow();
        localTransactionManager.shutdownNow();
        resourceManager.shutdownNow();
        txService.destroy();
        }
        
    }
    
    /**
     * A test for restart of the {@link ResourceManager}. A directory is
     * created and pre-populated with a {@link Journal} and some
     * {@link IndexSegment}s are constructed from data on that {@link Journal}.
     * The {@link ResourceManager} is started and we verify that it locates the
     * various resources and opens the correct {@link Journal} as its "live"
     * journal.
     * 
     * @throws IOException
     */
    public void test_restartWithIndexSegments() throws Exception {
        
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
            
//            // commit the journal to assign [firstCommitTime].
//            journal.commit();

            // wait for at one distinct timestamp to go by.
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

                    // commit data on the journal.
                    final long commitTime = journal.commit();

                    final File outFile = new File(segmentsDir, "ndx" + i
                            + Options.SEG);

                    final int branchingFactor = 20;

                    final IndexSegmentBuilder builder = IndexSegmentBuilder.newInstance(
                            outFile, tmpDir, ndx.getEntryCount(), ndx
                                    .rangeIterator(), branchingFactor, ndx
                                    .getIndexMetadata(), commitTime, true/* compactingMerge */);
                    
                    builder.call();

                    segmentUUIDs[i] = builder.segmentUUID;
                    
                }

            }
            
            journal.shutdownNow();
            
            assertTrue(journalMetadata.getCreateTime() > 0L);
            
        }

        /*
         * Setup the resource manager.
         */
        
        final Properties properties = getProperties();

        // disable so that our resources are not automatically purged.
        properties.setProperty(StoreManager.Options.PURGE_OLD_RESOURCES_DURING_STARTUP,"false");

        final ResourceManager resourceManager = new MyResourceManager(properties);

        final AbstractTransactionService txService = new MockTransactionService(properties).start();
        
        final AbstractLocalTransactionManager localTransactionManager = new MockLocalTransactionManager(txService);

        final ConcurrencyManager concurrencyManager = new ConcurrencyManager(
                properties, localTransactionManager, resourceManager);
        
        try {

        resourceManager.setConcurrencyManager(concurrencyManager);
        
        assertTrue( resourceManager.awaitRunning() );

        /*
         * Do tests.
         */
        
        // verify #of journals discovered.
        assertEquals(1, resourceManager.getManagedJournalCount());

        // #of index segments discovered.
        assertEquals(nsegments, resourceManager.getManagedSegmentCount());

        // verify index segments discovered.
        for(int i=0; i<nsegments; i++) {
            
            IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
                    .openStore(segmentUUIDs[i]);

            // verify opened.
            assertNotNull(segStore);
            
            // verify same reference.
            assertTrue(segStore == resourceManager.openStore(segmentUUIDs[i]));
            
        }
        
        } finally {
        
        // shutdown
        concurrencyManager.shutdownNow();
        localTransactionManager.shutdownNow();
        resourceManager.shutdownNow();
        txService.destroy();
        
        }
        
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
    public void test_openIndexPartition() throws Exception {
        
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
            
//            // commit the journal to assign [firstCommitTime].
//            journal.commit();

            // wait for at one distinct timestamp to go by.
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
                ILocalBTreeView ndx = journal.registerIndex(indexName, BTree.create(journal, indexMetadata));
                
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
                final int partitionId = 0;
                {

                    // name the output file.
                    final File outFile = File.createTempFile(//
                            indexMetadata.getName()+"_"+partitionId, // prefix 
                            Options.SEG, // suffix
                            segmentsDir // directory
                            );

                    final int branchingFactor = 20;

                    final IndexSegmentBuilder builder = IndexSegmentBuilder.newInstance(
                            outFile, tmpDir, (int) ndx.rangeCount(null, null), ndx
                                    .rangeIterator(null, null),
                            branchingFactor, ndx
                                    .getIndexMetadata(), commitTime, true/* compactingMerge */);
                    
                    builder.call();

                    // assigned UUID for the index segment resource.
                    segmentUUID = builder.segmentUUID;

                    // the segment resource description.
                    segmentMetadata = builder.getSegmentMetadata();
                    
                }

                // clone before we start to modify the index metadata.
                indexMetadata = indexMetadata.clone();
                
                // describe the index partition.
                indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(
                        partitionId,//
                        -1, // not a move.
                        new byte[]{}, // left separator (first valid key)
                        null,         // right separator (no upper bound)
                        /*
                         * Note: The journal gets listed first since it can
                         * continue to receive writes and therefore logically
                         * comes before the index segment in the resource
                         * ordering since any writes on the live index on the
                         * journal will be more recent than the data on the
                         * index segment.
                         */
                        new IResourceMetadata[]{// resource metadata[].
                                journal.getResourceMetadata(),//
                                segmentMetadata //
                        },//
                        /*
                         * Note: using fake data here since the resource manager
                         * has not been instantiated yet.
                         */
                        new IndexPartitionCause(
                                        IndexPartitionCause.CauseEnum.Register,
                                        0/*overflowCounter*/, System
                                                .currentTimeMillis()/*lastCommitTime*/),
                        "bootstrap() "// history
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
            
            assertTrue(journalMetadata.getCreateTime() > 0L);
            
        }

        /*
         * Setup the resource manager.
         */
        
        final Properties properties = getProperties();

        final ResourceManager resourceManager = new MyResourceManager(properties);
        
        final AbstractTransactionService txService = new MockTransactionService(properties).start();
        
        final AbstractLocalTransactionManager localTransactionManager = new MockLocalTransactionManager(txService);
        
        final ConcurrencyManager concurrencyManager = new ConcurrencyManager(
                properties, localTransactionManager, resourceManager);

        try {
        
        resourceManager.setConcurrencyManager(concurrencyManager);
        
        assertTrue( resourceManager.awaitRunning() );

        /*
         * Do tests.
         */
        
        // verify journal discovered.
        assertEquals(1, resourceManager.getManagedJournalCount());
        
        // open the journal.
        IJournal journal = resourceManager.getLiveJournal();

        // verify index exists on that journal.
        assertNotNull(journal.getIndex(indexName));

        // verify resource manager returns the same index object.
        assertEquals(journal.getIndex(indexName), resourceManager.getIndexOnStore(indexName,
                0L/* timestamp */, journal));

        // an index segment was found.
        assertEquals(1, resourceManager.getManagedSegmentCount());

        // verify index segment discovered.
        IndexSegmentStore segStore = (IndexSegmentStore) resourceManager
                .openStore(segmentUUID);

        // verify opened.
        assertNotNull(segStore);

        // verify same reference.
        assertTrue(segStore == resourceManager.openStore(segmentUUID));
        
        /*
         * @todo verify does not double-open an index segement from its store
         * file!
         */
        {
          
            AbstractBTree[] sources = resourceManager
                    .getIndexSources(indexName, 0L/* timestamp */);

            assertNotNull("sources", sources);

            assertEquals("#sources", 2, sources.length);

            // mutable btree on journal is empty.
            assertTrue(sources[0] instanceof BTree);
            assertEquals(0, sources[0].getEntryCount());

            // immutable index segment holds all of the data.
            assertTrue(sources[1] instanceof IndexSegment);
            assertEquals(nentries, sources[1].getEntryCount());

        }
        
        } finally {

        // shutdown
        concurrencyManager.shutdownNow();
        localTransactionManager.shutdownNow();
        resourceManager.shutdownNow();
        txService.destroy();
        
        }

    }

    protected static class MyResourceManager extends ResourceManager {

        public MyResourceManager(Properties properties) {

            super(properties);

        }

        public UUID getDataServiceUUID() {

            throw new UnsupportedOperationException();

        }

        public UUID[] getDataServiceUUIDs() {

            throw new UnsupportedOperationException();
            
        }

        public IBigdataFederation getFederation() {

            throw new UnsupportedOperationException();

        }

        public DataService getDataService() {

            throw new UnsupportedOperationException();

        }

    }

}
