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
 * Created on Apr 30, 2008
 */

package com.bigdata.resources;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.AbstractTransactionService;

/**
 * Test release (aka purge) of old resources.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReleaseResources extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestReleaseResources() {
    }

    /**
     * @param arg0
     */
    public TestReleaseResources(String arg0) {
        super(arg0);
    }
    
    /*
     * Note: This interfers with the ability to run the individual test classes
     * by themselves.
     */
//    public static Test suite()
//    {
//
//        TestSuite suite = new TestSuite("releaseFree");
//
//        suite.addTestSuite(TestAddDeleteResource.class);
//        suite.addTestSuite(TestReleaseResources.TestReleaseFree.class);
//        suite.addTestSuite(TestReleaseResources.TestWithCopyNoRelease.class);
//        suite.addTestSuite(TestReleaseResources.TestWithCopyImmediateRelease.class);
//        
//        return suite;
//        
//    }
    
//    /**
//     * A unit test for the logic which determines which resources are "release
//     * free" as of a given commit time to be preserved (aka, resource which are
//     * not required for views based on any commit point from the timestamp to be
//     * preserved up to and including the unisolated views).
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static public class TestReleaseFree extends TestReleaseResources {
//        
//
//        /**
//         * 
//         */
//        public TestReleaseFree() {
//        }
//
//        /**
//         * @param arg0
//         */
//        public TestReleaseFree(String arg0) {
//            super(arg0);
//        }
//        
//        public Properties getProperties() {
//            
//            Properties properties = new Properties( super.getProperties() );
//
////            // Immediate release of old resources.
////            properties.setProperty(Options.MIN_RELEASE_AGE,
////                    Options.MIN_RELEASE_AGE_NO_HISTORY);
//            
////            // Overflow should not occur during this test.
////            properties.setProperty(OverflowManager.Options.OVERFLOW_ENABLED,
////                    "false");
//            
//            return properties;
//            
//        }
//        
//        /**
//         * Test creates a sequence of view declarations for an index and
//         * verifies that the correct set of resources are identified as being
//         * in-use for a variety of timestamps.
//         * 
//         * @throws IOException
//         * @throws ExecutionException
//         * @throws InterruptedException
//         */
//        public void test() throws IOException,
//                InterruptedException, ExecutionException {
//
//            fail("Write test");
//            
//        }
//        
//    }
    
    /**
     * Test where the index view is copied in its entirety onto the new journal
     * but the {@link ResourceManager} is not permitted to release old resources
     * (it is configured as an immortal store).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class TestWithCopyNoRelease extends TestReleaseResources {
        
        /**
         * 
         */
        public TestWithCopyNoRelease() {
        }

        /**
         * @param arg0
         */
        public TestWithCopyNoRelease(String arg0) {
            super(arg0);
        }
        
        public Properties getProperties() {
            
            Properties properties = new Properties( super.getProperties() );

            // Never release old resources.
            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE,
                    AbstractTransactionService.Options.MIN_RELEASE_AGE_NEVER);
            
            return properties;
            
        }

		/**
		 * Test creates an index whose view is initially defined by the initial
		 * journal on the {@link ResourceManager}. An overflow of the journal is
		 * then forced, which re-defines the view on the new journal. Since the
		 * index is very small (it is empty), it is "copied" onto the new
		 * journal rather than causing an index segment build to be scheduled.
		 * The original journal should not be released.
		 * 
		 * @throws IOException
		 * @throws ExecutionException
		 * @throws InterruptedException
		 */
        public void test() throws IOException,
                InterruptedException, ExecutionException {

            // no overflow yet.
            assertEquals(0,resourceManager.getAsynchronousOverflowCount());
            
            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // force overflow on the next commit.
//            concurrencyManager.getWriteService().forceOverflow.set(true);
            resourceManager.forceOverflow.set(true);
          
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);

            // did overflow.
            assertEquals(1,resourceManager.getAsynchronousOverflowCount());

            // the new journal.
            final AbstractJournal j1 = resourceManager.getLiveJournal();
            
            assertTrue(j0 != j1);

            final long createTime0 = j0.getResourceMetadata().getCreateTime();
            
            final long createTime1 = j1.getResourceMetadata().getCreateTime();

            assertTrue(createTime0 < createTime1);

            // verify can still open the original journal.
			assertTrue(j0 == resourceManager.openStore(j0.getResourceMetadata()
					.getUUID()));

            // verify can still open the new journal.
            assertTrue(j1 == resourceManager.openStore(j1.getResourceMetadata()
                    .getUUID()));
            
            // 1 journals.
            assertEquals(2,resourceManager.getManagedJournalCount());

            // no index segments.
            assertEquals(0,resourceManager.getManagedSegmentCount());
            
            /*
             * Verify that the commit time index.
             */

			// for j0
			assertEquals(j1.getRootBlockView().getFirstCommitTime(),
					resourceManager
							.getCommitTimeStrictlyGreaterThan(createTime1));

			// for j1.
			assertEquals(j1.getRootBlockView().getFirstCommitTime(),
					resourceManager
							.getCommitTimeStrictlyGreaterThan(createTime1));

            /*
             * Verify that the resources required for [A] are {j0, j1} when the
             * probe commitTime is the timestamp when we registered [A] on [j0].
             * (j1 is also represented since we include all dependencies for all
             * commit points subsequent to the probe in order to ensure that we
             * do not accidently release dependencies required for more current
             * views of the index).
             */
            {
                
                final long commitTime = j0.getRootBlockView()
                        .getFirstCommitTime();
                
                final Set<UUID> actual = resourceManager.getResourcesForTimestamp(commitTime);
                
                assertSameResources(new IRawStore[] { j0, j1 }, actual);
            
            }

            /*
             * Verify that the resources required for [A] after overflow are
             * exactly [j1].
             */
            {
                
                final long commitTime = j1.getRootBlockView()
                        .getFirstCommitTime();
                
                final Set<UUID> actual = resourceManager
                        .getResourcesForTimestamp(commitTime);
                
                assertSameResources(new IRawStore[] { j1 }, actual);
                
            }
            
        }
        
    }
    
    /**
     * Test where the index view is copied in its entirety onto the new journal
     * and the [minReleaseAge] is ZERO(0). In this case we have no dependencies
     * on the old journal and it is released during overflow processing.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class TestWithCopyImmediateRelease extends TestReleaseResources {
        
        /**
         * 
         */
        public TestWithCopyImmediateRelease() {
        }

        /**
         * @param arg0
         */
        public TestWithCopyImmediateRelease(String arg0) {
            super(arg0);
        }
        
        public Properties getProperties() {
            
            Properties properties = new Properties( super.getProperties() );

            // Never retain history.
            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "0");
            
            return properties;
            
        }

		/**
		 * Test creates an index whose view is initially defined by the initial
		 * journal on the sole data service. An overflow of the journal is then
		 * forced, which re-defines the view on the new journal. Since the index
		 * is very small (it is empty), it is "copied" onto the new journal
		 * rather than causing an index segment build to be scheduled. The
		 * original journal should be released (deleted) during synchronous
		 * overflow processing.
		 * 
		 * @throws IOException
		 * @throws ExecutionException
		 * @throws InterruptedException
		 */
        public void test() throws IOException,
                InterruptedException, ExecutionException {

            // no overflow yet.
            assertEquals(0,resourceManager.getAsynchronousOverflowCount());

            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // the UUID for that journal.
            final UUID uuid0 = j0.getResourceMetadata().getUUID();
            
            // force overflow on the next commit.
//            concurrencyManager.getWriteService().forceOverflow.set(true);
            resourceManager.forceOverflow.set(true);

            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);

            // did overflow.
            assertEquals(1,resourceManager.getAsynchronousOverflowCount());

//			/*
//			 * Note: the old journal should have been closed for writes during
//			 * synchronous overflow processing and deleted from the file system.
//			 */
//            assertTrue(j0.isOpen()); // still open
//            assertTrue(j0.isReadOnly()); // but no longer accepts writes.
//            
//            /*
//             * Purge old resources. If the index was copied to the new journal
//             * then there should be no dependency on the old journal and it
//             * should be deleted.
//             */
//            {
//              
//                final AbstractJournal liveJournal = resourceManager
//                        .getLiveJournal();
//
//                final long lastCommitTime = liveJournal.getLastCommitTime();
//                
//                final Set<UUID> actual = resourceManager
//                        .getResourcesForTimestamp(lastCommitTime);
//                
//                assertSameResources(new IRawStore[] {liveJournal}, actual);
//
//                // only retain the lastCommitTime.
//                resourceManager.setReleaseTime(lastCommitTime - 1);
//                
//            }
//
//            resourceManager
//                    .purgeOldResources(1000/* ms */, false/*truncateJournal*/);
            
            // verify that the old journal is no longer open.
            assertFalse(j0.isOpen());

            // verify unwilling to open the store with that UUID.
            try {

                resourceManager.openStore(uuid0);
                
                fail("Expecting exception: "+NoSuchStoreException.class);
                
            } catch(NoSuchStoreException ex) {
                
                log.warn("Expected exception: "+ex);
                
            }
            
            // the new journal.
            final AbstractJournal j1 = resourceManager.getLiveJournal();
            
            // the UUID for that resource.
            final UUID uuid1 = j1.getResourceMetadata().getUUID();

            // verify willing to "open" the store with that UUID.
            assertTrue(j1 == resourceManager.openStore(uuid1));

            // one journal remaining.
            assertEquals(1,resourceManager.getManagedJournalCount());

            // no index segments.
            assertEquals(0,resourceManager.getManagedSegmentCount());
            
            /*
             * Verify that the resources required for [A] after overflow are
             * exactly [j1].
             */
            final Set<UUID> actualResourceUUIDs = resourceManager
                    .getResourcesForTimestamp(j1.getRootBlockView()
                            .getFirstCommitTime());
            
            System.err.println("resources="+actualResourceUUIDs);
            
            assertSameResources(new IRawStore[] { j1 }, //
                    actualResourceUUIDs);
            
        }
        
    }
    
    /**
     * Test where the indices are copied during synchronous overflow processing
     * and where a non-zero [minReleaseAge] was specified.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class TestWithCopy_NonZeroMinReleaseAge extends TestReleaseResources {
        
        /**
         * 
         */
        public TestWithCopy_NonZeroMinReleaseAge() {
        }

        /**
         * @param arg0
         */
        public TestWithCopy_NonZeroMinReleaseAge(String arg0) {
        
            super(arg0);
            
        }

		/**
		 * This is the minimum release time that will be used for the test.
		 * <P>
		 * Note: 2000 is 2 seconds. This is what you SHOULD use for the test (it
		 * can be a little longer if you run into problems).
		 * <p>
		 * Note: 200000 is 200 seconds. This can be used for debugging, but
		 * always restore the value so that the test will run in a reasonable
		 * time frame.
		 */
        final private long MIN_RELEASE_AGE = 2000; 
        
        public Properties getProperties() {
            
            final Properties properties = new Properties( super.getProperties() );

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, ""
                            + MIN_RELEASE_AGE);
            
            return properties;
            
        }

		/**
		 * Test where the index view is copied in its entirety onto the new
		 * journal and the [minReleaseAge] is 2 seconds. In this case we have no
		 * dependencies on the old journal, but the [minReleaseAge] is not
		 * satisfied immediately so no resources are released during synchronous
		 * overflow processing (assuming that synchronous overflow processing is
		 * substantially faster than the [minReleaseAge]). We then wait until
		 * the [minReleaseAge] has passed and force overflow processing again
		 * and verify that the original journal was released while the 2nd and
		 * 3rd journals are retained.
		 * 
		 * @throws IOException
		 * @throws ExecutionException
		 * @throws InterruptedException
		 */
        public void test() throws IOException,
                InterruptedException, ExecutionException {

            // no overflow yet.
            assertEquals(0,resourceManager.getAsynchronousOverflowCount());

            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // the UUID for that journal.
            final UUID uuid0 = j0.getResourceMetadata().getUUID();
            
            // force overflow on the next commit.
//            concurrencyManager.getWriteService().forceOverflow.set(true);
            resourceManager.forceOverflow.set(true);
            
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);
            
            // mark a timestamp after the 1st overflow event.
            final long begin = System.currentTimeMillis();

            // did overflow.
            assertEquals(1,resourceManager.getAsynchronousOverflowCount());

            // two journals.
            // @todo unit test needs to be updated to reflect purge in sync overflow.
            assertEquals(2,resourceManager.getManagedJournalCount());
            
            // no index segments.
            assertEquals(0,resourceManager.getManagedSegmentCount());
            
            // should have been closed against further writes but NOT deleted.
            assertTrue(resourceManager.openStore(uuid0).isReadOnly());
            
            // the new journal.
            final AbstractJournal j1 = resourceManager.getLiveJournal();

            // the UUID for that journal.
            final UUID uuid1 = j1.getResourceMetadata().getUUID();
            
            /*
             * Verify that the resources required for [A] after overflow are
             * exactly [j1].
             */
            assertSameResources(new IRawStore[] { j1 }, //
                    resourceManager.getResourcesForTimestamp(j1.getRootBlockView()
                            .getFirstCommitTime()));
            
            {
            
                final long elapsed = System.currentTimeMillis() - begin;
             
                final long delay = MIN_RELEASE_AGE - elapsed;
                
                System.err.println("Waiting " + delay
                        + "ms to force an overflow");
                
                // wait until the minReleaseAge would be satisified.
                Thread.sleep( delay );
            
            }

            // force overflow on the next commit.
//            concurrencyManager.getWriteService().forceOverflow.set(true);
            resourceManager.forceOverflow.set(true);

            // register another index - will force another overflow.
            registerIndex("B");
            
            // did overflow.
            assertEquals(2,resourceManager.getAsynchronousOverflowCount());

            // note: purge is not invoke on overflow anymore, so do it ourselves.
            resourceManager.purgeOldResources(100/*ms*/, false/*truncateJournal*/);
            
            // should have been closed no later than when it was deleted.
            assertFalse(j0.isOpen());
            
            // two journals (the original journal should have been deleted).
            assertEquals(2,resourceManager.getManagedJournalCount());
            
            // no index segments.
            assertEquals(0,resourceManager.getManagedSegmentCount());
            
            // should have been closed against further writes but NOT deleted.
            assertTrue(resourceManager.openStore(uuid1).isReadOnly());

            // verify unwilling to open the store with that UUID.
            try {

                resourceManager.openStore(uuid0);
                
                fail("Expecting exception: "+NoSuchStoreException.class);
                
            } catch(NoSuchStoreException ex) {
                
                log.warn("Expected exception: "+ex);
                
            }

        }
        
    }

}
