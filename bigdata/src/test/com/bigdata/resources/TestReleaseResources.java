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
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StoreManager.NoSuchStoreException;
import com.bigdata.resources.StoreManager.Options;

/**
 * Test release of old resources using {@link Options#MIN_RELEASE_AGE}.
 * 
 * @todo write test where the [releaseAge] is set.
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
            properties.setProperty(Options.MIN_RELEASE_AGE,Options.MIN_RELEASE_AGE_NEVER);
            
            return properties;
            
        }
        
        /**
         * Test creates an index whose view is initially defined by the initial
         * journal on the {@link ResourceManager}. An overflow of the journal
         * is then forced, which re-defines the view on the new journal. Since
         * the index is very small (it is empty), it is "copied" onto the new
         * journal rather than causing an index segment build to be scheduled.
         * Once the asynchronous overflow completes, the original journal should
         * qualify for release.
         * 
         * @throws IOException
         * @throws ExecutionException
         * @throws InterruptedException
         */
        public void test() throws IOException,
                InterruptedException, ExecutionException {

            // no overflow yet.
            assertEquals(0,resourceManager.getOverflowCount());
            
            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // force overflow on the next commit.
            concurrencyManager.getWriteService().forceOverflow.set(true);
            
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);

            // did overflow.
            assertEquals(1,resourceManager.getOverflowCount());

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
            
            // 2 journals.
            assertEquals(2,resourceManager.getJournalCount());

            // no index segments.
            assertEquals(0,resourceManager.getIndexSegmentCount());
            
            /*
             * Verify that we can the first commit record when we provide the
             * create time for a journal.
             */
            
            // for j0
            assertEquals(j0.getRootBlockView().getFirstCommitTime(),
                    resourceManager
                            .getCommitTimeStrictlyGreaterThan(createTime0));

            // for j1.
            assertEquals(j1.getRootBlockView().getFirstCommitTime(),
                    resourceManager
                            .getCommitTimeStrictlyGreaterThan(createTime1));

            /*
             * Verify that the resources required for [A] when we first
             * registered it are exactly [j0].
             */
            assertSameResources(new IRawStore[] { j0 }, //
                    resourceManager.getResourcesForTimestamp(j0.getRootBlockView()
                            .getFirstCommitTime()));


            /*
             * Verify that the resources required for [A] after overflow are
             * exactly [j1].
             */
            assertSameResources(new IRawStore[] { j1 }, //
                    resourceManager.getResourcesForTimestamp(j1.getRootBlockView()
                            .getFirstCommitTime()));
            
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
            properties.setProperty(Options.MIN_RELEASE_AGE,"0");
            
            return properties;
            
        }
        
        /**
         * Test creates an index whose view is initially defined by the initial
         * journal on the sole data service. An overflow of the journal is then
         * forced, which re-defines the view on the new journal. Since the index
         * is very small (it is empty), it is "copied" onto the new journal
         * rather than causing an index segment build to be scheduled.  Once the
         * asynchronous overflow completes, the original journal should qualify
         * for release.
         * 
         * @throws IOException
         * @throws ExecutionException
         * @throws InterruptedException
         */
        public void test() throws IOException,
                InterruptedException, ExecutionException {

            // no overflow yet.
            assertEquals(0,resourceManager.getOverflowCount());

            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // the UUID for that journal.
            final UUID uuid0 = j0.getResourceMetadata().getUUID();
            
            // force overflow on the next commit.
            concurrencyManager.getWriteService().forceOverflow.set(true);
            
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);

            // did overflow.
            assertEquals(1,resourceManager.getOverflowCount());

            // should have been closed (and deleted) during sync. overflow processing.
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
            assertEquals(1,resourceManager.getJournalCount());

            // no index segments.
            assertEquals(0,resourceManager.getIndexSegmentCount());
            
            /*
             * Verify that the resources required for [A] after overflow are
             * exactly [j1].
             */
            assertSameResources(new IRawStore[] { j1 }, //
                    resourceManager.getResourcesForTimestamp(j1.getRootBlockView()
                            .getFirstCommitTime()));
            
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
         * The value that will be configured for the {@link Options#MIN_RELEASE_AGE}.
         * 
         * @todo restore to 2000 (2 seconds).
         * 
         * Note: 20000 is 20 seconds.
         * 
         * Note: 200000 is 200 seconds.
         */
        final private long MIN_RELEASE_AGE = 2000; 
        
        public Properties getProperties() {
            
            Properties properties = new Properties( super.getProperties() );

            // 2 seconds (in milliseconds).
            properties.setProperty(Options.MIN_RELEASE_AGE,""+MIN_RELEASE_AGE);
            
            return properties;
            
        }
        
        /**
         * Test where the index view is copied in its entirety onto the new
         * journal and the [minReleaseAge] is 2 seconds. In this case we have no
         * dependencies on the old journal, but the [minReleaseAge] is not
         * satisified immediately so no resources are released during overflow
         * processing (assuming that overflow processing is substantially faster
         * than the [minReleaseAge]). We then wait until the [minReleaseAge] has
         * passed and force overflow processing again and verify that the
         * original journal was released while the 2nd and 3rd journals are
         * retained.
         * 
         * @throws IOException
         * @throws ExecutionException
         * @throws InterruptedException
         */
        public void test() throws IOException,
                InterruptedException, ExecutionException {

            // verify configuration.
            assertEquals(MIN_RELEASE_AGE,resourceManager.getMinReleaseAge());
            
            // no overflow yet.
            assertEquals(0,resourceManager.getOverflowCount());

            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // the UUID for that journal.
            final UUID uuid0 = j0.getResourceMetadata().getUUID();
            
            // force overflow on the next commit.
            concurrencyManager.getWriteService().forceOverflow.set(true);
            
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);
            
            // mark a timestamp after the 1st overflow event.
            final long begin = System.currentTimeMillis();

            // did overflow.
            assertEquals(1,resourceManager.getOverflowCount());

            // two journals.
            assertEquals(2,resourceManager.getJournalCount());
            
            // no index segments.
            assertEquals(0,resourceManager.getIndexSegmentCount());
            
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
             
                final long delay = resourceManager.minReleaseAge - elapsed;
                
                System.err.println("Waiting "+delay+"ms to force an overflow");
                
                // wait until the minReleaseAge would be satisified.
                Thread.sleep( delay );
            
            }

            // force overflow on the next commit.
            concurrencyManager.getWriteService().forceOverflow.set(true);

            // register another index - will force another overflow.
            registerIndex("B");
            
            // did overflow.
            assertEquals(2,resourceManager.getOverflowCount());

            // should have been closed no later than when it was deleted.
            assertFalse(j0.isOpen());
            
            // two journals (the original journal should have been deleted).
            assertEquals(2,resourceManager.getJournalCount());
            
            // no index segments.
            assertEquals(0,resourceManager.getIndexSegmentCount());
            
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
