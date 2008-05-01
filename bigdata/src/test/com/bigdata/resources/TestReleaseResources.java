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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.rawstore.IRawStore;
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

            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // force overflow on the next commit.
            concurrencyManager.getWriteService().forceOverflow.set(true);
            
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);
            
            // the new journal.
            final AbstractJournal j1 = resourceManager.getLiveJournal();
            
            assertTrue(j0 != j1);

            final long createTime0 = j0.getResourceMetadata().getCreateTime();
            
            final long createTime1 = j1.getResourceMetadata().getCreateTime();

            assertTrue(createTime0 < createTime1);

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

            // the initial journal.
            final AbstractJournal j0 = resourceManager.getLiveJournal();
            
            // force overflow on the next commit.
            concurrencyManager.getWriteService().forceOverflow.set(true);
            
            // disable asynchronous overflow processing to simplify the test environment.
            resourceManager.asyncOverflowEnabled.set(false);
            
            final String name = "A";
            
            // register the index - will also force overflow.
            registerIndex(name);
            
            // should have been closed (and deleted) during sync. overflow processing.
            assertFalse(j0.isOpen());
            
            // the new journal.
            final AbstractJournal j1 = resourceManager.getLiveJournal();

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
     * Test helper.
     * 
     * @param expected
     * @param actual
     */
    protected void assertSameResources(IRawStore[] expected, Set<UUID> actual) {
        
        // copy to avoid side-effects.
        final Set<UUID> tmp = new HashSet<UUID>(actual);
        
        for(int i=0; i<expected.length; i++) {

            final UUID uuid = expected[i].getResourceMetadata().getUUID();
            
            assertFalse(tmp.isEmpty());

            if(!tmp.remove(uuid)) {
                
                fail("Expecting "+expected[i].getResourceMetadata());
                
            }
            
        }

        assertTrue(tmp.isEmpty());
        
    }

}
