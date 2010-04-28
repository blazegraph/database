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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal.ha;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import com.bigdata.LRUNexus;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.journal.ProxyTestCase;

/**
 * <p>
 * Abstract harness for testing under a variety of configurations. In order to
 * test a specific configuration, create a concrete instance of this class. The
 * configuration can be described using a mixture of a <code>.properties</code>
 * file of the same name as the test class and custom code.
 * </p>
 * <p>
 * When debugging from an IDE, it is very helpful to be able to run a single
 * test case. You can do this, but you MUST define the property
 * <code>testClass</code> as the name test class that has the logic required
 * to instantiate and configure an appropriate object manager instance for the
 * test.
 * </p>
 */
abstract public class AbstractHAJournalTestCase
    extends AbstractJournalTestCase
{

    //
    // Constructors.
    //

    public AbstractHAJournalTestCase() {}
    
    public AbstractHAJournalTestCase(String name) {super(name);}

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        super.setUp(testCase);
        
//        if(log.isInfoEnabled())
//        log.info("\n\n================:BEGIN:" + testCase.getName()
//                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        if (stores != null) {
            for (Journal store : stores) {
                store.destroy();
            }
        }

        super.tearDown(testCase);

    }

    private Journal[] stores = null;

    /**
     * Note: Due to the manner in which the {@link MockQuorumManager} is
     * initialized, the elements in {@link #stores} will be <code>null</code>
     * initially. This should be Ok as long as the test gets fully setup before
     * you start making requests of the {@link QuorumManager} or the
     * {@link Quorum}.
     */
    @Override
    protected Journal getStore(final Properties properties) {

        final int k = 3; // @todo config by Properties?

        stores = new Journal[k];
        
        for (int i = 0; i < k; i++) {

            stores[i] = newJournal(i, properties);

        }

        // return the master.
        return stores[0];

    }

    protected Journal newJournal(final int index, final Properties properties) {

        return new Journal(properties) {
        
            protected QuorumManager newQuorumManager() {

                return AbstractHAJournalTestCase.this.newQuorumManager(index,
                        stores);

            };
            
        };

    }

    protected MockQuorumManager newQuorumManager(final int index,
            final Journal[] stores) {

        return new MockQuorumManager(index, stores);

    };

    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed or if the store can not be
     *                re-opened, e.g., from failure to obtain a file lock, etc.
     */
    @Override
    protected Journal reopenStore(final Journal store) {

        if (stores[0] != store)
            throw new AssertionError();
        
        for (int i = 0; i < stores.length; i++) {

            Journal aStore = stores[i];

            if (LRUNexus.INSTANCE != null) {
                /*
                 * Drop the record cache for this store on reopen. This makes it
                 * easier to find errors related to a difference in the bytes on
                 * the disk versus the bytes in the record cache.
                 */
                LRUNexus.INSTANCE.deleteCache(aStore.getUUID());
            }

            // close the store.
            aStore.close();

            if (!aStore.isStable()) {

                throw new UnsupportedOperationException(
                        "The backing store is not stable");

            }

            // Note: clone to avoid modifying!!!
            final Properties properties = (Properties) getProperties().clone();

            // Turn this off now since we want to re-open the same store.
            properties.setProperty(Options.CREATE_TEMP_FILE, "false");

            // The backing file that we need to re-open.
            final File file = aStore.getFile();

            if (file == null)
                throw new AssertionError();

            // Set the file property explicitly.
            properties.setProperty(Options.FILE, file.toString());

            stores[i] = newJournal(i, properties);

        }

        return stores[0];
        
    }

}
