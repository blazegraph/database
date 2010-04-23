/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 23, 2010
 */

package com.bigdata.journal.ha;

import java.io.File;
import java.util.Properties;

import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

public class MockQuorumManager implements QuorumManager {

    /** Properties from the unit test setup. */
    private final Properties properties;
    
    private final int k;

    private long token;
    
    final Journal[] stores;

    /**
     * Properties from the unit test setup.
     */
    protected Properties getProperties() {
        
        return properties;
        
    }
    
    public int replicationFactor() {

        return k;
        
    }
    
    public MockQuorumManager(final Properties properties) {

        this.properties = properties;
        
        // @todo configure w/ property?
        k = 3;

        stores = new Journal[k];
                              
        for (int i = 0; i < k; i++) {

            stores[i] = newJournal(properties);

        }
        
        // Start the quorum as met on token ZERO (0).
        token = 0L;

    }
    
    protected Journal newJournal(final Properties properties) {

        return new Journal(properties) {
        
            protected QuorumManager newQuorumManager() {

                return MockQuorumManager.this;

            };
            
        };

    }

    public void assertQuorum(long token) {
        if (token != this.token)
            throw new IllegalStateException();
    }

    public Quorum awaitQuorum() throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    public Quorum getQuorum() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Unit test helper tears down the stores.
     */
    void destroy() {
        
        for(Journal store : stores) {
            
            store.destroy();
            
        }
        
    }

    /**
     * Unit test helper reopens the stores in the quorum.
     */
    void reopen() {

        for (int i = 0; i < k; i++) {

            Journal store = stores[i];

            // close the store.
            store.close();

            if (!store.isStable()) {

                throw new UnsupportedOperationException(
                        "The backing store is not stable");

            }

            // Note: clone to avoid modifying!!!
            final Properties properties = (Properties) getProperties().clone();

            // Turn this off now since we want to re-open the same store.
            properties.setProperty(Options.CREATE_TEMP_FILE, "false");

            // The backing file that we need to re-open.
            final File file = store.getFile();

            if (file == null)
                throw new AssertionError();

            // Set the file property explicitly.
            properties.setProperty(Options.FILE, file.toString());

            stores[i] = newJournal(properties);

        }

    }

}
