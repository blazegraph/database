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

import java.util.Properties;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Journal;

public class MockQuorumManager implements QuorumManager {

    final Journal[] stores;

    public MockQuorumManager(final Properties properties) {

        final int k = 3;

        stores = new Journal[k];
                              
        for (int i = 0; i < k; i++) {

            stores[i] = new Journal(properties) {

                protected QuorumManager newQuorumManager() {

                    return MockQuorumManager.this;

                };

            };

        }

    }
    
    public void assertQuorum(long token) {
        // TODO Auto-generated method stub
        
    }

    public Quorum awaitQuorum() throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    public Quorum getQuorum() {
        // TODO Auto-generated method stub
        return null;
    }

    public int replicationFactor() {
        // TODO Auto-generated method stub
        return 0;
    }
    
}