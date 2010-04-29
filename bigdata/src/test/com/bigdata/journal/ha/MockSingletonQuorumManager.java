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
 * Created on Apr 29, 2010
 */

package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.IRootBlockView;

public class MockSingletonQuorumManager implements QuorumManager {

    final int k = 1;
    final long token = 0L;

    public void assertQuorum(long token) {
        if (token != this.token)
            throw new IllegalStateException();
    }

    public Quorum awaitQuorum() throws InterruptedException {
        return quorum;
    }

    public Quorum getQuorum() {
        return quorum;
    }

    public int replicationFactor() {
        return k;
    }

    public boolean isHighlyAvailable() {
        return k > 1;
    }
    
    public void terminate() {
        // NOP
    }
    
    final Quorum quorum = new Quorum() {

        public boolean isMaster() {
            return true;
        }

        public boolean isQuorumMet() {
            return true;
        }

        /** The master is always at index ZERO (0). */
        public int getIndex() {
            return 0;
        }
        
        public void abort2Phase() throws IOException {
            // TODO Auto-generated method stub
            
        }

        public void commit2Phase(long commitTime) throws IOException {
            // TODO Auto-generated method stub
            
        }

        public HAGlue getHAGlue(int index) {
            if (index < 0 || index >= replicationFactor())
                throw new IndexOutOfBoundsException();
            // TODO Auto-generated method stub
            return null;
        }

        public int prepare2Phase(IRootBlockView rootBlock, long timeout,
                TimeUnit unit) throws InterruptedException,
                TimeoutException, IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        public void readFromQuorum(long addr, ByteBuffer b) {
            // TODO Auto-generated method stub
            
        }

        public int replicationFactor() {
            return k;
        }

        public int size() {
            return 1;
        }

        public long token() {
            return token;
        }
        
    };
    
}
