/*

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
 * Created on Nov 26, 2007
 */

package com.bigdata.journal;

import java.io.File;

import com.bigdata.journal.ReplicatedStore.IRemoteStore;

/**
 * Test suite for low-level data replication.
 * 
 * FIXME the replicas all need to be associated with the same journal instance
 * UUID since they are bitwise replicas of that journal. The UUID probably needs
 * to be part of the connection protocol to the remote store and requests need
 * to verify that they are being written to the correct store, so maybe it also
 * needs to be in the header for the socket level data transfers.
 * <p>
 * Validate replicas by byte comparison of the store files (unless an abort has
 * occured, in which case some bytes might be skipped) and by read-committed
 * comparison of high level index scans (which should always agree after a
 * commit).
 * 
 * @todo in order to be able to do read-committed or historical reads against a
 *       replica we have to be able to open it as a journal while it is being
 *       written on as a replica. this should not present any problems, but it
 *       does mean that the replica write features need to be integrated at a
 *       low level with the journal so that they correctly write onto both write
 *       caches (since reads will read through the write cache) and onto the
 *       backing file/buffer. (Actually, without the address associated with
 *       each record we can't read back from a write cache, so the replica IO
 *       needs to go direct to the backing file or buffer without touching the
 *       write cache).
 *       <p>
 *       Since the integration point is so low-level maybe this can be done in
 *       the journal package and then layered to support sockets and discovery
 *       in the services package?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReplicatedStore extends ProxyTestCase {

    /**
     * 
     */
    public TestReplicatedStore() {
    }

    /**
     * @param arg0
     */
    public TestReplicatedStore(String arg0) {

        super(arg0);
        
    }

    /**
     * Trivial implementation used for testing/developing the code.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class RemoteStoreImpl implements IRemoteStore {

        private long nextOffset = 0L;
        
        public void write(byte[] data, long offset, boolean commit) {

            if (data == null)
                throw new IllegalArgumentException();

            if (data.length == 0)
                throw new IllegalArgumentException();
            
            if(offset!=nextOffset) {
                
                throw new IllegalStateException();
                
            }

            // NOP (does not write the data anywhere).
            
            nextOffset += data.length;
            
        }

        public long nextOffset() {
            
            return nextOffset;
            
        }

        public void close() {
            
            // NOP.
            
        }

        public void abort(long nextOffset) {

            if(this.nextOffset>nextOffset) {
                
                throw new IllegalStateException();
                
            }
            
            this.nextOffset = nextOffset;
            
        }


        /** Not backed by stable media so we return null. */
        public File getFile() {
            
            return null;
            
        }
        
    }
    
    public void test_dataReplication() {
        
        log.warn("Write tests");
        
    }
    
}
