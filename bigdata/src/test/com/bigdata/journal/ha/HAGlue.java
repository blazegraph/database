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
 * Created on Apr 21, 2010
 */

package com.bigdata.journal.ha;

import java.nio.ByteBuffer;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.bd.BufferDescriptor;
import com.bigdata.service.ResourceService;

/**
 * The High Availability nexus for a set of journals or data services having
 * shared persistent state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HAGlue {

    /**
     * The {@link ResourceService} used to deliver raw records to the members of
     * the quorum.
     * 
     * @todo Should this be the same or different from the one used to handle
     *       resynchronization of the entire store and used to ship index
     *       segment files around (for a data service). E.g., questions about
     *       latency, throttling, concurrency, etc. Also, this needs to read
     *       from and write on buffers, so the implementation needs to be a bit
     *       different.
     */
    ResourceService getResourceService() {
        throw new UnsupportedOperationException();
    }

    /**
     * Write a record on the failover chain. Only quorum members will accept the
     * write.
     * 
     * @param newaddr
     * @param oldaddr
     * @param bd
     * @return
     */
    public RunnableFuture<Void> write(long newaddr, long oldaddr, BufferDescriptor bd) {
        
        throw new UnsupportedOperationException();
        
    }

    /*
     * file extension.
     */
    
    public RunnableFuture<Void> truncate(long extent) {
        
        throw new UnsupportedOperationException();
        
    }

    /*
     * bad reads
     */

    /**
     * Read a record from another member of the quorum. This is used to handle
     * bad reads (when a checksum or IO error is reported by the local disk).
     * 
     * @todo hook for monitoring (nagios, etc). bad reads indicate a probable
     *       problem.
     */
    public RunnableFuture<Void> read(long addr, ByteBuffer b) {

        throw new UnsupportedOperationException();

    }
    
    /*
     * commit protocol.
     */

    /**
     * Request a commit by the quorum. This method is invoked on the master. The
     * master will issue the appropriate prepare and related messages to the
     * failover nodes. The commit will go foward if a quorum votes "yes". An
     * {@link #abort()} will be taken if the quorum votes "no".
     * 
     * @param commitTime
     *            The commit time assigned to this commit by the transaction
     *            service.
     * 
     * @todo add timeout configuration parameters for commit.
     */
    public void commitNow(long commitTime) {
     
        throw new UnsupportedOperationException();

    }

    /**
     * Request an abort by the quorum. This message is invoked on the master
     * (all write operations are restricted to the master). The master will
     * issue the appropriate request to the failover nodes, directing them to
     * discard their current write set and reload from the current root block.
     */
    public void abort() {
 
        throw new UnsupportedOperationException();

    }

    /**
     * Message sent to each non-master member of the quorum. The node should
     * retrieve the root block from the master, flush all writes to the backing
     * channel, and acknowledge "yes" if ready to commit.  If the node can not
     * prepare for any reason, then it must return "no".
     * 
     * @param commitCounter
     * @param bd
     *            A buffer from which the node can obtain the root block for the
     *            commit operation.
     * 
     * @return Vote yes/no on prepared to commit.
     */
    RunnableFuture<Boolean> prepare2Phase(long commitCounter, BufferDescriptor bd) {

        throw new UnsupportedOperationException();

    }

    /**
     * Message sent to each non-master member of the quorum telling it to commit
     * using the root block from the corresponding prepare message.
     * 
     * @param commitCounter
     * 
     * @return
     */
    RunnableFuture<Void> commit2Phase(long commitCounter) {

        throw new UnsupportedOperationException();

    }

    /**
     * Message sent to each non-master member of the quorum telling it to
     * discard any writes which would be associated with the specified commit
     * counter. If the node had prepared, then it would also discard the root
     * block image if it had been read. The node then issues an
     * {@link AbstractJournal#abort()} request, reloading all state from the
     * last root block.
     * 
     * @param commitCounter
     * @return
     */
    RunnableFuture<Void> abort2Phase(long commitCounter) {

        throw new UnsupportedOperationException();

    }

    /*
     * Quorum membership
     * 
     * @todo dynamics of quorum membership changes.
     */

    /**
     * 
     */
    public interface Quorum {
       
        /** The #of members of the quorum. */
        int size();
        
        /** The quorum target capacity is the replication factor (k). */
        int capacity();
        
        /** Return true iff size GTE (capacity + 1). */
        boolean isQuorumMet();
        
        /**
         * Submits the message for execution by the next node in the quorum.
         */
        <T> void applyNext(RunnableFuture<T> r);

//        /**
//         * The set of nodes which are discovered and have an agreement on the
//         * distributed state of the journal.
//         */
//        ServiceID[] getServiceIDsInQuorum();
        
    }
    
    public Quorum getQuorum() {

        throw new UnsupportedOperationException();
        
    }

    /*
     * Resynchronization.
     * 
     * @todo resynchronization API.
     */
    
}
