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
 * Created on Nov 27, 2007
 */

package com.bigdata.journal;

import java.util.Vector;

import com.bigdata.journal.ReplicatedStore.IRemoteStore;
import com.bigdata.rawstore.IRawStore;

/**
 * FIXME This is a sketch of a service for handling replication of low-level
 * writes on a journal.
 * 
 * @todo it would be best to define an interface for obtaining a socket on which
 *       we can write data. We could use RPC initially, but the overhead is more
 *       than we want - raw socket writes will be much, much better.
 * 
 * @todo consider opening the socket for the data transfer rather than keeping
 *       it open.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ReplicatedStore
 */
public class ReplicatedStoreService {

    /* @todo when defined as a service, expose a method to obtain a socket on
     * which we can write directly thereby avoiding RPC for low-level data
     * transfers.
     * 
//        public UUID getServiceID();

//        public Socket connect();

     */
    
    /**
     * The target replication count.
     */
    private int targetReplicationCount;
    
    /**
     * FIXME if the remote store fails, then we need to transparently substitute
     * a different remote store as the target for replication. This involves at
     * least changing over to write on the secondary remote store and may
     * require us to discover another remote store service in order to retain
     * the required replication factor.
     * 
     * Since we only know the state of the remote store(s) when we call
     * {@link #force(boolean)}, this procedure will also have to ensure that
     * the secondary has all data written since the last force of the local
     * store. This is straight forward since we only need to examine the last
     * byte written on the remote store. Any data not yet written on the remote
     * store must be transferred to that store no later than the next
     * {@link #force(boolean)}.
     * 
     * If a secondary store fails then we need to add another remote store to
     * the end of the list and the last remote store in sequence will be
     * responsible for bringing it up to date.
     */
    private final Vector<IRemoteStore> remoteStores;

    /**
     * Constructor wraps a local store such that it will be replicated N times
     * on remote stores.
     * 
     * @param localStore
     *            The local store.
     * @param targetReplicationCount
     *            The target replication count.
     */
    public ReplicatedStoreService(IRawStore localStore, int targetReplicationCount) {
        
        if (localStore == null)
            throw new IllegalArgumentException();

//        this.localStore = localStore;
        
        this.targetReplicationCount = targetReplicationCount;

        this.remoteStores = new Vector<IRemoteStore>();
        
        /*
         * @todo discover the initial remote stores asynchronously and use
         * conditions to handle notice of need for a new remote store.
         * 
         * @todo smoothly handle add of a new remote store.  until it has
         * been synchronized with either the local store or a synchronized
         * replica of the local store it must not be awaited.
         */
        
        while(remoteStores.size()<targetReplicationCount) {
            
            IRemoteStore remoteStore = discoverRemoteStore();
            
            remoteStores.add(remoteStore);
            
        }
        
    }

    /**
     * Discover an unused remote store that can be used to replicate the data in
     * the local store.
     * 
     * @return The remote store.
     * 
     * @todo choose the remote store from a different machine if at all
     *       possible.
     * 
     * @todo be "rack" aware to minimize network congestion such that we try to
     *       allocate the remote stores as few different racks as possible
     *       (minimum of two different racks of course).
     */
    protected IRemoteStore discoverRemoteStore() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * The target replication count for the local store.
     */
    public int getTargetReplicationCount() {
        
        return targetReplicationCount;
        
    }
    
    /**
     * The actual replication count for the local store is the #of replications
     * of that store that are up to date as of the last {@link #force(boolean)}
     * and which are not known to be failed. The actual replication count is
     * incremented each time a new replica is brought online after the replica
     * has been synchronized with either the local store or one of its
     * synchronized replicas.
     */
    public int getActualReplicationCount() {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Adds another remote store to the chain of remote stores that replicate
     * the data in the local store.
     * 
     * @param remoteStore
     */
    protected void add(IRemoteStore remoteStore) {

        throw new UnsupportedOperationException();

    }
    
    /**
     * Removes a remote store from the chain of remote stores that replicate the
     * data in the local store.
     * 
     * @param remoteStore
     * 
     * @throws IllegalArgumentException
     *             if the remoteStore is not part of the replication chain.
     */
    protected void remove(IRemoteStore remoteStore) {
       
        throw new UnsupportedOperationException();
        
    }

}
