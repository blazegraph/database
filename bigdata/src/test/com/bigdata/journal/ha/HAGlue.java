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

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Remote;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IRootBlockView;
import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * A remote interface for methods supporting high availability for a set of
 * journals or data services having shared persistent state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Scale-out needs to add {@link AbstractJournal#closeForWrites(long)} to
 *       this API.
 */
public interface HAGlue extends Remote {

    /**
     * Return the address that will be used by the write pipeline to talk to the
     * next service in the quorum order.
     */
    InetAddress getWritePipelineAddr();

    /**
     * Return the port that will be used by the write pipeline to talk to the
     * next service in the quorum order.
     */
    int getWritePipelinePort();
    
//    int getReadServicePort();
    
//    /**
//     * The {@link ResourceService} used to deliver raw records to the members of
//     * the quorum.
//     * 
//     * @todo Should this be the same or different from the one used to handle
//     *       resynchronization of the entire store and used to ship index
//     *       segment files around (for a data service). E.g., questions about
//     *       latency, throttling, concurrency, etc. Also, this needs to read
//     *       from and write on buffers, so the implementation needs to be a bit
//     *       different.
//     */
//    ResourceService getResourceService() {
//        throw new UnsupportedOperationException();
//    }

    /*
     * Note: write() is handled at the WriteCache.  Entire WriteCache instances
     * are send down a configured pipeline at a time.
     */
//    /**
//     * Write a record on the failover chain. Only quorum members will accept the
//     * write.
//     * 
//     * @param newaddr
//     * @param oldaddr
//     * @param bd
//     * @return
//     */
//    public RunnableFuture<Void> write(long newaddr, long oldaddr, BufferDescriptor bd) {
//        
//        throw new UnsupportedOperationException();
//        
//    }

    /*
     * file extension.
     */

    /**
     * Set the file length on on the backing store for this service.
     * 
     * @param token The token for the quorum making the request.
     * @param extent The new extent for the backing file.
     */
    public RunnableFuture<Void> truncate(long token, long extent)
            throws IOException;

    /*
     * bad reads
     */

//    /**
//     * @todo This could be implemented over the {@link ResourceService} with
//     *       the modification to read from and write on a {@link ByteBuffer}
//     *       . In that case, it does not need to be a remote method call and
//     *       could be removed from the {@link HAGlue} interface.
//     */
//    public RunnableFuture<Void> readFromQuorum(long addr, BufferDescriptor b)
//            throws IOException;
    
    /*
     * commit protocol.
     */

    /**
     * Save a reference to the caller's root block, flush writes to the backing
     * channel and acknowledge "yes" if ready to commit. If the node can not
     * prepare for any reason, then it must return "no".
     * 
     * @param rootBlock
     *            The new root block.
     * 
     * @return A {@link Future} which evaluates to a yes/no vote on whether the
     *         service is prepared to commit.
     */
    RunnableFuture<Boolean> prepare2Phase(IRootBlockView rootBlock)
            throws IOException;

    /**
     * Commit using the root block from the corresponding prepare message. It is
     * an error if a commit message is observed without the corresponding
     * prepare message.
     * 
     * @param commitTime
     *            The commit time that will be assigned to the new commit point.
     */
    RunnableFuture<Void> commit2Phase(long commitTime) throws IOException;

    /**
     * Discard the current write set using {@link AbstractJournal#abort()},
     * reloading all state from the last root block, etc.
     * 
     * @param token
     *            The token for the quorum for which this request was made.
     */
    RunnableFuture<Void> abort2Phase(long token) throws IOException;
    
    /*
     * Resynchronization.
     * 
     * @todo resynchronization API.
     */
    
}
