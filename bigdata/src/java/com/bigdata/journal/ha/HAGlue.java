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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IRootBlockView;

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
     * Return the address at which this service will listen for write pipeline
     * messages sent from the upstream service.
     */
    InetSocketAddress getWritePipelineAddr();

    /*
     * bad reads
     */

    /**
     * Read a record from disk.
     * 
     * @param token
     *            The quorum token.
     * @param addr
     *            The address of the record.
     * 
     * @return A runnable which will transfer the contents of the record into
     *         the buffer returned by the future.
     */
    public RunnableFuture<ByteBuffer> readFromDisk(long token, long addr)
            throws IOException;
    
    /*
     * quorum commit protocol.
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
     * Write replication pipeline.
     */
    /**
     * Accept metadata describing an NIO buffer transfer along the write
     * pipeline.
     * 
     * @param msg
     *            The metadata.
     * 
     * @return The {@link Future} which will become available once the buffer
     *         transfer is complete.
     */
    Future<Void> replicate(HAWriteMessage msg) throws IOException;

    /*
     * Resynchronization.
     * 
     * @todo resynchronization API.
     */
    
}
