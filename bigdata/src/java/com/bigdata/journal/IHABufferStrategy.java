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
 * Created on May 18, 2010
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.quorum.Quorum;

/**
 * A highly available {@link IBufferStrategy}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHABufferStrategy extends IBufferStrategy {

    /**
     * Write a buffer containing data replicated from the master onto the local
     * persistence store.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    void writeRawBuffer(IHAWriteMessage msg, IBufferAccess b) throws IOException,
            InterruptedException;

    /**
     * Send an {@link IHAWriteMessage} and the associated raw buffer through the
     * write pipeline.
     * 
     * @param req
     *            The {@link IHALogRequest} for some HALog file.
     * @param msg
     *            The {@link IHAWriteMessage}.
     * @param b
     *            The raw buffer. Bytes from position to limit will be sent.
     *            remaining() must equal {@link IHAWriteMessage#getSize()}.
     * 
     * @return The {@link Future} for that request.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    Future<Void> sendHALogBuffer(IHALogRequest req, IHAWriteMessage msg,
            IBufferAccess b) throws IOException, InterruptedException;

    /**
     * Send an {@link IHAWriteMessage} and the associated raw buffer through the
     * write pipeline.
     * 
     * @param req
     *            The {@link IHARebuildRequest} to replicate the backing file to
     *            the requesting service.
     * @param sequence
     *            The sequence of this {@link IHAWriteMessage} (origin ZERO
     *            (0)).
     * @param quorumToken
     *            The quorum token of the leader, which must remain valid across
     *            the rebuild protocol.
     * @param fileExtent
     *            The file extent as of the moment that the leader begins to
     *            replicate the existing backing file.
     * @param offset
     *            The starting offset (relative to the root blocks).
     * @param nbytes
     *            The #of bytes to be sent.
     * @param b
     *            The raw buffer. The buffer will be cleared and filled with the
     *            specified data, then sent down the write pipeline.
     * 
     * @return The {@link Future} for that request.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    Future<Void> sendRawBuffer(IHARebuildRequest req, 
            //long commitCounter,
            //long commitTime, 
            long sequence, long quorumToken, long fileExtent,
            long offset, int nbytes, ByteBuffer b) throws IOException,
            InterruptedException;

    /**
     * Read from the local store in support of failover reads on nodes in a
     * highly available {@link Quorum}.
     * 
     * @throws InterruptedException
     */
    ByteBuffer readFromLocalStore(final long addr) throws InterruptedException;

    /**
     * Extend local store for a highly available {@link Quorum}.
     * 
     * @throws InterruptedException
     */
    void setExtentForLocalStore(final long extent) throws IOException,
    		InterruptedException;
    
    /**
     * Called from {@link AbstractJournal} commit2Phase to ensure is able to
     * read committed data that has been streamed directly to the backing store.
     */
    void resetFromHARootBlock(final IRootBlockView rootBlock);

    /**
     * Return the #of {@link WriteCache} blocks that have been written out as
     * part of the current write set. This value is origin ZERO (0) and is reset
     * to ZERO (0) after each commit or abort.
     */
    long getBlockSequence();

    /**
     * Snapshot the allocators in preparation for computing a digest of the
     * committed allocations.
     * 
     * @return The snapshot in a format that is backing store specific.
     */
    Object snapshotAllocators();

    /**
     * Compute the digest.
     * <p>
     * Note: The digest is not reliable unless you either use a snapshot or
     * suspend writes (on the quorum) while it is computed.
     * 
     * @param snapshot
     *            The allocator snapshot (optional). When given, the digest is
     *            computed only for the snapshot. When <code>null</code> it is
     *            computed for the entire file.
     * @throws DigestException
     */
    void computeDigest(Object snapshot, MessageDigest digest)
            throws DigestException, IOException;

}
