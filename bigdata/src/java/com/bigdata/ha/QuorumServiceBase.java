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
 * Created on Jun 1, 2010
 */

package com.bigdata.ha;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.AbstractQuorumMember;

/**
 * Abstract implementation provides the logic for distributing messages for the
 * quorum 2-phase commit protocol, failover reads, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Change the generic type of <L> to {@link IResourceManager}.
 */
abstract public class QuorumServiceBase<S extends HAGlue, L extends AbstractJournal>
        extends AbstractQuorumMember<S> implements QuorumService<S> {

    static protected transient final Logger log = Logger
            .getLogger(QuorumServiceBase.class);

    private final S service;

    private final L localService;

    private final QuorumPipelineImpl<S> pipelineImpl;

    private final QuorumCommitImpl<S> commitImpl;

    private final QuorumReadImpl<S> readImpl;

    /**
     * @param logicalServiceId
     *            The identifier of the logical service.
     * @param serviceId
     *            The {@link UUID} for this service (a physical instance of the
     *            logical service).
     * @param service
     *            The interface for the local service that is exposed to remote
     *            clients (typically as a smart proxy).
     * @param localService
     *            The local service implementation.
     */ 
    protected QuorumServiceBase(final String logicalServiceId,
            final UUID serviceId, final S service, final L localService) {

        super(logicalServiceId, serviceId);

        if (localService == null)
            throw new IllegalArgumentException();

        this.service = service;
        
        this.localService = localService;

        /*
         * Delegates. 
         */
        
        addListener(this.pipelineImpl = new QuorumPipelineImpl<S>(this) {

            @Override
            protected void handleReplicatedWrite(final HAWriteMessage msg,
                    final ByteBuffer data) throws Exception {

                QuorumServiceBase.this.handleReplicatedWrite(msg, data);

            }
            
            @Override
            public long getLastCommitTime() {

                return QuorumServiceBase.this.getLastCommitTime();
                
            }
        
            @Override
            public long getLastCommitCounter() {

                return QuorumServiceBase.this.getLastCommitCounter();
                
            }

        });

        addListener(this.commitImpl = new QuorumCommitImpl<S>(this));

        addListener(this.readImpl = new QuorumReadImpl<S>(this));
        
    }
    
    @Override
    public S getService() {
        
        return service;
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public S getService(UUID serviceId);

    /**
     * FIXME Return the {@link IResourceManager}, {@link Journal}, [@link
     * DataService}, etc. Probably rename to getResourceManager().
     */
    protected L getLocalService() {
        
        return localService;
        
    }

    @Override
    public Executor getExecutor() {

        return getLocalService().getExecutorService();
        
    }

    // @todo fast code path for self? or use RMI proxy for self?
//  public S getService(UUID serviceId) {
//      return null;
//  }

    /*
     * QuorumPipeline
     */
    
//    @Override
//    public HAReceiveService<HAWriteMessage> getHAReceiveService() {
//        
//        return pipelineImpl.getHAReceiveService();
//        
//    }

//    @Override
//    public HASendService getHASendService() {
//        
//        return pipelineImpl.getHASendService();
//        
//    }

    @Override
    public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
            throws IOException {
        
        return pipelineImpl.receiveAndReplicate(msg);
        
    }

    @Override
    public Future<Void> replicate(final HAWriteMessage msg, final ByteBuffer b)
            throws IOException {
    
        return pipelineImpl.replicate(msg, b);
        
    }

    /**
     * Core implementation handles the message and payload when received on a
     * service.
     * 
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param data
     *            The buffer containing the data.
     * 
     * @throws Exception
     * 
     * @see QuorumPipelineImpl#handleReplicatedWrite(HAWriteMessage, ByteBuffer)
     */
    abstract protected void handleReplicatedWrite(HAWriteMessage msg,
            ByteBuffer data) throws Exception;
 
    /**
     * Log the {@link HAWriteMessage} and the associated data onto the
     * appropriate log file.
     * <p>
     * Note: Logging MUST NOT start in the middle of a write set (all log files
     * must be complete). The {@link HAWriteMessage#getSequence()} MUST be ZERO
     * (0) when we open a log file.
     * 
     * TODO The WORM should not bother to log the write cache block since it can
     * be obtained directly from the backing store. Abstract out an object to
     * manage the log file for a commit counter and make it smart about the WORM
     * versus the RWStore. The same object will need to handle the read back
     * from the log file and in the case of the WORM should get the data block
     * from the backing file. However, this object is not responsible for reply
     * of log files. We'll have to locate a place for that logic next - probably
     * in this class (QuorumServiceBase).
     * 
     * FIXME NOTHING IS CALLING THIS CODE YET! Invoke from
     * {@link #handleReplicatedWrite(HAWriteMessage, ByteBuffer)} and from
     * {@link WriteCacheService}s WriteTask.call() method (on the leader).
     */
    public void logWriteCacheBlock(final HAWriteMessage msg,
            final ByteBuffer data) throws IOException {

//        final long currentCommitCounter = getLastCommitCounter();

        getQuorum().assertQuorum(msg.getQuorumToken());

        if (msg.getSequence() == 0L) {

            if (processLog != null) {
                processLog.close();
                processLog = null;
            }

            /*
             * The commit counter that will be used to identify the file.
             * 
             * Note: We use commitCounter+1 so the file will be labeled by the
             * commit point that will be achieved when that log file is applied
             * to a journal whose current commit point is [commitCounter].
             */
            final long commitCounter = msg.getCommitCounter() + 1;

            /*
             * Format the name of the log file.
             * 
             * Note: The commit counter in the file name should be zero filled
             * to 20 digits so we have the files in lexical order in the file
             * system (for convenience).
             */
            final String logFile;
            {

                final StringBuilder sb = new StringBuilder();
                
                final Formatter f = new Formatter(sb);
                
                f.format("%020d.log", commitCounter);
                
                logFile = sb.toString();
                
            }
            
            // Establish new log file.
            processLog = new ObjectOutputStream(new FileOutputStream(new File(
                    getHALogDir(), logFile)));
           
        }

        /*
         * FIXME We need to track whether or not we began the sequence at ZERO
         * (0). If we did, then we can open a log and start writing for the
         * current commitCounter. We do need to keep track of the commit counter
         * associated with the log file so we can correctly refuse to log blocks
         * on the log file that are associated with a different commit counter.
         * We also need to manage the abort() and commit() transitions, ensuring
         * that we truncate() the log for abort() (assuming it is for the same
         * commit counter) and that we append the root block, force() and
         * close() the log for commit.
         */
        
    }
    
    /**
     * Process log to which the receiveService should write the messages to and
     * <code>null</code> if we may not write on it.
     * 
     * FIXME We need to clear this any time the quorum breaks.
     */
    private ObjectOutputStream processLog = null;

    /*
     * QuorumCommit.
     */

    @Override
    public void abort2Phase(final long token) throws IOException,
            InterruptedException {

        commitImpl.abort2Phase(token);

    }

    @Override
    public void commit2Phase(final long token, final long commitTime)
            throws IOException, InterruptedException {

        commitImpl.commit2Phase(token, commitTime);

    }

    @Override
    public int prepare2Phase(final boolean isRootBlock0,
            final IRootBlockView rootBlock, final long timeout,
            final TimeUnit unit) throws InterruptedException, TimeoutException,
            IOException {

        return commitImpl.prepare2Phase(isRootBlock0, rootBlock, timeout, unit);

    }

    @Override
    final public long getLastCommitTime() {

        final L localService = getLocalService();

        return localService.getRootBlockView().getLastCommitTime();
        
    }
    
    @Override
    final public long getLastCommitCounter() {

        final L localService = getLocalService();

        return localService.getRootBlockView().getCommitCounter();
        
    }

    @Override
    final public File getHALogDir() {

        return getLocalService().getHALogDir();
        
    }
    
    /*
     * QuorumRead
     */

    @Override
    public byte[] readFromQuorum(UUID storeId, long addr)
            throws InterruptedException, IOException {

        return readImpl.readFromQuorum(storeId, addr);

    }

}
