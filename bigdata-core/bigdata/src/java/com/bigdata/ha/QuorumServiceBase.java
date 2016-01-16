/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.AbstractQuorumMember;
import com.bigdata.service.proxy.ThickFuture;
import com.bigdata.util.InnerCause;

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
            protected void handleReplicatedWrite(final IHASyncRequest req,
                    final IHAWriteMessage msg, final ByteBuffer data)
                    throws Exception {

                QuorumServiceBase.this.handleReplicatedWrite(req, msg, data);

            }
            
            @Override
            protected void incReceive(final IHASyncRequest req,
                    final IHAWriteMessage msg, final int nreads,
                    final int rdlen, final int rem) throws Exception {

                QuorumServiceBase.this.incReceive(req, msg, nreads, rdlen, rem);

            }
            
            @Override
            protected long getRetrySendTimeoutNanos() {

                return QuorumServiceBase.this.getRetrySendTimeoutNanos();
                
            }
            
            @Override
            public UUID getStoreUUID() {

                return QuorumServiceBase.this.getStoreUUID();
                
            }
        
            @Override
            public long getLastCommitTime() {

                return QuorumServiceBase.this.getLastCommitTime();
                
            }
        
            @Override
            public long getLastCommitCounter() {

                return QuorumServiceBase.this.getLastCommitCounter();
                
            }

            @Override
            public void logWriteCacheBlock(final IHAWriteMessage msg,
                    final ByteBuffer data) throws IOException {

                QuorumServiceBase.this.logWriteCacheBlock(msg, data);
                
            }
            
            @Override
            public void logRootBlock(//final boolean isJoinedService,
                    final IRootBlockView rootBlock) throws IOException {

                QuorumServiceBase.this.logRootBlock(/*isJoinedService,*/ rootBlock);

            }

            @Override
            public void purgeHALogs(final long token) {

                QuorumServiceBase.this.purgeHALogs(token);

            }

        });

        addListener(this.commitImpl = new QuorumCommitImpl<S>(this));

        addListener(this.readImpl = new QuorumReadImpl<S>(this));
        
    }
    
    abstract protected long getRetrySendTimeoutNanos();

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
    public ExecutorService getExecutor() {

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
    public Future<Void> receiveAndReplicate(final IHASyncRequest req,
            final IHASendState snd, final IHAWriteMessage msg)
            throws IOException {

        return pipelineImpl.receiveAndReplicate(req, snd, msg);
        
    }

    @Override
    public Future<Void> replicate(final IHASyncRequest req,
            final IHAWriteMessage msg, final ByteBuffer b) throws IOException {

        return pipelineImpl.replicate(req, msg, b);

    }

    @Override
    public Future<IHAPipelineResetResponse> resetPipeline(
            IHAPipelineResetRequest req) throws IOException {

        return pipelineImpl.resetPipeline(req);

    }

    /**
     * Core implementation handles the message and payload when received on a
     * service.
     * <p>
     * Note: Replication of the message and payload is handled by the caller.
     * The implementation of this method is NOT responsible for replication.
     * 
     * @param req
     *            The synchronization request (optional). When non-
     *            <code>null</code> the message and payload are historical data.
     *            When <code>null</code> they are live data.
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param data
     *            The buffer containing the data.
     * 
     * @throws Exception
     * 
     * @see QuorumPipelineImpl#handleReplicatedWrite(IHAWriteMessage,
     *      ByteBuffer)
     */
    abstract protected void handleReplicatedWrite(IHASyncRequest req,
            IHAWriteMessage msg, ByteBuffer data) throws Exception;

    /**
     * Core implementation of callback for monitoring progress of replicated
     * writes.
     * 
     * @param req
     *            The synchronization request (optional). When non-
     *            <code>null</code> the message and payload are historical data.
     *            When <code>null</code> they are live data.
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param rdlen
     *            The number of bytes read from the socket in this read.
     * @param rem
     *            The number of bytes remaining before the payload has been
     *            fully read.
     *            
     * @throws Exception
     */
    abstract protected void incReceive(final IHASyncRequest req,
            final IHAWriteMessage msg, final int nreads, final int rdlen,
            final int rem) throws Exception;

        /**
     * {@inheritDoc}
     * <p>
     * Note: The default implementation is a NOP.
     */
    @Override
    public void logWriteCacheBlock(final IHAWriteMessage msg,
            final ByteBuffer data) throws IOException {

        // NOP
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The default implementation is a NOP.
     */
    @Override
    public void purgeHALogs(final long token) {
        
        // NOP
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: The default implementation is a NOP.
     */
    @Override
    public void logRootBlock(//final boolean isJoinedService,
            final IRootBlockView rootBlock) throws IOException {

        // NOP

    }

    /*
     * QuorumCommit.
     */

    @Override
    public void abort2Phase(final long token) throws IOException,
            InterruptedException {

        commitImpl.abort2Phase(token);

    }

    @Override
    public CommitResponse commit2Phase(final CommitRequest req) throws IOException,
            InterruptedException {

        return commitImpl.commit2Phase(req);

    }

    @Override
    public PrepareResponse prepare2Phase(final PrepareRequest req)
            throws InterruptedException, TimeoutException, IOException {

        return commitImpl.prepare2Phase(req);

    }

    @Override
    final public UUID getStoreUUID() {

        final L localService = getLocalService();

        return localService.getRootBlockView().getUUID();
        
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

//    @Override
//    final public File getHALogDir() {
//
//        return getLocalService().getHALogDir();
//        
//    }
    
    @Override
    public long getPrepareTimeout() {

        return getLocalService().getHAPrepareTimeout();
        
    }
    
    /*
     * QuorumRead
     */

    @Override
    public byte[] readFromQuorum(UUID storeId, long addr)
            throws InterruptedException, IOException {

        return readImpl.readFromQuorum(storeId, addr);

    }
    
    /**
     * Called from ErrorTask in HAJournalServer to ensure that events are
     * processed before entering SeekConsensus.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
     * HAJournalServer reports "follower" but is in SeekConsensus and is not
     * participating in commits</a>
     */
    protected void processEvents() {

        pipelineImpl.processEvents();
        
    }

    /**
     * Cancel the requests on the remote services (RMI). This is a best effort
     * implementation. Any RMI related errors are trapped and ignored in order
     * to be robust to failures in RMI when we try to cancel the futures.
     * <p>
     * NOte: This is not being done in parallel. However, due to a DGC thread
     * leak issue, we now use {@link ThickFuture}s. Thus, the tasks that are
     * being cancelled are all local tasks running on the
     * {@link #executorService}. If that local task is doing an RMI, then
     * cancelling it will cause an interrupt in the NIO request.
     */
    public static <F extends Future<T>, T> void cancelFutures(
            final List<F> futures) {

        if (log.isInfoEnabled())
            log.info("");

        for (F f : futures) {

            if (f == null) {

                continue;

            }

            try {

                if (!f.isDone()) {

                    f.cancel(true/* mayInterruptIfRunning */);

                }

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                    // Propagate interrupt.
                    Thread.currentThread().interrupt();

                }

                // ignored (to be robust).

            }

        }

    }

}
