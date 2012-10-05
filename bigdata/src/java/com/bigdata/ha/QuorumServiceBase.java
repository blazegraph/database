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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
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
            protected void handleReplicatedWrite(final IHAWriteMessage msg,
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

            @Override
            public void logWriteCacheBlock(final IHAWriteMessage msg,
                    final ByteBuffer data) throws IOException {

                QuorumServiceBase.this.logWriteCacheBlock(msg, data);
                
            }
            
            @Override
            public void logRootBlock(final IRootBlockView rootBlock)
                    throws IOException {

                QuorumServiceBase.this.logRootBlock(rootBlock);

            }

            @Override
            public void purgeHALogs() {

                QuorumServiceBase.this.purgeHALogs();

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
    public Future<Void> receiveAndReplicate(final IHAWriteMessage msg)
            throws IOException {
        
        return pipelineImpl.receiveAndReplicate(msg);
        
    }

    @Override
    public Future<Void> replicate(final IHAWriteMessage msg, final ByteBuffer b)
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
     * @see QuorumPipelineImpl#handleReplicatedWrite(IHAWriteMessage, ByteBuffer)
     */
    abstract protected void handleReplicatedWrite(IHAWriteMessage msg,
            ByteBuffer data) throws Exception;
 
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
    public void purgeHALogs() {
        
        // NOP
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: The default implementation is a NOP.
     */
    @Override
    public void logRootBlock(final IRootBlockView rootBlock) throws IOException {

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
    public void commit2Phase(
            final UUID[] joinedServiceIds, //
            final Set<UUID> nonJoinedPipelineServiceIds, final long token,
            final long commitTime) throws IOException, InterruptedException {

        commitImpl.commit2Phase(joinedServiceIds, nonJoinedPipelineServiceIds,
                token, commitTime);

    }

    @Override
    public int prepare2Phase(final UUID[] joinedServiceIds, //
            final Set<UUID> nonJoinedPipelineServiceIds,
            //final boolean isRootBlock0,
            final IRootBlockView rootBlock, final long timeout,
            final TimeUnit unit) throws InterruptedException, TimeoutException,
            IOException {

        return commitImpl.prepare2Phase(joinedServiceIds,
                nonJoinedPipelineServiceIds, /* isRootBlock0, */rootBlock,
                timeout, unit);

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

}
