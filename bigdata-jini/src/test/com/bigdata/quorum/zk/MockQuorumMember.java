package com.bigdata.quorum.zk;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.AbstractQuorumMember;
import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * NOP client base class used for the individual clients for each
 * {@link MockQuorum} registered with of a shared {@link MockQuorumFixture} -
 * you can actually use any {@link QuorumMember} implementation you like with
 * the {@link MockQuorumFixture}, not just this one. The implementation you use
 * DOES NOT have to be derived from this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract class MockQuorumMember<S extends Remote> extends AbstractQuorumMember<S> {

    /**
     * The local implementation of the {@link Remote} interface.
     */
    private final S service;
    
    /**
     * Simple service registrar.
     */
    private final MockServiceRegistrar<S> registrar;
    
    /**
     * The last lastCommitTime value around which a consensus was achieved
     * and initially -1L, but this is cleared to -1L each time the consensus
     * is lost.
     */
    protected volatile long lastConsensusValue = -1L;

    /**
     * The downstream service in the write pipeline.
     */
    protected volatile UUID downStreamId = null;

    private volatile ExecutorService executorService = null;
    
    /**
     * @param quorum
     */
    protected MockQuorumMember(final String logicalServiceId,
            final MockServiceRegistrar<S> registrar) {

        super(logicalServiceId, UUID.randomUUID()/* serviceId */);
        
        this.service = newService();
        
        this.registrar = registrar;
        
    }

    @Override
    public void start(final Quorum<?, ?> quorum) {
        if (executorService == null)
            executorService = Executors
                    .newSingleThreadExecutor(DaemonThreadFactory
                            .defaultThreadFactory());
        super.start(quorum);
    }
    
    @Override
    public void terminate() {
        super.terminate();
        if(executorService!=null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }
    
    public Executor getExecutor() {
        return executorService;
    }

    /**
     * Factory for the local service implementation object.
     */
    abstract S newService();
    
    public S getService() {
        return service;
    }

    /**
     * Can not resolve services (this functionality is not required for the
     * unit tests in the <code>com.bigdata.quorum</code> package.
     */
    public S getService(UUID serviceId) {
        return registrar.get(serviceId);
    }

    /**
     * {@inheritDoc}
     * 
     * Overridden to save the <i>lastCommitTime</i> on
     * {@link #lastConsensusValue}.
     */
    @Override
    public void consensus(long lastCommitTime) {
        super.consensus(lastCommitTime);
        this.lastConsensusValue = lastCommitTime;
    }

    @Override
    public void lostConsensus() {
        super.lostConsensus();
        this.lastConsensusValue = -1L;
    }

    /**
     * {@inheritDoc}
     * 
     * Overridden to save the current downstream service {@link UUID} on
     * {@link #downStreamId}
     */
    public void pipelineChange(final UUID oldDownStreamId,
            final UUID newDownStreamId) {
        super.pipelineChange(oldDownStreamId, newDownStreamId);
        this.downStreamId = newDownStreamId;
    }

    /**
     * {@inheritDoc}
     * 
     * Overridden to clear the {@link #downStreamId}.
     */
    public void pipelineRemove() {
        super.pipelineRemove();
        this.downStreamId = null;
    }

    /**
     * Inner base class for service implementations provides access to the
     * {@link MockQuorumMember}.
     */
    protected class ServiceBase implements Remote {
        
    }
    
    /**
     * Mock service class.
     */
    class MockService extends ServiceBase implements HAPipelineGlue {

        final InetSocketAddress addrSelf;
        
        public MockService() throws IOException {
            this.addrSelf = new InetSocketAddress(getPort(0));
        }
        
        public InetSocketAddress getWritePipelineAddr() {
            return addrSelf;
        }

        /**
         * @todo This is not fully general purpose since it is not strictly
         *       forbidden that the service's lastCommitTime could change, e.g.,
         *       due to explicit intervention, and hence be updated across this
         *       operation. The real implemention should be a little more
         *       sophisticated.
         */
        public Future<Void> moveToEndOfPipeline() throws IOException {
            final FutureTask<Void> ft = new FutureTask<Void>(new Runnable() {
                public void run() {

                    // note the current vote (if any).
                    final Long lastCommitTime = getQuorum().getCastVote(
                            getServiceId());
                    
                    if (isPipelineMember()) {
                    
//                        System.err
//                                .println("Will remove self from the pipeline: "
//                                        + getServiceId());
                        
                        getActor().pipelineRemove();
                        
//                        System.err
//                                .println("Will add self back into the pipeline: "
//                                        + getServiceId());
                        
                        getActor().pipelineAdd();
                        
                        if (lastCommitTime != null) {
                        
//                            System.err
//                                    .println("Will cast our vote again: lastCommitTime="
//                                            + +lastCommitTime
//                                            + ", "
//                                            + getServiceId());
                            
                            getActor().castVote(lastCommitTime);
                            
                        }

                    }
                }
            }, null/* result */);
            getExecutor().execute(ft);
            return ft;
        }

        public Future<Void> receiveAndReplicate(HAWriteMessage msg)
                throws IOException {
            throw new UnsupportedOperationException();
        }
        
    }

    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    protected static int getPort(final int suggestedPort) throws IOException {
        
        ServerSocket openSocket;
        
        try {
        
            openSocket = new ServerSocket(suggestedPort);
            
        } catch (BindException ex) {
            
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        
        }

        final int port = openSocket.getLocalPort();
        
        openSocket.close();

        return port;
        
    }

}
