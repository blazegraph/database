package com.bigdata.journal.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ha.HAReceiveService.IHAReceiveCallback;

/**
 * A mock {@link Quorum} used to configure a set of {@link Journal}s running in
 * the same JVM instance for HA unit tests without dynamic quorum events.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Factor a bunch of this stuff into an AbstractQuorum class which we can
 *       use for the real implementations as well. Add getLocalHAGlue() or
 *       getLocalService() to access the {@link HAGlue} or other interface for
 *       the local service w/o an RMI proxy. Add method to enumerate over the
 *       non-master {@link HAGlue} objects. Refactor the implementation to avoid
 *       the direct references to {@link #stores}.
 * 
 * @todo add assertOpen() stuff to check for invalidation?
 */
public class MockQuorumImpl extends AbstractQuorum {

    static protected final Logger log = Logger.getLogger(MockQuorumImpl.class);

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final int index;

    /**
     * Note: This is a {@link Journal}[] in the {@link MockQuorumImpl} rather
     * than an {@link HAGlue} since the {@link Journal}s are not yet initialized
     * when our constructor is called so we have to lazily obtain their
     * {@link HAGlue} interfaces.
     */
    private final Journal[] stores;

    private final HASendService sendService;
    private final HAReceiveService<HAWriteMessage> receiveService;
    private final ByteBuffer receiveAndReplicateBuffer;
    
    /**
     * 
     * @param index
     *            The index of this service in the failover chain. The service
     *            at index ZERO (0) is the master.
     * @param stores
     *            The failover chain.
     * 
     * FIXME move the initialization to init().
     */
    public MockQuorumImpl(final int index, final Journal[] stores) {

        if (index < 0)
            throw new IllegalArgumentException();

        if (stores == null)
            throw new IllegalArgumentException();

        if (index >= stores.length)
            throw new IllegalArgumentException();

        this.index = index;

        /*
         * Note: Copy the array reference since the journals in the array have
         * not yet been initialized so we can't copy the array values or reach
         * through into their HAGlue interfaces yet.
         * 
         * Note: AbstractHAJournalTestCase creates the journals in reverse
         * quorum order so we can do things like getHAGlue(index+1) here.
         */
        this.stores = stores;
        
        if(isLeader()) {

            sendService = new HASendService(getHAGlue(getIndex() + 1)
                    .getWritePipelineAddr());
            
            try {
                sendService.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            receiveService = null;

            receiveAndReplicateBuffer = null;
            
        } else {

            sendService = null;

            try {
                /*
                 * Acquire buffer from the pool.
                 */
                receiveAndReplicateBuffer = DirectBufferPool.INSTANCE.acquire();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }

            final InetSocketAddress addrSelf = getHAGlue(getIndex())
                    .getWritePipelineAddr();

            final InetSocketAddress addrNext = isLastInChain() ? null
                    : getHAGlue(getIndex() + 1).getWritePipelineAddr();

            receiveService = new HAReceiveService<HAWriteMessage>(addrSelf,
                    addrNext, new IHAReceiveCallback<HAWriteMessage>() {

                        public void callback(HAWriteMessage msg, ByteBuffer data)
                                throws Exception {
                            
                            handleReplicatedWrite(msg,data);
                            
                        }
                
                    });
            
            receiveService.start();

        }
        
    }

    @Override
    protected ByteBuffer getReceiveBuffer() {

        return receiveAndReplicateBuffer;
        
    }

    public void invalidate() {
        
        if (!open.compareAndSet(true/* expect */, false/* update */))
            return;
        
        if(isLeader()) {
        
            sendService.terminate();
        
        } else {

            receiveService.terminate();

            try {
                /*
                 * Await shutdown so we can be positive that no thread can
                 * acquire() the direct buffer while the receive service might
                 * still be writing on it!
                 */
                receiveService.awaitShutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (receiveAndReplicateBuffer != null) {
                try {
                    /*
                     * Release the buffer back to the pool.
                     */
                    DirectBufferPool.INSTANCE
                            .release(receiveAndReplicateBuffer);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

        }
        
    }
    
    /** A fixed token value of ZERO (0L). */
    public long token() {
        return 0;
    }

    public int replicationFactor() {
        return stores.length;
    }

    public int size() {
        return stores.length;
    }

    /** assumed true. */
    public boolean isQuorumMet() {
        return true;
    }

    public boolean isLeader() {
        return index == 0;
    }

    public boolean isLastInChain() {
        return index == stores.length - 1;
    }

    public int getIndex() {
        return index;
    }
    
    public HAGlue getHAGlue(final int index) {

        if (index < 0 || index >= replicationFactor())
            throw new IndexOutOfBoundsException();

        if (stores[index] == null)
            throw new AssertionError("Journal not initialized: index=" + index);

        return stores[index].getHAGlue();

    }
    
    public HADelegate getHADelegate() {

        if (index < 0 || index >= replicationFactor())
            throw new IndexOutOfBoundsException();

        if (stores[index] == null)
            throw new AssertionError("Journal not initialized: index=" + index);

        return stores[index].getHADelegate();

    }

    public HAReceiveService<HAWriteMessage> getHAReceiveService() {

        assertFollower();
        
        return receiveService;
        
    }

    public HASendService getHASendService() {
        
        assertLeader();
        
        return sendService;
        
    }

    /**
     * Tunnel through and get the local journal.
     */
    private AbstractJournal getLocalJournal() {
        
        return ((HADelegate) getHADelegate()).getLiveJournal();

    }

    /**
     * Core implementation causes the data to get written onto the local
     * persistence store.
     * 
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param data
     *            The buffer containing the data.
     * @throws Exception
     */
    private void handleReplicatedWrite(final HAWriteMessage msg,
            final ByteBuffer data) throws Exception {

        ((IHABufferStrategy) getLocalJournal().getBufferStrategy())
                .writeRawBuffer(msg, data);

    }

    public ExecutorService getExecutorService() {

        return getLocalJournal().getExecutorService();

    }

}
