package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;

/**
 * A mock {@link Quorum} used to configure a set of {@link Journal}s running in
 * the same JVM instance for HA unit tests without dynamic quorum events. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockQuorumImpl implements Quorum {

    private final int index;
    private final Journal[] stores;

    /**
     * 
     * @param index
     *            The index of this service in the failover chain. The service
     *            at index ZERO (0) is the master.
     * @param stores
     *            The failover chain.
     */
    public MockQuorumImpl(final int index, final Journal[] stores) {

        this.index = index;

        this.stores = stores;

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

    public boolean isMaster() {
        return index == 0;
    }

    public HAGlue getHAGlue(int index) {

        return stores[index].getHAGlue();
        
    }

    public void readFromQuorum(long addr, ByteBuffer b) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    protected void assertMaster() {
        if (!isMaster())
            throw new IllegalStateException();
    }

//    public void truncate(final long extent) {
//        assertMaster();
//        for (int i = 0; i < stores.length; i++) {
//            Journal store = stores[i];
//            if (i == 0) {
//                /*
//                 * This is a NOP because the master handles this for its local
//                 * backing file and there are no other services in the singleton
//                 * quorum.
//                 */
//                continue;
//            }
//            try {
//                final RunnableFuture<Void> f = store.getHAGlue().truncate(
//                        token(), extent);
//                f.run();
//                f.get();
//            } catch (Throwable e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    public int prepare2Phase(final IRootBlockView rootBlock,
            final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException, IOException {
        assertMaster();
        try {
            final RunnableFuture<Boolean> f = haGlue
                    .prepare2Phase(rootBlock);
            /*
             * Note: In order to avoid a deadlock, this must run() on the
             * master in the caller's thread and use a local method call.
             * For the other services in the quorum it should submit the
             * RunnableFutures to an Executor and then await their outcomes.
             * To minimize latency, first submit the futures for the other
             * services and then do f.run() on the master. This will allow
             * the other services to prepare concurrently with the master's
             * IO.
             */
            f.run();
            return f.get(timeout, unit) ? 1 : 0;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public void commit2Phase(final long commitTime) throws IOException {
        assertMaster();
        try {
            final RunnableFuture<Void> f = haGlue.commit2Phase(commitTime);
            /*
             * Note: In order to avoid a deadlock, this must run() on the
             * master in the caller's thread and use a local method call.
             * For the other services in the quorum it should submit the
             * RunnableFutures to an Executor and then await their outcomes.
             * To minimize latency, first submit the futures for the other
             * services and then do f.run() on the master. This will allow
             * the other services to prepare concurrently with the master's
             * IO.
             */
            f.run();
            f.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public void abort2Phase() throws IOException {
        assertMaster();
        try {
            final RunnableFuture<Void> f = haGlue.abort2Phase(token());
            /*
             * Note: In order to avoid a deadlock, this must run() on the
             * master in the caller's thread and use a local method call.
             * For the other services in the quorum it should submit the
             * RunnableFutures to an Executor and then await their outcomes.
             * To minimize latency, first submit the futures for the other
             * services and then do f.run() on the master. This will allow
             * the other services to prepare concurrently with the master's
             * IO.
             */
            f.run();
            f.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
