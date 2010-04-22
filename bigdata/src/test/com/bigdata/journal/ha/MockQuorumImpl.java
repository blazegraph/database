package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    private final AbstractJournal[] failoverChain;

    /**
     * 
     * @param index
     *            The index of this service in the failover chain. The service
     *            at index ZERO (0) is the master.
     * @param failoverChain
     *            The failover chain.
     */
    public MockQuorumImpl(final int index, final AbstractJournal[] failoverChain) {

        this.index = index;

        this.failoverChain = failoverChain;

    }

    // public <T> void applyNext(RunnableFuture<T> r) {
    //
    // // must be invoked by the master (for this mock impl).
    // if (!isMaster())
    // throw new IllegalStateException();
    //
    // for(AbstractJournal j : failoverChain) {
    //            
    // j.getExecutorService().execute(r);
    //                
    // }
    //            
    // }

    /** A fixed token value of ZERO (0L). */
    public long token() {
        return 0;
    }

    public int replicationFactor() {
        return failoverChain.length;
    }

    public int size() {
        return failoverChain.length;
    }

    /** assumed true. */
    public boolean isQuorumMet() {
        return true;
    }

    public boolean isMaster() {
        return index == 0;
    }

    public void readFromQuorum(long addr, ByteBuffer b) {
        // TODO Auto-generated method stub

    }

    public void truncate(long extent) {
        // TODO Auto-generated method stub
    }

    public int prepare2Phase(long commitCounter, IRootBlockView rootBlock,
            long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    public void commit2Phase(long commitTime) throws IOException {
        // TODO Auto-generated method stub
        
    }

    public void abort2Phase() throws IOException {
        // TODO Auto-generated method stub
        
    }

}
