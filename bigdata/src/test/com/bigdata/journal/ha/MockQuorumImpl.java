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

    public void readFromQuorum(long addr, ByteBuffer b) {
        // TODO Auto-generated method stub

    }

    public void truncate(long extent) {
        // TODO Auto-generated method stub
    }

    public int prepare2Phase(IRootBlockView rootBlock, long timeout,
            TimeUnit unit) throws InterruptedException, TimeoutException,
            IOException {
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
