package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.util.concurrent.ExecutionExceptions;

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
 */
public class MockQuorumImpl implements Quorum {

    static protected final Logger log = Logger.getLogger(MockQuorumImpl.class);
    
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

    /**
     * Cancel the requests on the remote services (RMI). Any RMI related errors
     * are trapped.
     */
    private <T> void cancelRemoteFutures(final List<RunnableFuture<T>> remoteFutures) {

        for (RunnableFuture<T> rf : remoteFutures) {
        
            try {

                rf.cancel(true/* mayInterruptIfRunning */);
                
            } catch (Throwable t) {
                
                // ignored (to be robust).
                
            }
            
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation runs the operation on the master in the caller's
     * thread to avoid deadlock. The other services run the operation
     * asynchronously on their side while the master awaits their future's using
     * get().
     */
    public int prepare2Phase(final IRootBlockView rootBlock,
            final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException, IOException {

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the master. This will allow the other
         * services to prepare concurrently with the master's IO.
         */
        
        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;
        assertMaster();
        
        int nyes = 0;

        final List<RunnableFuture<Boolean>> remoteFutures = new LinkedList<RunnableFuture<Boolean>>();

        /*
         * For services (other than the master) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        for (int i = 1; i < stores.length; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Boolean> rf = getHAGlue(i).prepare2Phase(
                    rootBlock);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the master's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            stores[0/* master */].getExecutorService().submit(rf);

        }

        {
            /*
             * Run the operation on the master using local method call in the
             * caller's thread to avoid deadlock.
             * 
             * Note: Because we are running this in the caller's thread on the
             * master the timeout will be ignored for the master.
             */
            final RunnableFuture<Boolean> f = getHAGlue(0/* master */)
                    .prepare2Phase(rootBlock);
            // Note: This runs synchronously (ignores timeout).
            f.run();
            try {
                remaining = nanos - (begin - System.nanoTime());
                nyes += f.get(remaining, TimeUnit.NANOSECONDS) ? 1 : 0;
            } catch (ExecutionException e) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Error on the master. 
                throw new RuntimeException(e);
            } finally {
                f.cancel(true/* mayInterruptIfRunning */);
            }
        }

        /*
         * Check the futures for the other services in the quorum.
         */
        for (Future<Boolean> rf : remoteFutures) {
            boolean done = false;
            try {
                remaining = nanos - (begin - System.nanoTime());
                nyes += rf.get(remaining, TimeUnit.NANOSECONDS) ? 1 : 0;
                done = true;
            } catch (ExecutionException ex) {
                log.error(ex, ex);
            } finally {
                if (!done) {
                    // Cancel the request on the remote service (RMI).
                    try {
                        rf.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        // ignored.
                    }
                }
            }
        }

        if (nyes < (replicationFactor() + 1) >> 1) {

            log.error("prepare rejected: nyes=" + nyes + " out of "
                    + replicationFactor());
            
        }

        return nyes;

    }

    public void commit2Phase(final long commitTime) throws IOException, InterruptedException {

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the master. This will allow the other
         * services to commit concurrently with the master's IO.
         */

        assertMaster();

        final List<RunnableFuture<Void>> remoteFutures = new LinkedList<RunnableFuture<Void>>();

        /*
         * For services (other than the master) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        for (int i = 1; i < stores.length; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Void> rf = getHAGlue(i).commit2Phase(
                    commitTime);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the master's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            stores[0/* master */].getExecutorService().submit(rf);

        }

        {
            /*
             * Run the operation on the master using local method call in the
             * caller's thread to avoid deadlock.
             */
            final RunnableFuture<Void> f = getHAGlue(0/* master */)
                    .commit2Phase(commitTime);
            // Note: This runs synchronously (ignores timeout).
            f.run();
            try {
                f.get();
            } catch (ExecutionException e) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Error on the master.
                throw new RuntimeException(e);
            } finally {
                f.cancel(true/* mayInterruptIfRunning */);
            }
        }

        /*
         * Check the futures for the other services in the quorum.
         */
        final List<Throwable> causes = new LinkedList<Throwable>();
        for (Future<Void> rf : remoteFutures) {
            boolean done = false;
            try {
                rf.get();
                done = true;
            } catch (InterruptedException ex) {
                log.error(ex, ex);
                causes.add(ex);
            } catch (ExecutionException ex) {
                log.error(ex, ex);
                causes.add(ex);
            } finally {
                if (!done) {
                    // Cancel the request on the remote service (RMI).
                    try {
                        rf.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        // ignored.
                    }
                }
            }
        }

        /*
         * If there were any errors, then throw an exception listing them.
         */
        if (causes.isEmpty()) {
            // Cancel remote futures.
            cancelRemoteFutures(remoteFutures);
            // Throw exception back to the master.
            throw new RuntimeException("remote errors: nfailures="
                    + causes.size(), new ExecutionExceptions(causes));
        }

    }
    
    public void abort2Phase() throws IOException, InterruptedException {
    
        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the master. This will allow the other
         * services to commit concurrently with the master's IO.
         */

        assertMaster();

        final long token = token();
        
        final List<RunnableFuture<Void>> remoteFutures = new LinkedList<RunnableFuture<Void>>();

        /*
         * For services (other than the master) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        for (int i = 1; i < stores.length; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Void> rf = getHAGlue(i).abort2Phase(token);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the master's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            stores[0/* master */].getExecutorService().submit(rf);

        }

        {
            /*
             * Run the operation on the master using local method call in the
             * caller's thread to avoid deadlock.
             */
            final RunnableFuture<Void> f = getHAGlue(0/* master */)
                    .abort2Phase(token);
            // Note: This runs synchronously (ignores timeout).
            f.run();
            try {
                f.get();
            } catch (ExecutionException e) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Error on the master.
                throw new RuntimeException(e);
            } finally {
                f.cancel(true/* mayInterruptIfRunning */);
            }
        }

        /*
         * Check the futures for the other services in the quorum.
         */
        final List<Throwable> causes = new LinkedList<Throwable>();
        for (Future<Void> rf : remoteFutures) {
            boolean done = false;
            try {
                rf.get();
                done = true;
            } catch (InterruptedException ex) {
                log.error(ex, ex);
                causes.add(ex);
            } catch (ExecutionException ex) {
                log.error(ex, ex);
                causes.add(ex);
            } finally {
                if (!done) {
                    // Cancel the request on the remote service (RMI).
                    try {
                        rf.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        // ignored.
                    }
                }
            }
        }

        /*
         * If there were any errors, then throw an exception listing them.
         */
        if (causes.isEmpty()) {
            // Cancel remote futures.
            cancelRemoteFutures(remoteFutures);
            // Throw exception back to the master.
            throw new RuntimeException("remote errors: nfailures="
                    + causes.size(), new ExecutionExceptions(causes));
        }

    }

}
