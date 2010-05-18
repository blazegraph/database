package com.bigdata.journal.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * 
 * @todo add assertOpen() stuff to check for invalidation?
 */
public class MockQuorumImpl implements Quorum {

    static protected final Logger log = Logger.getLogger(MockQuorumImpl.class);

    private final AtomicBoolean open = new AtomicBoolean(true);
    private final int index;
    private final ExecutorService executorService;
    private final HAGlue[] stores;

    private final HASendService sendService;
    private final HAReceiveService<HAWriteMessage> receiveService;
    
    /**
     * 
     * @param index
     *            The index of this service in the failover chain. The service
     *            at index ZERO (0) is the master.
     * @param stores
     *            The failover chain.
     */
    public MockQuorumImpl(final int index, final Journal[] stores) {

        if (index < 0)
            throw new IllegalArgumentException();

        if (stores == null)
            throw new IllegalArgumentException();

        if (index >= stores.length)
            throw new IllegalArgumentException();

        this.index = index;

        this.executorService = stores[index].getExecutorService();
        
        this.stores = new HAGlue[stores.length];
        
        for (int i = 0; i < stores.length; i++) {

            this.stores[i] = stores[i].getHAGlue();

        }

        if(isMaster()) {

            sendService = new HASendService(getHAGlue(getIndex() + 1)
                    .getWritePipelineAddr());
            
            receiveService = null;

        } else {

            sendService = null;

            final InetSocketAddress addrSelf = getHAGlue(getIndex())
                    .getWritePipelineAddr();

            final InetSocketAddress addrNext = isLastInChain() ? null
                    : getHAGlue(getIndex() + 1).getWritePipelineAddr();

            receiveService = new HAReceiveService<HAWriteMessage>(addrSelf,
                    addrNext, callback);

        }
        
    }

    protected void finalize() throws Throwable {
        
        _close();
        
        super.finalize();
        
    }

    public void invalidate() {
        _close();
    }

    protected void _close() {
        
        if (!open.compareAndSet(true/* expect */, false/* update */))
            return;
        
        if(isMaster()) {
            sendService.terminate();
        } else {
            receiveService.terminate();
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

    public boolean isMaster() {
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
        
        return stores[index];
        
    }
    
    public ExecutorService getExecutorService() {
        
        return executorService;
        
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
    private <F extends Future<T>, T> void cancelRemoteFutures(
            final List<F> remoteFutures) {

        for (F rf : remoteFutures) {
        
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
        
//        // Copy the root block into a byte[].
//        final byte[] data;
//        {
//            final ByteBuffer rb = rootBlock.asReadOnlyBuffer();
//            data = new byte[rb.limit()];
//            rb.get(data);
//        }

        final List<RunnableFuture<Boolean>> remoteFutures = new LinkedList<RunnableFuture<Boolean>>();

        /*
         * For services (other than the master) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        for (int i = 1; i < stores.length; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Boolean> rf = getHAGlue(i).prepare2Phase(rootBlock);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the master's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutorService().submit(rf);

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
            getExecutorService().submit(rf);

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
            getExecutorService().submit(rf);

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

    /**
     * Handle a bad read from the local disk as identified by a checksum error
     * on the data in the record by reading on another member of the
     * {@link Quorum}.
     */
    public ByteBuffer readFromQuorum(final long addr)
            throws InterruptedException, IOException {
        
        if(replicationFactor()>1) {

            // This service is not configured for high availability. 
            throw new IllegalStateException();
            
        }
        
        // The quorum must be met, in which case there will be at least 1 other
        // node.
        if(!isQuorumMet()) {
            throw new IllegalStateException();
        }
        /*
         * Prefer to read on the previous service in the quorum order since it
         * will always have anything which has been written onto this service.
         * Otherwise, read on the downstream service in the quorum order. The
         * downstream node should also always have the record. Recent small
         * records will still be in cache (both on this node and the downstream
         * node) and thus should never have resulted in a read through to the
         * disk and a ChecksumError. Large records are written directly through
         * to the disk, but they are written through to the disk synchronously
         * so the downstream node will always have the large record on the disk
         * as well.
         */
        // The index of this service in the quorum.
        final int indexSelf = getIndex();
        
        final int indexOther = indexSelf > 0 ? indexSelf - 1 : indexSelf + 1;
        
        // The RMI interface to the node on which we will read.
        final HAGlue haGlue = getHAGlue(indexOther);

        /*
         * Read from that node.  The request runs in the caller's thread.
         */
        try {
            
            final RunnableFuture<ByteBuffer> rf = haGlue.readFromDisk(token(),
                    addr);
            
            rf.run();
            
            return rf.get();
            
        } catch (ExecutionException e) {
            
            throw new RuntimeException(e);
            
        }

    }

    public Future<Void> replicate(final HAWriteMessage msg, final ByteBuffer b)
            throws IOException {

        final int indexSelf = getIndex();

        final Future<Void> ft;

        if (isMaster()) {

            /*
             * This is the master, so send() the buffer.
             */
            
            ft = new FutureTask<Void>(new Callable<Void>() {

                public Void call() throws Exception {

                    // Get Future for send() outcome on local service.
                    final Future<Void> futSnd = getHASendService().send(b);

                    try {

                        // Get Future for receive outcome on the remote service.
                        final Future<Void> futRec = getHAGlue(indexSelf + 1)
                                .replicate(msg);

                        try {

                            /*
                             * Await the Futures, but spend more time waiting on
                             * the local Future and only check the remote Future
                             * every second. Timeouts are ignored during this
                             * loop.
                             */
                            while (!futSnd.isDone() && !futRec.isDone()) {
                                try {
                                    futSnd.get(1L, TimeUnit.SECONDS);
                                } catch (TimeoutException ignore) {
                                }
                                try {
                                    futRec.get(10L, TimeUnit.MILLISECONDS);
                                } catch (TimeoutException ignore) {
                                }
                            }
                            futSnd.get();
                            futRec.get();

                        } finally {
                            if (!futRec.isDone()) {
                                // cancel remote Future unless done.
                                futRec.cancel(true/* mayInterruptIfRunning */);
                            }
                        }

                    } finally {
                        // cancel the local Future.
                        futSnd.cancel(true/* mayInterruptIfRunning */);
                    }

                    // done
                    return null;
                }

            });

            // execute the FutureTask.
            getExecutorService().submit((FutureTask<Void>) ft);

        } else if (isLastInChain()) {

            /*
             * This is the last node in the write pipeline, so just receive the
             * buffer.
             * 
             * Note: The receive service is executing this Future locally on
             * this host.
             */
            
            try {
                ft = getHAReceiveService().receiveData(msg, b);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        } else {

            /*
             * A node in the middle of the write pipeline.
             */
            
            ft = new FutureTask<Void>(new Callable<Void>() {

                public Void call() throws Exception {

                    // Get Future for send() outcome on local service.
                    final Future<Void> futSnd = getHAReceiveService()
                            .receiveData(msg, b);

                    try {

                        // Get future for receive outcome on the remote service.
                        final Future<Void> futRec = getHAGlue(indexSelf + 1)
                                .replicate(msg);

                        try {

                            /*
                             * Await the Futures, but spend more time waiting on
                             * the local Future and only check the remote Future
                             * every second. Timeouts are ignored during this
                             * loop.
                             */
                            while (!futSnd.isDone() && !futRec.isDone()) {
                                try {
                                    futSnd.get(1L, TimeUnit.SECONDS);
                                } catch (TimeoutException ignore) {
                                }
                                try {
                                    futRec.get(10L, TimeUnit.MILLISECONDS);
                                } catch (TimeoutException ignore) {
                                }
                            }
                            futSnd.get();
                            futRec.get();
                            
                        } finally {
                            if (!futRec.isDone()) {
                                // cancel remote Future unless done.
                                futRec.cancel(true/* mayInterruptIfRunning */);
                            }
                        }
                        
                    } finally {
                        // cancel the local Future.
                        futSnd.cancel(true/* mayInterruptIfRunning */);
                    }

                    // done
                    return null;
                }

            });

            // execute the FutureTask.
            getExecutorService().submit((FutureTask<Void>) ft);

        }

        return ft;

    }

    public HAReceiveService<HAWriteMessage> getHAReceiveService() {

        if(isMaster())
            throw new UnsupportedOperationException();
        
        return receiveService;
        
    }

    public HASendService getHASendService() {
        
        if(!isMaster())
            throw new UnsupportedOperationException();
        
        return sendService;
        
    }
    
}
