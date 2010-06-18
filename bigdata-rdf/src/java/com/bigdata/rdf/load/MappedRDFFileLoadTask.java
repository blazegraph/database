package com.bigdata.rdf.load;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.load.MappedRDFDataLoadMaster.JobState;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AsynchronousStatementBufferFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.AbstractAsynchronousClientTask;
import com.bigdata.service.jini.master.ClientLocator;
import com.bigdata.service.jini.master.INotifyOutcome;

/**
 * Task reads files from the file system, loads them into an
 * {@link ITripleStore}, and optionally deletes the source files once they are
 * restart safe on the database. This is a non-transactional bulk load using
 * unisolated writes. This task may be used in conjunction with any process that
 * writes files into a known directory on the hosts of a cluster.
 * <p>
 * Note: Counters reporting the progress of this task will be attached to the
 * service within which this task is executing.
 * <p>
 * Note: When loading files from a <em>local</em> file system, this task can
 * not handle the death of the service on which it is running.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic for the {@link JobState}.
 * @param <V>
 *            The generic type of the client state (stored in zookeeper).
 */
public class MappedRDFFileLoadTask<//
S extends JobState,//
V extends Serializable,//
L extends ClientLocator//
> extends AbstractAsynchronousClientTask<Void,V,L>//
implements Serializable {

    final protected transient static Logger log = Logger
            .getLogger(MappedRDFFileLoadTask.class);

    /**
     * 
     */
    private static final long serialVersionUID = 6787939197771556658L;

    protected final S jobState;

    protected final L locator;
    
    /**
     * Instantiated by {@link #call()} on the {@link IRemoteExecutor} service.
     */
    private transient AsynchronousStatementBufferFactory<BigdataStatement,V> statementBufferFactory;

    /**
     * Note: transient field set by {@link #call()}. 
     */
    private transient ReentrantLock lock;

    /**
     * Condition signaled when done.
     * <p>
     * Note: transient field set by {@link #call()}.
     */
    private transient Condition allDone;
    
    /**
     * Condition signaled when ready.
     * <p>
     * Note: transient field set by {@link #call()}.
     */
    private transient Condition ready;
    
    /**
     * Flag set once {@link #call()} has initialized our transient state.
     * <p>
     * Note: this flag is serialized so it is <code>false</code> before the
     * methods on this class can be invoked using RMI, especially
     * {@link #accept(Serializable[])}.
     */
    private volatile boolean isReady = false;
    
    public String toString() {

        return getClass().getName() + "{clientNum=" + locator + "}";

    }

    public MappedRDFFileLoadTask(final S jobState,
            final INotifyOutcome<V, L> notifyProxy, final L locator) {

        super(notifyProxy);

        if (locator == null)
            throw new IllegalArgumentException();

        this.jobState = jobState;

        this.locator = locator;

    }

    /**
     * The federation object used by the {@link IRemoteExecutor} on which this
     * task is executing.
     */
    public JiniFederation<?> getFederation() {

        return (JiniFederation<?>) super.getFederation();

    }

    protected void setUp() throws InterruptedException {

        // set transient fields.
        lock = new ReentrantLock();
        allDone = lock.newCondition();
        ready = lock.newCondition();

        lock.lockInterruptibly();
        try {

            final AbstractTripleStore tripleStore = (AbstractTripleStore) getFederation()
                    .getResourceLocator().locate(jobState.namespace,
                            ITx.UNISOLATED);

            if (tripleStore == null) {

                throw new RuntimeException("KB not found: namespace="
                        + jobState.namespace);

            }

            statementBufferFactory = new AsynchronousStatementBufferFactory<BigdataStatement, V>(
                    (ScaleOutTripleStore) tripleStore,//
                    jobState.producerChunkSize,//
                    jobState.valuesInitialCapacity,//
                    jobState.bnodesInitialCapacity,//
                    jobState.getFallbackRDFFormat(), // 
                    jobState.parserOptions,//
                    false, // deleteAfter is handled by the master!
                    jobState.parserPoolSize, //  
                    jobState.parserQueueCapacity, // 
                    jobState.term2IdWriterPoolSize,//
                    jobState.otherWriterPoolSize,//
                    jobState.notifyPoolSize,//
                    jobState.unbufferedStatementThreshold//
            ) {

                /*
                 * Override the "notifyService" to do asynchronous RMI back to
                 * this class indicating success or failure for each resource.
                 * 
                 * Note: It is very important to log ANY errors thrown back from
                 * the master!
                 */
                @Override
                protected Runnable newSuccessTask(final V resource) {
                    return new Runnable() {
                        public void run() {
                            try {
                                getNotifyProxy().success(resource, locator);
                            } catch (Throwable ex) {
                                log.error(resource, ex);
                            }
                        }
                    };
                }

                @Override
                protected Runnable newFailureTask(final V resource,
                        final Throwable cause) {
                    return new Runnable() {
                        public void run() {
                            try {
                                getNotifyProxy()
                                        .error(resource, locator, cause);
                            } catch (Throwable ex) {
                                log.error(resource, ex);
                            }
                        }
                    };
                }

            };

            /*
             * Update the flag and notify all blocked threads since they can now
             * execute.
             */
            isReady = true;

            ready.signalAll();

        } finally {

            lock.unlock();
            
        }
        
    }
    
    public Void call() throws Exception {

        setUp();
        
        try {

            /*
             * Add the counters to be reported to the client's counter set. The
             * added counters will be reported when the client reports its own
             * counters.
             */
            {

                final CounterSet serviceRoot = getFederation()
                        .getServiceCounterSet();

                final String relPath = jobState.jobName;

                // Create path to counter set.
                final CounterSet tmp = serviceRoot.makePath(relPath);

                // Attach counters.
                tmp
                        .attach(statementBufferFactory.getCounters(), true/* replace */);
            }
            
            /*
             * Wait until either (a) interrupted by the master using
             * Future#cancel(); or (b) the master invokes close(), indicating
             * that no more work items will be tasked to the client.
             */
            lock.lockInterruptibly();
            try {
                try {
                    allDone.await();
                } catch (InterruptedException ex) {
                    if (log.isInfoEnabled())
                        log.info("Client will terminate.");
                }
            } finally {
                lock.unlock();
            }

        } finally {

            try {
                statementBufferFactory
                        .cancelAll(true/* mayInterruptIfRunning */);
            } catch (Throwable t2) {
                log.warn(this, t2);
            }

        }

        return null;

    }

//    /**
//     * This knows how to open {@link File}s and {@link URL}s, but it is
//     * not integrated into the {@link AsynchronousStatementBufferFactory}.
//     */
//    @Override
//    protected InputStream openResource(final V resource) throws IOException {
//
//        final InputStream is;
//
//        if (resource instanceof File) {
//
//            is = new FileInputStream((File) resource);
//
//        } else if (resource instanceof URL) {
//
//            is = ((URL) resource).openStream();
//
//        } else {
//
//            throw new UnsupportedOperationException();
//
//        }
//
//        return new BufferedInputStream(is);
//
//    }

    /**
     * Block until {@link #call()} has fully initialized the instance of this
     * class running on the {@link IRemoteExecutor}. This method should be used
     * to guard methods on this or derived classes which can be invoked by RMI
     * and which depend on {@link #setUp()}.
     */
    protected void awaitReady() throws InterruptedException {

        if (lock == null)
            throw new IllegalStateException();

        lock.lockInterruptibly();
        try {

            /*
             * We use a combination of the volatile [isReady] flag and the
             * [ready] Condition to ensure that the instance of this class
             * running on the remote executor service has been fully initialized
             * before we allow access to its methods other than call().
             */
            if (!isReady) {

                // wait until ready.
                ready.await();

            }

        } finally {

            lock.unlock();

        }

    }
    
    public void accept(final V[] chunk) throws RemoteException,
            InterruptedException {

        awaitReady();

        for (V resource : chunk) {

            try {
                
                /*
                 * Try to submit the resource for processing.
                 */
                
                statementBufferFactory.submitOne(resource,
                        jobState.rejectedExecutionDelay);
                
            } catch (InterruptedException ex) {
                
                /*
                 * The client was interrupted.
                 */
                
                throw ex;
                
            } catch (Exception ex) {
                
                /*
                 * The client was not able to process this resource.
                 */
                
                getNotifyProxy().error(resource, locator, ex);
                
            }

        }

    }

    public void close() throws RemoteException, InterruptedException {

        awaitReady();
        
        lock.lockInterruptibly();
        try {

            statementBufferFactory.awaitAll();

            if (log.isInfoEnabled())
                log.info("Done.");

            /*
             * Signal in case master did not interrupt the main thread in
             * call().
             */
            allDone.signal();
            
        } catch (ExecutionException ex) {

            throw new RuntimeException(ex);
            
        } finally {

            lock.unlock();
            
        }

    }

}
