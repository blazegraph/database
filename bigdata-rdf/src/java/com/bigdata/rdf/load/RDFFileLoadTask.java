package com.bigdata.rdf.load;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.load.RDFDataLoadMaster.JobState;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.jini.JiniFederation;

/**
 * Task reads files from the file system, loads them into an
 * {@link ITripleStore}, and then deletes the files if they have been loaded
 * successfully. This is a non-transactional bulk load using unisolated writes.
 * If the writes succeed then the client knows that the data are on stable
 * storage and may safely delete the source files. This task may be used in
 * conjunction with any process that writes files into a known directory on the
 * hosts of a cluster. The task will continue until cancelled.
 * <p>
 * Note: Counters reporting the progress of this task will be attached to the
 * data service on which this task is executing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo report counters to federation. they will be attached to the
 *       {@link JiniFederation} of the {@link DataService}. they should be
 *       reported under the namespace of the job.
 */
public class RDFFileLoadTask implements Callable<Void>, Serializable,
        IDataServiceAwareProcedure {

    final protected static Logger log = Logger.getLogger(RDFFileLoadTask.class);

    final protected static boolean INFO = log.isInfoEnabled();

    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     */
    private static final long serialVersionUID = 6787939197771556658L;

    /**
     * The {@link DataService} on which the client is executing. This is set
     * automatically by the {@link DataService} when it notices that this
     * class implements the {@link IDataServiceAwareProcedure} interface.
     */
    private transient DataService dataService;

    protected final JobState jobState;

    protected final int clientNum;

    public String toString() {

        return getClass().getName() + "{clientNum=" + clientNum + "}";

    }

    public RDFFileLoadTask(final JobState jobState, final int clientNum) {

        this.jobState = jobState;

        this.clientNum = clientNum;

    }

    public void setDataService(final DataService dataService) {

        this.dataService = dataService;

    }

    /**
     * Return the {@link DataService} on which this task is executing.
     * 
     * @throws IllegalStateException
     *             if the task is not executing on a {@link DataService}.
     */
    public DataService getDataService() {

        if (dataService == null)
            throw new IllegalStateException();

        return dataService;

    }

    /**
     * The federation object used by the {@link DataService} on which this
     * task is executing.
     */
    public JiniFederation getFederation() {

        return (JiniFederation) getDataService().getFederation();

    }

    /**
     * {@link ConcurrentDataLoader}
     */
    public Void call() throws Exception {

        if (dataService == null)
            throw new IllegalStateException();

        final JiniFederation fed = (JiniFederation) dataService.getFederation();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) fed
                .getResourceLocator()
                .locate(jobState.namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("KB not found: namespace="
                    + jobState.namespace);

        }

        // @todo factory method.
        final RDFLoadTaskFactory taskFactory = new RDFLoadTaskFactory(
                tripleStore, jobState.bufferCapacity, jobState.parserValidates,
                jobState.deleteAfter, jobState.fallback);

        // Setup loader.
        final ConcurrentDataLoader loader = new ConcurrentDataLoader(fed,
                jobState.nthreads, jobState.queueCapacity,
                jobState.rejectedExecutionDelay, jobState.maxTries);

        try {

            /*
             * Note: Add the counters to be reported to the client's counter
             * set. The added counters will be reported when the client reports
             * its own counters.
             */
            final CounterSet serviceRoot = fed.getServiceCounterSet();

            final String relPath = jobState.jobName + ICounterSet.pathSeparator
                    + "Concurrent Data Loader";

            synchronized (serviceRoot) {

                if (serviceRoot.getPath(relPath) == null) {

                    // Create path to CDL counter set.
                    final CounterSet tmp = serviceRoot.makePath(relPath);

                    // Attach CDL counters.
                    tmp.attach(loader.getCounters());

                    // Attach task factory counters.
                    tmp.attach(taskFactory.getCounters());

                }

            }

            // Let the loader know that we will run tasks.
            taskFactory.notifyStart();

            // Load data.
            loadData(loader, taskFactory, tripleStore);

            // Done loading data (stops the clock).
            taskFactory.notifyEnd();

            // Shutdown the loader.
            loader.shutdown();

        } catch (Throwable t) {

            try {
                loader.shutdownNow();
            } catch (Throwable t2) {
                log.warn(this, t2);
            }

            throw new RuntimeException(t);
            
        }
        
        return null;

    }

    /**
     * Hook to load data is invoked once by {@link #call()}. The default
     * implementation uses {@link #loadDataFromFiles(AbstractTripleStore)}
     * to load any files found in the {@link JobState#dataDir}.
     * 
     * @param tripleStore
     *            Where to put the data.
     * 
     * @throws InterruptedException
     *             if interrupted (job cancelled).
     * @throws Exception
     *             if something else goes wrong.
     */
    protected void loadData(//
            final ConcurrentDataLoader loader,//
            final AbstractRDFTaskFactory taskFactory,//
            final AbstractTripleStore tripleStore//
    ) throws InterruptedException, Exception {

        loadDataFromFiles(loader, taskFactory, tripleStore);

    }

    /**
     * Starts a {@link RunnableFileSystemLoader} which loads data from the
     * {@link JobState#dataDir} and optionally deletes files after they have
     * been loaded successfully. This task is designed to run forever. It
     * SHOULD be interrupted in order to halt processing. You MUST specify
     * {@link JobState#deleteAfter} or it will continually re-load the same
     * data.
     * 
     * @throws InterruptedException
     * @throws Exception
     * 
     * @todo It would be better to set a flag on the CDL to halt processing
     *       so that we do not interrupt processing in the middle of a file
     *       load. That could also be accomplished by draining the CDL's
     *       input queue. Interrupt of the CDL should be reserved for a very
     *       long running data load task where you really need to abort the
     *       data load operation, e.g., when processing some extremely large
     *       file.
     */
    protected void loadDataFromFiles(final ConcurrentDataLoader loader,
            final AbstractRDFTaskFactory taskFactory,
            final AbstractTripleStore tripleStore) throws InterruptedException,
            Exception {

        // @todo config delay
        final long initialDelay = 0;
        final long delay = 5000;

        /*
         * Start a scheduled task. It will run until explicitly cancelled.
         */
        final ScheduledFuture f = getFederation().addScheduledTask(
                new RunnableFileSystemLoader(loader, taskFactory, tripleStore,
                        jobState.dataDir), initialDelay, delay,
                TimeUnit.MILLISECONDS);

        /*
         * The scheduled task should run forever so this blocks until this
         * thread is interruted.
         */
        f.get();

        // cancel the scheduled task.
        f.cancel(true/*mayInterruptIfRunning*/);

    }

    /**
     * {@link Runnable} class applies the factory to each file found in the
     * specified directory.
     */
    static protected class RunnableFileSystemLoader implements Runnable {

        volatile boolean done = false;

        final ConcurrentDataLoader loader;

        final AbstractRDFTaskFactory taskFactory;

        final AbstractTripleStore tripleStore;

        final File file;

        public RunnableFileSystemLoader(final ConcurrentDataLoader loader,
                final AbstractRDFTaskFactory taskFactory,
                final AbstractTripleStore tripleStore, final File file) {

            if (loader == null)
                throw new IllegalArgumentException();
            if (taskFactory == null)
                throw new IllegalArgumentException();
            if (tripleStore == null)
                throw new IllegalArgumentException();
            if (file == null)
                throw new IllegalArgumentException();

            this.loader = loader;

            this.taskFactory = taskFactory;

            this.tripleStore = tripleStore;

            this.file = file;

        }

        /**
         * Creates a task using the {@link #taskFactory}, submits it to the
         * {@link #loader} and and waits for the task to complete.
         * <p>
         * Errors are logged, but not thrown. This makes the task suitable
         * for use with a {@link ScheduledExecutorService}.
         * 
         * @throws RuntimeException
         *             if interrupted.
         */
        public void run() {

            try {

                if (INFO)
                    log.info("start: file=" + file);

                // loads everything in the file or directory.
                loader.submitTask(file.getPath(), taskFactory);

                // wait until the data have been loaded.
                loader.awaitCompletion(Long.MAX_VALUE, TimeUnit.SECONDS);

                if (INFO)
                    log.info("done : file=" + file);

            } catch (InterruptedException t) {

                // Note: This is normal termination.
                log.warn("Interrupted");

                throw new RuntimeException("Interrupted.");

            } catch (Throwable t) {

                // Catch and log all errors.
                log.error(file, t);

            }

        }

    }

}
