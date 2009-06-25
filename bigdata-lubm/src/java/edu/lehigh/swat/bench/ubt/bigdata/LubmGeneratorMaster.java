/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 16, 2009
 */

package edu.lehigh.swat.bench.ubt.bigdata;

import java.io.File;
import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.bigdata.rdf.load.AbstractRDFTaskFactory;
import com.bigdata.rdf.load.ConcurrentDataLoader;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.load.RDFDataLoadMaster;
import com.bigdata.rdf.load.RDFFileLoadTask;
import com.bigdata.rdf.rio.IAsynchronousWriteStatementBufferFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.AbstractClientTask;
import com.bigdata.service.jini.master.TaskMaster;

import edu.lehigh.swat.bench.uba.Generator;

/**
 * Modified LUBM test harness generates the data dynamically using a set of
 * distributed processes and loads the data into an {@link ITripleStore} as it
 * goes.
 * <p>
 * There are two client tasks: one runs the LUBM generator and writes files into
 * the local file system. The other reads files from the local file system and
 * loads the data into an {@link ITripleStore}. The second task is designed to
 * be reusable to bulk load data from any source which can dump files into a
 * file system.
 * 
 * @todo exact statement counts for a large triple store are relatively
 *       expensive. look into ways to decrease that cost, e.g., more buffering
 *       of data and lazy decompression of the values for a leaf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LubmGeneratorMaster<S extends LubmGeneratorMaster.JobState, T extends Callable<U>, U>
        extends RDFDataLoadMaster<S, T, U> {

    final protected static Logger log = Logger
            .getLogger(LubmGeneratorMaster.class);

    // @todo consider also a GenerateThenLoad (sequential rather than parallel)
    static enum RunMode {
        Load,
        Generate,
        GenerateAndLoad
    }
    
    /**
     * {@link Configuration} options for the {@link LubmGeneratorMaster}.
     * <p>
     * Many of the of options defined here are for the original
     * {@link Generator}, but additional options have been defined for the
     * distributed data generator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see Generator
     */
    public interface ConfigurationOptions extends RDFDataLoadMaster.ConfigurationOptions {

        /**
         * number of universities to generate.
         */
        String UNIV_NUM = "univNum";

        /**
         * starting index of the universities (default is 0).
         */
        String START_INDEX = "startIndex";

        /**
         * seed used for random data generation (default is 0).
         */
        String SEED = "seed";

        /**
         * The url of the univ-bench ontology. The ontology does not need to be
         * accessible at that URL. It is just written into the generated data.
         */
        String ONTOLOGY_URL = "ontologyURL";

        /**
         * The {@link File} in the file system of the clients where the data
         * will be written. The {@link File} identifies a directory. The actual
         * directory has the form <code><i>outDir</i>/client#</code>, where
         * "#" is the client number. This makes it possible to place the
         * generated files onto a shared volume.
         * <p>
         * The directory will be created if it does not exist. Each generated
         * university will be written into a subdirectory of the output
         * directory for that client.
         */
        String OUT_DIR = "outDir";

        /**
         * The path to the directory where the LUBM log file will be written.
         * The actual log file has the form <code><i>logDir</i>/log#.txt</code>,
         * where "#" is the client number. This makes it possible to place the
         * log files onto a shared volume.
         * <p>
         * The LUBM generator log file can grow to be very large for scale-out
         * runs. Therefore, you may specify <code>/dev/null</code> as the output
         * file on platforms which support that device.
         */
        String LOG_DIR = "logDir";

        /**
         * The capacity of the queue used to feed the consumer. Use
         * {@link Integer#MAX_VALUE} ({@value Integer#MAX_VALUE}) if there is
         * no limit on the queue capacity.
         */
        String GENERATOR_QUEUE_CAPACITY = "generatorQueueCapacity";

        /**
         * This controls the behavior of the generator. You can either generate
         * an LUBM data set, writing files into the {@link #OUT_DIR}, load data
         * from the {@link #OUT_DIR}, or run both phases in parallel. Note that
         * the LUBM generator is single threaded. On a server class machine in a
         * cluster, the generator can not run fast enough to supply the parsers
         * or the database.
         * 
         * @see RunMode
         */
        String RUN_MODE = "runMode";
        
    }

    /**
     * State describing the job to be executed. The various properties are all
     * defined by {@link ConfigurationOptions}.
     */
    public static class JobState extends RDFDataLoadMaster.JobState {

        /**
         * 
         */
        private static final long serialVersionUID = -5035248794602794629L;

        /*
         * Properties defined by the original data generator.
         */

        /**
         * @see ConfigurationOptions#UNIV_NUM
         */
        final int univNum;

        /**
         * @see ConfigurationOptions#START_INDEX
         */
        final int startIndex;

        /**
         * @see ConfigurationOptions#SEED
         */
        final int seed;

        /**
         * @see ConfigurationOptions#ONTOLOGY_URL
         */
        final String ontologyURL;

        /**
         * @see ConfigurationOptions#OUT_DIR
         */
        final File outDir;

        /**
         * @see ConfigurationOptions#LOG_DIR
         */
        final File logDir;

        /**
         * The capacity of the queue used to feed the consumer.
         * 
         */
        final int generatorQueueCapacity;

        /**
         * @see ConfigurationOptions#RUN_MODE
         */
        final RunMode runMode;

        @Override
        protected void toString(StringBuilder sb) {
        
            sb.append(", " + ConfigurationOptions.UNIV_NUM + "="
                    + univNum);

            sb.append(", " + ConfigurationOptions.START_INDEX + "="
                    + startIndex);

            sb.append(", " + ConfigurationOptions.SEED + "="
                    + seed);

            sb.append(", " + ConfigurationOptions.ONTOLOGY_URL + "="
                    + ontologyURL);

            sb.append(", " + ConfigurationOptions.OUT_DIR + "="
                    + outDir);

            sb.append(", " + ConfigurationOptions.LOG_DIR + "="
                    + logDir);

            sb.append(", " + ConfigurationOptions.RUN_MODE + "="
                    + runMode);

        }
        
        public JobState(final String component, final Configuration config)
                throws ConfigurationException {

            super(component, config);

            /*
             * Properties defined by the original data generator.
             */

            univNum = (Integer) config.getEntry(component,
                    ConfigurationOptions.UNIV_NUM, Integer.TYPE);

            startIndex = (Integer) config.getEntry(component,
                    ConfigurationOptions.START_INDEX, Integer.TYPE, Integer
                            .valueOf(0));

            seed = (Integer) config
                    .getEntry(component, ConfigurationOptions.SEED,
                            Integer.TYPE, Integer.valueOf(0));

            ontologyURL = (String) config.getEntry(component,
                    ConfigurationOptions.ONTOLOGY_URL, String.class);

            /*
             * Additional properties for distributed data generation.
             */

            outDir = (File) config.getEntry(component,
                    ConfigurationOptions.OUT_DIR, File.class);

            logDir = (File) config.getEntry(component,
                    ConfigurationOptions.LOG_DIR, File.class);

            generatorQueueCapacity = (Integer) config
                    .getEntry(component,
                            ConfigurationOptions.GENERATOR_QUEUE_CAPACITY,
                            Integer.TYPE);

            runMode = RunMode.valueOf((String) config.getEntry(component,
                    ConfigurationOptions.RUN_MODE, String.class));

        }

    }

    /**
     * Runs the master. SIGTERM (normal kill or ^C) will cancel the job,
     * including any running clients.
     * 
     * @param args
     *            The {@link Configuration} and any overrides.
     * 
     * @throws ConfigurationException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws KeeperException
     */
    static public void main(final String[] args) throws ConfigurationException,
            ExecutionException, InterruptedException, KeeperException {

        final JiniFederation fed = new JiniClient(args).connect();

        final TaskMaster task = new LubmGeneratorMaster(fed);

        // execute master wait for it to finish.
        try {

            task.innerMain().get();
            
        } finally {
            
            // always write the date when the master terminates.
            System.err.println("Done: " + new Date());
            
        }
        
    }

    public LubmGeneratorMaster(final JiniFederation fed)
            throws ConfigurationException {

        super(fed);
        
    }

    /**
     * Creates either an {@link LubmGenerateClientTask} or an
     * {@link LubmGenerateAndLoadClientTask} depending on the
     * {@link JobState#generateOnly} flag.
     */
    @SuppressWarnings("unchecked")
    protected T newClientTask(final int clientNum) {

        final S jobState = getJobState();

        switch (jobState.runMode) {

        case Load:
            return (T) new LoadDataTask<S, ClientState>(jobState, clientNum);

        case Generate:
            return (T) new LubmGeneratorClientTask(jobState, clientNum);

        case GenerateAndLoad:
            // generate, writing on zqueue and file system, and bulk load data.
            return (T) new LubmGenerateAndLoadClientTask(jobState, clientNum);

        default:
            throw new AssertionError("Unknown mode: " + jobState.runMode);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected S newJobState(final String component, final Configuration config)
            throws ConfigurationException {

        return (S) new JobState(component, config);
        
    }
    
    /**
     * This class is written into the znode of the client. Its state tells the
     * client the next university to generate. When the client is done, its
     * state will be one more than the #of universities that were generated for
     * the entire job. E.g., 5001 for U5000.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ClientState implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -5081399318622375424L;

        /**
         * The index of the next university to be generated. When the job is
         * first created, this is the {@link JobState#startIndex}.
         */
        public int univIndex;

        public ClientState(final int univIndex) {

            this.univIndex = univIndex;

        }

        public String toString() {

            return "ClientState" + "{ univIndex=" + univIndex + "}";

        }

    }

    /**
     * A client running the LUBM generator.
     * <p>
     * You specify the #of universities, the #of clients, the namespace of the
     * target {@link ITripleStore}. Each client is assigned an integer in
     * [0:N-1] by the master. That integer is used to hash partition the
     * responsibility for generating the data. The master discovers joined
     * {@link IDataService}s and farms out the clients to run on those
     * services. Each client will generate 1/Nth of the Universities.
     * 
     * @todo [This logic has been commented out due to difficulties handling the
     *       errors when using a zlock and incomplete handling of client restart
     *       in any case.] Clients track the university whose data they are
     *       currently generating in a persistent znode. The client updates that
     *       znode each time another University worth of data has been generated
     *       and loaded into the target {@link ITripleStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param <S>
     *            The generic for the {@link JobState}.
     * @param <U>
     *            The generic for the {@link Callable}'s return type.
     * @param <V>
     *            The generic type of the client state (stored in zookeeper).
     */
    public static class LubmGeneratorClientTask<S extends LubmGeneratorMaster.JobState, Void/* U */, V extends ClientState>
            extends AbstractClientTask<S, Void/* U */, V> {

        /**
         * 
         */
        private static final long serialVersionUID = 1462301585073089799L;

        public LubmGeneratorClientTask(final S jobState, final int clientNum) {

            super(jobState, clientNum);
            
        }

        /**
         * Runs the generator.
         */
        @Override
        public Void call() throws Exception {

            if (log.isInfoEnabled())
                log.info("Running: client#=" + clientNum + ", " + jobState);

            runWithZLock(setupClientState());
            
//            boolean done = false;
//
//            while (!done) {
//
//                zlock = ZLockImpl.getLock(fed.getZookeeper(), jobState
//                        .getLockNodeZPath(fed, clientNum), acl);
//
//                zlock.lock();
//                try {
//
//                    runWithZLock();
//
//                    done = true;
//
//                } catch (SessionExpiredException ex) {
//
//                    /*
//                     * Log warning and then try to re-obtain the zlock so we can
//                     * finish the job.
//                     */
//
//                    log.warn(this + " : will seek zlock again", ex);
//                    
//                    continue;
//
//                } finally {
//
//                    zlock.unlock();
//
//                }
//
//            }

            if (log.isInfoEnabled())
                log.info("Finished: client#=" + clientNum + ", " + jobState);

            return null;

        }

        @Override// @todo compiler error under 64-bit linux jdk_1.6.0_07, but Ok under 1.6.0_12 on same host.
        protected Void/* U */runWithZLock(V clientState) throws Exception, KeeperException,
                InterruptedException {

            final File outDir = new File(jobState.outDir, "client" + clientNum);

            outDir.mkdirs();

            /*
             * @todo If we restart this client then we MUST bulk load any
             * pre-generated files in [outDir]. See RunnableFileSystemLoader.
             */
            
            jobState.logDir.mkdirs();

            final File logFile;
            if (jobState.logDir.getPath().equals("/dev/null")) {

                logFile = new File("/dev/null");
                
            } else {

                jobState.logDir.mkdirs();

                logFile = new File(jobState.logDir, "lubmGenerator" + clientNum
                        + ".log");
                
            }

            /*
             * Note: This task does not declare the IDataServiceAwareProcedure
             * interface. It can run on the normal executor service.
             */

            final Generator generator = new Generator();

            // run the generator.
            generator.start( //
                    jobState.univNum, // #of universities to generate
                    clientState.univIndex, // startIndex.
                    jobState.seed, //
                    false, // daml
                    jobState.ontologyURL, //
                    false, // subdirs
                    outDir, //
                    logFile, //
                    jobState.nclients,//
                    clientNum //
                    );

            return null;
            
        }

        @SuppressWarnings("unchecked")
        @Override
        protected V newClientState() {

            return (V) new ClientState(jobState.startIndex/* univIndex */);
            
        }

    }
    
    /**
     * Task which generates lubm data, writing onto a {@link BlockingQueue} and
     * runs another task on the same {@link DataService} that consumes the
     * {@link BlockingQueue} bulk loading the data into an {@link ITripleStore}.
     * The files MAY be located either on a shared volume or on local disk.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param <S>
     *            The generic for the {@link JobState}.
     * @param <V>
     *            The generic type of the client state (stored in zookeeper).
     * 
     * @todo if you restart the job you must also bulk load files remaining in
     *       the data directory for the client (this presumes that the
     *       [deleteAfter] flag was specified and that any files remaining in
     *       the data directory have therefore not been loaded successfully).
     */
    public static class LubmGenerateAndLoadClientTask<S extends LubmGeneratorMaster.JobState, Void/* U */, V extends ClientState>
            extends LubmGeneratorClientTask<S, Void/* U */, V> {

        /**
         * 
         */
        private static final long serialVersionUID = 447108800617047436L;
        
        /**
         * Queue containing the files to be consumed. The producer writes on the
         * queue. The consumer drains it.
         */
        protected final BlockingQueue<File> queue;

        /**
         * The consumer will exit once the queue is empty AND this flag is set.
         */
        protected volatile boolean producerDone = false;

        public LubmGenerateAndLoadClientTask(final S jobState,
                final int clientNum) {

            super(jobState, clientNum);

            this.queue = (jobState.generatorQueueCapacity == Integer.MAX_VALUE) //
                ? new LinkedBlockingQueue<File>()//
                : new LinkedBlockingQueue<File>(jobState.generatorQueueCapacity)//
                ;
            
        }

        @Override
        protected Void runWithZLock(final V clientState) throws KeeperException,
                InterruptedException, Exception {

            /*
             * Note: DO NOT run the base class method. It will generate ALL the
             * data (you will be running two generators).
             */
//            super.runWithZLock();

            if (log.isInfoEnabled())
                log.info("Running: " + clientState);

            final File outDir = new File(jobState.outDir, "client" + clientNum);

            outDir.mkdirs();

//            jobState.logDir.mkdirs();
//
//            final File logFile = new File(jobState.logDir, "lubmGenerator"
//                    + clientNum + ".log");

            final File logFile;
            if (jobState.logDir.getPath().equals("/dev/null")) {

                logFile = new File("/dev/null");
                
            } else {

                jobState.logDir.mkdirs();

                logFile = new File(jobState.logDir, "lubmGenerator" + clientNum
                        + ".log");
                
            }

            /*
             * Start the consumer.
             * 
             * Note: The task MUST be submitted to the local IRemoteExecutor
             * since it declares the IFederationCallable and is expecting to
             * have the federation reference set on itself before it runs. The
             * task MUST be submitted without using RMI since that would cause
             * the instance of this class to be serialized, which would break
             * the handshaking on the queue, etc.
             */
            final Future consumerFuture = ((IRemoteExecutor) getFederation()
                    .getService()).submit(new LoadDataFromQueueTask(this));

            /*
             * Start the producer.
             * 
             * Note: Since this task does not declare IFederationCallable it can
             * run on the normal executor service.
             */
            final Future producerFuture = getFederation().getExecutorService().submit(
                    new Callable<Void>() {

                        public Void call() throws Exception {

                            // create the generator.
                            final GeneratorWithQueue generator = new GeneratorWithQueue(queue,
                                    consumerFuture);

                            // run the generator.
                            generator.start( //
                                    jobState.univNum, // #of universities
                                    // to generate
                                    clientState.univIndex, // startIndex.
                                    jobState.seed, //
                                    false, // daml
                                    jobState.ontologyURL, //
                                    false, // subdirs
                                    outDir, //
                                    logFile, //
                                    jobState.nclients,//
                                    clientNum //
                                    );

                            // done.
                            return null;

                        }

                    });

            try {

                // block until the producer is finished.
                producerFuture.get();

                if (log.isInfoEnabled())
                    log.info("Producer is done.");

                /*
                 * The consumer will exit once the queue is empty AND this flag
                 * is set. The lock makes the consideration of the flag state
                 * and whether or not the queue is empty atomic.
                 */
                producerDone = true;

                /*
                 * Await the consumer. It will exit when its queued load tasks
                 * are complete.
                 */
                consumerFuture.get();

                if (log.isInfoEnabled())
                    log.info("Consumer is done.");

            } finally {

                if (!producerFuture.isDone()) {
                    log.warn("Cancelling producer.");
                    try {
                        // make sure the producer is finished.
                        producerFuture.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        log.warn(t);
                    }
                }

                if (!consumerFuture.isDone()) {
                    log.warn("Cancelling consumer.");
                    try {
                        // make sure the consumer is finished.
                        consumerFuture.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        log.warn(t);
                    }
                }

            }

            if (log.isInfoEnabled())
                log.info("Client is done.");

            return null;
            
        }

        /**
         * Concrete class with hooks to coordinate with the RDF bulk data loader
         * via zookeeper. The class writes the generated RDF files into the file
         * system and places {@link File}s telling the consumer where to get
         * the data onto a {@link BlockingQueue} for coordination with a
         * consumer.
         * <p>
         * Note: This can place either individual files (which correspond to
         * departments) or directories (which correspond to universities) onto
         * the queue. I am going with individual files because that means that I
         * do not wind up with a bunch of empty directories after a bulk data
         * load. This decision is reflected in how the files are named and in
         * which hook method is used to place the files onto the {@link #queue}.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        protected class GeneratorWithQueue extends Generator {

            /**
             * The queue on which the {@link File}s to be loaded are written.
             * Those {@link File}s identify files in the data directory for the
             * client which are to be loaded into the triple store.
             */
            final private BlockingQueue<File> queue;

            /**
             * The {@link Future} for the consumer task.
             */
            final private Future consumerFuture;
            
            /**
             * 
             * @param queue
             *            The queue on which the {@link File}s to be loaded are
             *            written. Those {@link File}s identify files in the
             *            data directory for the client which are to be loaded
             *            into the triple store.
             * @param consumerFuture
             *            The {@link Future} for the consumer task.
             */
            public GeneratorWithQueue(final BlockingQueue<File> queue,
                    final Future consumerFuture) {

                if (queue == null)
                    throw new IllegalArgumentException();

                this.queue = queue;
                
                this.consumerFuture = consumerFuture;
                
            }

            @Override
            protected void didGenerateUniv(final int univIndex, final File dir) {
                
                try {

                    if (log.isDebugEnabled())
                        log.debug("Generated: " + dir);

                    /*
                     * FIXME I have removed the logic which seeks and verifies
                     * the ZLock. Zookeeper kept dropping the ball and expiring
                     * the client' s session. Since there are actually two tasks
                     * running in the client (producer and loader) this made it
                     * more than just a little difficult to handle the expired
                     * session with the zlock on the outer class. In fact, I
                     * would have to abort and restart the producer and the
                     * consumer and handle any files which were already
                     * generated but not yet loaded. Instead, I have removed the
                     * ZLock logic. Implications are: (a) you can not restart
                     * this client; and (b) you must not run two masters for the
                     * same job.
                     * 
                     * I think that the best way to fix this is to make the
                     * client capable of restart (it would have to load any
                     * pregenerated files before generating new giles). The
                     * restart could be handled in the master, which would be
                     * slightly more robust.
                     */
//                    if (!zlock.isLockHeld())
//                        throw new InterruptedException("Lost ZLock");
//
//                    try {
//                        /*
//                         * Update the client state.
//                         * 
//                         * Note: We only update the client state after every
//                         * university that has been generated. If there is a
//                         * restart, then we will regenerate at most one
//                         * university per client.
//                         */
//                        fed.getZookeeper().setData(
//                                clientZPath,
//                                SerializerUtil.serialize(new ClientState(
//                                        univIndex + 1)), -1/* version */);
//                    } catch (ConnectionLossException ex) {
//                        /*
//                         * Note: There are a variety of transient errors which
//                         * can occur. We can ignore all of them. Next time we
//                         * are connected and we complete a university we will
//                         * update the client state, and that should be good
//                         * enough.
//                         */
//                        log.warn(ex);
//                    }

                } catch (Throwable t) {

                    throw new RuntimeException(t);

                }

            }

            @Override
            protected void didGenerateDept(final int univIndex,
                    final int deptIndex, final File file) {

                if (log.isDebugEnabled())
                    log.debug("Generated: " + file);

                if(consumerFuture.isDone()) {

                    /*
                     * Halt if the consumer dies.
                     */
                    try {

                        // get the outcome.
                        consumerFuture.get();

                        /*
                         * Note: We expect an exception if the consumer
                         * terminates before the producer interrupts it. If
                         * there is no exception then we throw an AssertionError
                         * since there is some other problem.
                         */
                        throw new AssertionError("Consumer died prematurely?");

                    } catch (Throwable t) {

                        /*
                         * Rethrow the problem from the consumer.
                         */
                        throw new RuntimeException("Consumer died.", t);
                        
                    }
                    
                }

                try {

                    /*
                     * Add to queue of departments (files) to be loaded.
                     * 
                     * Note: This will block until the queue can accept another
                     * element.
                     */
                    queue.put(file);

                } catch (InterruptedException ex) {

                    // masquerade and rethrow.
                    throw new RuntimeException(ex);

                }

            }

        }

    }

    /**
     * Glue class knows how to consume the files in a {@link BlockingQueue} on
     * which an {@link LubmGeneratorClientTask} is writing.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <S>
     *            The generic for the {@link JobState}.
     * @param <V>
     *            The generic type of the client state (stored in zookeeper).
     */
    public static class LoadDataFromQueueTask<S extends LubmGeneratorMaster.JobState, V extends LubmGeneratorMaster.ClientState>
            extends RDFFileLoadTask<S, V> {

        /**
         * 
         */
        private static final long serialVersionUID = 3311814427757555827L;

        final LubmGenerateAndLoadClientTask<S, Void/*U*/, V> clientTask;
        
        /**
         * @param clientTask
         */
        public LoadDataFromQueueTask(
                final LubmGenerateAndLoadClientTask<S, Void/*U*/, V> clientTask) {

            super(clientTask.getJobState(), clientTask.getClientNum());

            this.clientTask = clientTask;

        }

        @Override
        protected void loadData(final ConcurrentDataLoader loader,
                final AbstractRDFTaskFactory taskFactory,
                final AbstractTripleStore tripleStore)
                throws InterruptedException, Exception {

            while (true) {

                final boolean isQueueEmpty = clientTask.queue.isEmpty();

                final IStatementBufferFactory bufferFactory = taskFactory.getBufferFactory();
                
                if (bufferFactory instanceof IAsynchronousWriteStatementBufferFactory) {

                    if (((IAsynchronousWriteStatementBufferFactory) bufferFactory)
                            .isAnyDone()) {
                    
                        /*
                         * This should report 'done' until we close the factory
                         * below and once we close the factory this loop should
                         * not be repeated.
                         */
                        throw new RuntimeException("Consumer is done?");
                        
                    }
                    
                }

                if (isQueueEmpty && clientTask.producerDone) {

                    /*
                     * Note: There are no more elements to be taken from the
                     * queue but there may be tasks still running in the
                     * ConcurrentDataLoader. Therefore we now wait until all of
                     * those tasks are complete.
                     */

                    if (log.isInfoEnabled())
                        log.info("Will await completion of loader tasks.");

                    loader.awaitCompletion(Long.MAX_VALUE, TimeUnit.SECONDS);

//                    if (bufferFactory instanceof IAsynchronousWriteBufferFactory) {
//
//                        /*
//                         * Now that there are no more tasks running in the CDL
//                         * we can close the buffer factory. This will close the
//                         * blocking buffers for the async writes, which will
//                         * allow the iterators to drain out anything left in the
//                         * buffers and then report that they are exhausted.
//                         */
//                        
//                        if (log.isInfoEnabled())
//                            log.info("Closing factory.");
//
//                        ((IAsynchronousWriteBufferFactory) bufferFactory)
//                                .close();
//                        
//                    }

                    if (log.isInfoEnabled())
                        log.info("Done.");

                    // done.
                    return;

                }

                if (isQueueEmpty) {

                    /*
                     * Pause for a bit if the queue is empty (polling).
                     * 
                     * Note: This will only occur during startup since the queue
                     * may initially be empty. Once [producerDone] is true, we
                     * will take the other code branch and await the completion
                     * of the tasks which have already been submitted to the
                     * loader.
                     */
                    Thread.sleep(10/* ms */);

                    continue;

                }

                // take the element from the queue.
                final File file = clientTask.queue.take();

                /*
                 * Submit a task which will load everything in the file or
                 * directory into the triple store. The task will run
                 * asynchronously. If the task fails then it will be retried up
                 * to a configured retry count before being placed into a failed
                 * state. Metadata about task outcomes is reported via counters
                 * to the load balancer.
                 */
                loader.submitTask(file.getPath(), taskFactory);

            }

        }

    }

    /**
     * Glue class knows how to consume the files from the local file system.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <S>
     *            The generic for the {@link JobState}.
     * @param <V>
     *            The generic type of the client state (stored in zookeeper).
     */
    public static class LoadDataTask<S extends LubmGeneratorMaster.JobState, V extends LubmGeneratorMaster.ClientState>
            extends RDFFileLoadTask<S, V> {

        /**
         * @param clientTask
         */
        public LoadDataTask(final S jobState, final int clientNum) {

            super(jobState, clientNum);

        }
        
    }

}
