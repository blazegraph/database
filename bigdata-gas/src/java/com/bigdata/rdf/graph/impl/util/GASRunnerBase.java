package com.bigdata.rdf.graph.impl.util;

import java.lang.reflect.Constructor;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.GASState;
import com.bigdata.rdf.graph.impl.GASStats;

/**
 * Base class for running performance tests.
 * 
 * @param <VS>
 *            The generic type for the per-vertex state. This is scoped to the
 *            computation of the {@link IGASProgram}.
 * @param <ES>
 *            The generic type for the per-edge state. This is scoped to the
 *            computation of the {@link IGASProgram}.
 * @param <ST>
 *            The generic type for the SUM. This is often directly related to
 *            the generic type for the per-edge state, but that is not always
 *            true. The SUM type is scoped to the GATHER + SUM operation (NOT
 *            the computation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Do we need a different driver if the algorithm always visits all
 *         vertices? For such algorithms, we just run them once per graph
 *         (unless the graph is dynamic).
 */
//* @param <GE>
//*            The generic type for the {@link IGASEngine}.
//* @param <BE>
//*            The generic type for the backend implementation.

public abstract class GASRunnerBase<VS, ES, ST> implements
        Callable<IGASStats> {

    private static final Logger log = Logger.getLogger(GASRunnerBase.class);
    
    /**
     * Configured options for the {@link GASRunner}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class OptionData {
        /**
         * The seed used for the random number generator (default {@value #seed}
         * ).
         */
        public long seed = 217L;
        /**
         * Random number generated used for sampling the starting vertices. Set
         * by #init().
         */
        public Random r = null;
        /**
         * The #of random starting vertices to use.
         */
        public int nsamples = 100;
        /**
         * The #of threads to use for GATHER and SCATTER operators.
         */
        public int nthreads = 4;
        /**
         * The analytic class to be executed.
         */
        public Class<IGASProgram<VS, ES, ST>> analyticClass;
        /**
         * The {@link IGASSchedulerImpl} class to use.
         * 
         * TODO Override or always? If always, then where to get the default?
         */
        public Class<IGASSchedulerImpl> schedulerClassOverride;
        
        /** Set of files to load (may be empty). */
        public final LinkedHashSet<String> loadSet = new LinkedHashSet<String>();
        
        /** The name of the implementation specific configuration file. */
        public String propertyFile; 

        protected OptionData() {
            
        }

        /**
         * Initialize any resources, including the connection to the backend.
         */
        public void init() throws Exception {

            // Setup the random number generator.
            this.r = new Random(seed);

            r = new Random(seed);
            
        }
        
        /**
         * Shutdown any resources, including the connection to the backend.
         * <p>
         * Note: This method must be safe. It may be called if {@link #init()}
         * fails. It may be called more than once.
         */
        public void shutdown() {
            
        }

        /**
         * Return <code>true</code>iff one or more arguments can be parsed
         * starting at the specified index.
         * 
         * @param i
         *            The index into the arguments.
         * @param args
         *            The arguments.
         * @return <code>true</code> iff any arguments were recognized.
         */
        public boolean handleArg(final AtomicInteger i, final String[] args) {

            return false;
            
        }
        
        /**
         * Print the optional message on stderr, print the usage information on
         * stderr, and then force the program to exit with the given status code.
         * 
         * @param status
         *            The status code.
         * @param msg
         *            The optional message
         */
        public void usage(final int status, final String msg) {

            if (msg != null) {

                System.err.println(msg);

            }

            System.err.println("[options] analyticClass propertyFile");

            System.exit(status);

        }

        /**
         * Extension hook for reporting at the end of the test run.
         * 
         * @param sb A buffer into which more information may be appended.
         */
        public void report(final StringBuilder sb) {
            
            // NOP
            
        }
        
    } // class OptionData

    /**
     * The configuration metadata for the run.
     */
    private final OptionData opt;
    
    /**
     * Factory for the {@link OptionData}.
     */
    abstract protected OptionData newOptionData();
    
    /**
     * The {@link OptionData} for the run.
     */
    protected OptionData getOptionData() {

        return opt;
        
    }

    /**
     * Factory for the {@link IGASEngine}.
     */
    abstract protected IGASEngine newGASEngine();
    
    /**
     * Load files into the backend if they can not be assumed to already exist
     * (a typical pattern is that files are loaded into an empty KB instance,
     * but not loaded into a pre-existing one).
     * 
     * @throws Exception
     */
    abstract protected void loadFiles() throws Exception;

    /**
     * Run a GAS analytic against some data set.
     * 
     * @param args
     *            USAGE:<br/>
     *            <code>(options) analyticClass propertyFile</code>
     *            <p>
     *            <i>Where:</i>
     *            <dl>
     *            <dt>propertyFile</dt>
     *            <dd>The implementation specific property file or other type of
     *            configuration file.</dd>
     *            </dl>
     *            and <i>options</i> are any of:
     *            <dl>
     *            <dt>-nthreads</dt>
     *            <dd>The #of threads which will be used for GATHER and SCATTER
     *            operations.</dd>
     *            <dt>-nsamples</dt>
     *            <dd>The #of random sample starting vertices that will be
     *            selected. The algorithm will be run ONCE for EACH sampled
     *            vertex.</dd>
     *            <dt>-seed</dt>
     *            <dd>The seed for the random number generator (default is
     *            <code>217L</code>).</dd>
     *            <dt>-schedulerClass</dt>
     *            <dd>Override the default {@link IGASScheduler}. Class must
     *            implement {@link IGASSchedulerImpl}.</dd>
     *            <dt>-load</dt>
     *            <dd>Loads the named resource IFF the KB is empty (or does not
     *            exist) at the time this utility is executed. This option may
     *            appear multiple times. The resources will be searched for as
     *            URLs, on the CLASSPATH, and in the file system.</dd>
     *            </p>
     * @throws ClassNotFoundException
     */
    public GASRunnerBase(final String[] args) throws ClassNotFoundException {

        final OptionData opt = newOptionData();
        
        /*
         * Handle all arguments starting with "-". These should appear before
         * any non-option arguments to the program.
         */
        final AtomicInteger i = new AtomicInteger(0);
        while (i.get() < args.length) {
            final String arg = args[i.get()];
            if (arg.startsWith("-")) {
                if (arg.equals("-seed")) {
                    opt.seed = Long.valueOf(args[i.incrementAndGet()]);
                } else if (arg.equals("-nsamples")) {
                    final String s = args[i.incrementAndGet()];
                    opt.nsamples = Integer.valueOf(s);
                    if (opt.nsamples <= 0) {
                        opt.usage(1/* status */,
                                "-nsamples must be positive, not: " + s);
                    }
                } else if (arg.equals("-nthreads")) {
                    final String s = args[i.incrementAndGet()];
                    opt.nthreads = Integer.valueOf(s);
                    if (opt.nthreads < 0) {
                        opt.usage(1/* status */,
                                "-nthreads must be non-negative, not: " + s);
                    }
                } else if (arg.equals("-schedulerClass")) {
                    final String s = args[i.incrementAndGet()];
                    opt.schedulerClassOverride = (Class<IGASSchedulerImpl>) Class.forName(s);
                } else if (arg.equals("-load")) {
                    final String s = args[i.incrementAndGet()];
                    opt.loadSet.add(s);
                } else {
                    if (!opt.handleArg(i, args)) {
                        opt.usage(1/* status */, "Unknown argument: " + arg);
                    }
                }
            } else {
                break;
            }
            i.incrementAndGet();
        }

        /*
         * Check for the remaining (required) argument(s).
         */
        final int nremaining = args.length - i.get();
        if (nremaining != 2) {
            /*
             * There are either too many or too few arguments remaining.
             */
            opt.usage(1/* status */, nremaining < 1 ? "Too few arguments."
                    : "Too many arguments");
        }

        /*
         * The analytic to be executed.
         */
        {

            final String s = args[i.getAndIncrement()];

            opt.analyticClass = (Class<IGASProgram<VS, ES, ST>>) Class
                    .forName(s);

        }

        /*
         * Property file.
         */
        opt.propertyFile = args[i.getAndIncrement()];

        this.opt = opt; // assign options.

    }

    /**
     * Return the object used to access the as-configured graph.
     */
    abstract protected IGraphAccessor newGraphAccessor();

    /**
     * Return an instance of the {@link IGASProgram} to be evaluated.
     */
    protected IGASProgram<VS, ES, ST> newGASProgram() {

        final Class<IGASProgram<VS, ES, ST>> cls = (Class<IGASProgram<VS, ES, ST>>)opt.analyticClass;

        try {

            final Constructor<IGASProgram<VS, ES, ST>> ctor = cls
                    .getConstructor(new Class[] {});

            final IGASProgram<VS, ES, ST> gasProgram = ctor
                    .newInstance(new Object[] {});

            return gasProgram;

        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }
    
    /**
     * Run the test.
     * <p>
     * This provides a safe pattern for either loading data into a temporary
     * journal, which is then destroyed, or using an exiting journal and
     * optionally loading in some data set. When we load the data the journal is
     * destroyed afterwards and when the journal is pre-existing and we neither
     * load the data nor destroy the journal. This has to do with the effective
     * BufferMode (if transient) and whether the file is specified and whether a
     * temporary file is created (CREATE_TEMP_FILE). If we do our own file
     * create if the effective buffer mode is non-transient, then we can get all
     * this information.
     */
    @Override
    final public IGASStats call() throws Exception {

        try {

            // initialize backend / connection to backend.
            opt.init(); 
            
            // Load data sets
            loadFiles();

            // Run GAS program.
            return runAnalytic();

        } finally {

            // Shutdown backend / connection to backend.
            opt.shutdown();
            
        }
        
    } 
    
    /**
     * Run the analytic.
     * 
     * @return The performance statistics for the run.
     * 
     * @throws Exception
     */
    final protected IGASStats runAnalytic() throws Exception {

        final IGASEngine gasEngine = newGASEngine();

        try {

            if (opt.schedulerClassOverride != null) {

                ((GASEngine) gasEngine)
                        .setSchedulerClass(opt.schedulerClassOverride);

            }
            
            final IGASProgram<VS, ES, ST> gasProgram = newGASProgram();

            final IGraphAccessor graphAccessor = newGraphAccessor();

            final IGASContext<VS, ES, ST> gasContext = gasEngine.newGASContext(
                    graphAccessor, gasProgram);

            final IGASState<VS, ES, ST> gasState = gasContext.getGASState();
            
            final VertexDistribution dist = graphAccessor.getDistribution(opt.r);
            
            final Value[] sampled = dist.getWeightedSample(opt.nsamples);

            final IGASStats total = new GASStats();

            for (int i = 0; i < sampled.length; i++) {

                final Value startingVertex = sampled[i];

                gasState.init(startingVertex);

                final IGASStats stats = (IGASStats) gasContext.call();

                total.add(stats);

                if (log.isInfoEnabled()) {
                    log.info("Run complete: vertex[" + i + "] of "
                            + sampled.length + " : startingVertex="
                            + startingVertex + ", stats(sample)=" + stats);
                }

            }

            // Total over all sampled vertices.
            final StringBuilder sb = new StringBuilder();
            sb.append("TOTAL");
            sb.append(": analytic=" + gasProgram.getClass().getSimpleName());
            sb.append(", nseed=" + opt.seed);
            sb.append(", nsamples=" + opt.nsamples); // #desired samples
            sb.append(", nsampled=" + sampled.length);// #actually sampled
            sb.append(", nthreads=" + opt.nthreads);
            sb.append(", scheduler=" + ((GASState<VS, ES, ST>)gasState).getScheduler().getClass().getSimpleName());
            sb.append(", gasEngine=" + gasEngine.getClass().getSimpleName());
            opt.report(sb); // extension hook.
            // performance results.
            sb.append(", stats(total)=" + total);
            System.out.println(sb);
                    
            return total;

        } finally {

            gasEngine.shutdownNow();
            
        }

    }
    
}
