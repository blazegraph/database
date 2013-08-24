package com.bigdata.rdf.graph.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.Banner;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Base class for running performance tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * TODO Need a different driver if the algorithm always visits all vertices.
 */
abstract public class GASRunner<VS, ES, ST> implements Callable<GASStats> {

    private static final Logger log = Logger.getLogger(GASRunner.class);

    /**
     * The seed used for the random number generator.
     */
    private final long seed;
    
    /**
     * Random number generated used for sampling the starting vertices.
     */
    private final Random r;

    /**
     * The #of random starting vertices to use.
     */
    private final int nsamples;

    /**
     * The #of threads to use for GATHER and SCATTER operators.
     */
    private final int nthreads;

    /**
     * The property file
     */
    private final String propertyFile;

    /**
     * When non-<code>null</code>, this overrides the current buffer mode.
     */
    private final BufferMode bufferModeOverride;

    /**
     * When non-<code>null</code>, this overrides the KB namespace.
     */
    private final String namespaceOverride;

    /**
     * When non-<code>null</code>, a list of zero or more resources to be
     * loaded. The resources will be searched for as URLs, on the CLASSPATH, and
     * in the file system.
     */
    private final String[] loadSet;

    /**
     * Print the optional message on stderr, print the usage information on
     * stderr, and then force the program to exit with the given status code.
     * 
     * @param status
     *            The status code.
     * @param msg
     *            The optional message
     */
    private static void usage(final int status, final String msg) {

        if (msg != null) {

            System.err.println(msg);

        }

        System.err.println("[options] propertyFile");

        System.exit(status);

    }

    /**
     * Run a GAS analytic against some data set.
     * 
     * @param args
     *            USAGE:<br/>
     *            <code>(options) propertyFile</code>
     *            <p>
     *            <i>Where:</i>
     *            <dl>
     *            <dt>propertyFile</dt>
     *            <dd>A java properties file for a standalone {@link Journal}.</dd>
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
     *            <dt>-namespace</dt>
     *            <dd>The namespace of the default SPARQL endpoint (the
     *            namespace will be <code>kb</code> if none was specified when
     *            the triple/quad store was created).</dd>
     *            <dt>-load</dt>
     *            <dd>Loads the named resource IFF the KB is empty (or does not
     *            exist) at the time this utility is executed. This option may
     *            appear multiple times. The resources will be searched for as
     *            URLs, on the CLASSPATH, and in the file system.</dd>
     *            <dt>-bufferMode</dt>
     *            <dd>Overrides the {@link BufferMode} (if any) specified in the
     *            <code>propertyFile</code>.</dd>
     *            </p>
     */
    public GASRunner(final String[] args) {

        Banner.banner();

        long seed = 217L;
        int nsamples = 100;
        int nthreads = 4;
        BufferMode bufferMode = null; // override only.
        String namespace = "kb";
        // Set of files to load (optional).
        LinkedHashSet<String> loadSet = new LinkedHashSet<String>();

        /*
         * Handle all arguments starting with "-". These should appear before
         * any non-option arguments to the program.
         */
        int i = 0;
        while (i < args.length) {
            final String arg = args[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-seed")) {
                    seed = Long.valueOf(args[++i]);
                } else if (arg.equals("-nsamples")) {
                    final String s = args[++i];
                    nsamples = Integer.valueOf(s);
                    if (nsamples <= 0) {
                        usage(1/* status */,
                                "-nsamples must be positive, not: " + s);
                    }
                } else if (arg.equals("-nthreads")) {
                    final String s = args[++i];
                    nthreads = Integer.valueOf(s);
                    if (nthreads < 0) {
                        usage(1/* status */,
                                "-nthreads must be non-negative, not: " + s);
                    }
                } else if (arg.equals("-bufferMode")) {
                    final String s = args[++i];
                    bufferMode = BufferMode.valueOf(s);
                } else if (arg.equals("-namespace")) {
                    final String s = args[++i];
                    namespace = s;
                } else if (arg.equals("-load")) {
                    final String s = args[++i];
                    loadSet.add(s);
                } else {
                    usage(1/* status */, "Unknown argument: " + arg);
                }
            } else {
                break;
            }
            i++;
        }

        /*
         * Check for the remaining (required) argument(s).
         */
        final int nremaining = args.length - i;
        if (nremaining != 1) {
            /*
             * There are either too many or too few arguments remaining.
             */
            usage(1/* status */, nremaining < 1 ? "Too few arguments."
                    : "Too many arguments");
        }

        /*
         * Property file.
         */
        this.propertyFile = args[i++];

        /*
         * Assign parsed values.
         */
        this.seed = seed;
        this.nsamples = nsamples;
        this.nthreads = nthreads;
        this.namespaceOverride = namespace;
        this.bufferModeOverride = bufferMode;
        this.loadSet = loadSet.isEmpty() ? null : loadSet
                .toArray(new String[loadSet.size()]);

        // Setup the random number generator.
        this.r = new Random(seed);

    }

    /**
     * Return the {@link IGASProgram} to be evaluated.
     */
    abstract protected IGASProgram<VS, ES, ST> newGASProgram();

    private Properties getProperties(final String resource) throws IOException {

        if (log.isInfoEnabled())
            log.info("Reading properties: " + resource);

        InputStream is = null;
        try {

            // try the classpath
            is = getClass().getResourceAsStream(resource);

            if (is != null) {

            } else {

                // try file system.
                final File file = new File(resource);

                if (file.exists()) {

                    is = new FileInputStream(file);

                } else {

                    throw new IOException("Could not locate resource: "
                            + resource);

                }

            }

            /*
             * Obtain a buffered reader on the input stream.
             */

            final Properties properties = new Properties();

            final Reader reader = new BufferedReader(new InputStreamReader(is));

            try {

                properties.load(reader);

            } finally {

                try {

                    reader.close();

                } catch (Throwable t) {

                    log.error(t);

                }

            }

            return properties;

        } finally {

            if (is != null) {

                try {

                    is.close();

                } catch (Throwable t) {

                    log.error(t);

                }

            }

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
    public GASStats call() throws Exception {

        final Properties properties = getProperties(propertyFile);

        /*
         * Note: Allows override through the command line argument. The default
         * is otherwise the default and the value in the properties file (if
         * any) will be used unless it is overridden.
         */
        final BufferMode bufferMode = this.bufferModeOverride == null ? BufferMode
                .valueOf(properties.getProperty(Journal.Options.BUFFER_MODE,
                        Journal.Options.DEFAULT_BUFFER_MODE)) : this.bufferModeOverride;

        final boolean isTransient = !bufferMode.isStable();

        final boolean isTemporary;
        if (isTransient) {

            isTemporary = true;

        } else {

            final String fileStr = properties.getProperty(Journal.Options.FILE);

            if (fileStr == null) {

                /*
                 * We will use a temporary file that we create here. The journal
                 * will be destroyed below.
                 */
                isTemporary = true;

                final File tmpFile = File.createTempFile(
                        GASRunner.class.getSimpleName(), Journal.Options.JNL);

                // Set this on the Properties so it will be used by the jnl.
                properties.setProperty(Journal.Options.FILE,
                        tmpFile.getAbsolutePath());

            } else {

                // real file is named.
                isTemporary = false;

            }

        }

        // The effective KB name.
        final String namespace = this.namespaceOverride == null ? properties
                .getProperty(BigdataSail.Options.NAMESPACE,
                        BigdataSail.Options.DEFAULT_NAMESPACE) : this.namespaceOverride;

        /*
         * TODO Could start NSS and use SPARQL UPDATE "LOAD" to load the data.
         * That exposes the SPARQL end point for other purposes during the test.
         * Is this useful? It could also let us run the GASEngine on a remote
         * service (submit a callable to an HA server or define a REST API for
         * submitting these GAS algorithms).
         */
        final Journal jnl = new Journal(properties);

        try {

            // Locate/create KB.
            final boolean newKB;
            {
                final AbstractTripleStore kb;
                if (isTemporary) {

                    kb = BigdataSail.createLTS(jnl, properties);
                    newKB = true;

                } else {

                    final AbstractTripleStore tmp = (AbstractTripleStore) jnl
                            .getResourceLocator().locate(namespace,
                                    ITx.UNISOLATED);

                    if (tmp == null) {

                        // create.
                        kb = BigdataSail.createLTS(jnl, properties);
                        newKB = true;

                    } else {

                        kb = tmp;
                        newKB = kb.getStatementCount() == 0L;

                    }

                }
            }

            /*
             * Load data sets. TODO Document that KB load is IFF empty!!! (Or change the code.)
             */
            if (newKB && (loadSet != null && loadSet.length > 0)) {

                loadFiles(jnl, namespace, loadSet);

            }

            return runAnalytic(jnl, namespace);

        } finally {

            if (isTemporary) {

                log.warn("Destroying temporary journal.");
                
                jnl.destroy();
                
            } else {
                
                jnl.close();
                
            }

        }

    }

    /**
     * Load files into the journal.
     * 
     * @param jnl
     *            The journal.
     * @param namespace
     *            The KB namespace.
     * @param loadSet
     *            The files.
     * @throws IOException 
     */
    private LoadStats loadFiles(final Journal jnl, final String namespace,
            final String[] loadSet) throws IOException {

        // final String path = "bigdata-rdf/src/resources/data/foaf";
        // final String dataFile[] = new String[] {//
        // path + "/data-0.nq.gz",//
        // path + "/data-1.nq.gz",//
        // path + "/data-2.nq.gz",//
        // path + "/data-3.nq.gz",//
        // };
        final String baseUrl[] = new String[loadSet.length];
        for (int i = 0; i < loadSet.length; i++) {
            baseUrl[i] = "file:" + loadSet[i];
        }
        // fall back RDFFormat.
        final RDFFormat[] rdfFormat = new RDFFormat[loadSet.length];
        for (int i = 0; i < loadSet.length; i++) {
            rdfFormat[i] = RDFFormat.RDFXML;
        }

        // Load data using the unisolated view.
        final AbstractTripleStore kb = (AbstractTripleStore) jnl
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        final LoadStats stats = kb.getDataLoader().loadData(loadSet, baseUrl,
                rdfFormat);

        System.out.println(stats.toString());

        return stats;

    }
    
    /**
     * Run the analytic.
     * 
     * @param jnl
     *            The journal.
     * @param namespace
     *            The KB namespace.
     * @return
     * @throws Exception
     * 
     * TODO What happened to the predicate summary/histogram/distribution code?
     * 
     * TODO Are we better off using sampling based on distinct vertices or with
     * a bais based on the #of edges for those vertices.
     */
    private GASStats runAnalytic(final Journal jnl, final String namespace)
            throws Exception {

        /*
         * Use a read-only view (sampling depends on access to the BTree rather
         * than the ReadCommittedIndex).
         */
        final AbstractTripleStore kb = (AbstractTripleStore) jnl
                .getResourceLocator()
                .locate(namespace, jnl.getLastCommitTime());

        @SuppressWarnings("rawtypes")
        final IV[] samples = GASGraphUtil.getRandomSample(r, kb, nsamples);

        // total #of edges in that graph.
        final long nedges = kb.getStatementCount();

        /*
         * Setup and run over those samples.
         */
        
        final IGASProgram<VS, ES, ST> gasProgram = newGASProgram();

        final IGASEngine<VS, ES, ST> gasEngine = new GASEngine<VS, ES, ST>(jnl,
                namespace, ITx.READ_COMMITTED, gasProgram, nthreads);

        final GASStats total = new GASStats();

        for (int i = 0; i < samples.length; i++) {

            @SuppressWarnings("rawtypes")
            final IV startingVertex = samples[i];

            gasEngine.init(startingVertex);

            // TODO Pure interface for this.
            final GASStats stats = (GASStats) gasEngine.call();

            total.add(stats);

            if (log.isInfoEnabled()) {
                log.info("Run complete: vertex[" + i + "] of " + samples.length
                        + " : startingVertex=" + startingVertex
                        + ", stats(sample)=" + stats);
            }
            
        }

        // Total over all sampled vertices.
        System.out.println("TOTAL: analytic="
                + gasProgram.getClass().getSimpleName() + ", nseed=" + seed
                + ", nsamples=" + nsamples + ", nthreads=" + nthreads
                + ", bufferMode=" + jnl.getBufferStrategy().getBufferMode()
                + ", edges(kb)=" + nedges + ", stats(total)=" + total);

        return total;

    }

}
