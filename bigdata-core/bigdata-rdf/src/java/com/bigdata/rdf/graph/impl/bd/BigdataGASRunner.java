package com.bigdata.rdf.graph.impl.bd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.sail.SailConnection;

import com.bigdata.Banner;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine.BigdataGraphAccessor;
import com.bigdata.rdf.graph.impl.bd.BigdataGraphFixture.BigdataSailGraphLoader;
import com.bigdata.rdf.graph.impl.util.GASRunnerBase;
import com.bigdata.rdf.graph.util.GraphLoader;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Base class for running performance tests against the bigdata backend.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataGASRunner<VS, ES, ST> extends GASRunnerBase<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(BigdataGASRunner.class);
    
    /**
     * Configured options for the {@link GASRunner}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class BigdataOptionData extends
            GASRunnerBase<VS, ES, ST>.OptionData {
        
        /**
         * The {@link BufferMode} to use.
         */
        private  BufferMode bufferModeOverride = null; // override only.
        
        /**
         * The namespace of the bigdata KB instance.
         */
        private String namespaceOverride = "kb";

        /**
         * The as-configured {@link Properties} for the {@link Journal}.
         */
        private Properties properties;

        /**
         * The effective KB name. This is set by consulting
         * {@link #namespaceOverride} and the as configured {@link #properties}.
         */
        private String namespace;

        /**
         * The backend.
         * 
         * TODO Could start NSS and use SPARQL UPDATE "LOAD" to load the data.
         * That exposes the SPARQL end point for other purposes during the test.
         * Is this useful? It could also let us run the GASEngine on a remote
         * service (submit a callable to an HA server or define a REST API for
         * submitting these GAS algorithms).
         */
        private Journal jnl;

        /**
         * <code>true</code> iff the backend is temporary (created on a
         * temporary backing file). Temporary backends are destroyed in
         * {@link #shutdown()}.
         */
        private boolean isTemporary;
        
        /**
         * Set to <code>true</code> iff we determine that the data needs to be
         * loaded (e.g., the KB was empty, so we have to load the data sets).
         * 
         * TODO Rename for clearer semantics. Basically, do we have to load the
         * data files or can we assume that the data are already loaded. Lift
         * into base class?
         */
        private boolean newKB = false;
        
        /**
         * The #of edges in the KB instance and <code>-1</code> until set by
         * {@link BigdataGASRunner#loadFiles()}.
         */
        private long nedges = -1;
        
        protected BigdataOptionData() {
            
            super();
            
        }

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

                /*
                 * Allow override of select options from the command line.
                 */
                {
                    final String[] overrides = new String[] {
                            // Journal options.
                            com.bigdata.journal.Options.FILE,
//                            // RDFParserOptions.
//                            RDFParserOptions.Options.DATATYPE_HANDLING,
//                            RDFParserOptions.Options.PRESERVE_BNODE_IDS,
//                            RDFParserOptions.Options.STOP_AT_FIRST_ERROR,
//                            RDFParserOptions.Options.VERIFY_DATA,
//                            // DataLoader options.
//                            DataLoader.Options.BUFFER_CAPACITY,
//                            DataLoader.Options.CLOSURE,
//                            DataLoader.Options.COMMIT,
//                            DataLoader.Options.FLUSH,
                    };
                    for (String s : overrides) {
                        if (System.getProperty(s) != null) {
                            // Override/set from the environment.
                            final String v = System.getProperty(s);
                            if (log.isInfoEnabled())
                                log.info("OVERRIDE:: Using: " + s + "=" + v);
                            properties.setProperty(s, v);
                        }
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
         * Initialization after all arguments have been set.
         */
        @Override
        public void init() throws Exception {

            super.init();

            properties = getProperties(propertyFile);

            /*
             * Note: Allows override through the command line argument. The default
             * is otherwise the default and the value in the properties file (if
             * any) will be used unless it is overridden.
             */
            final BufferMode bufferMode = bufferModeOverride == null ? BufferMode
                    .valueOf(properties.getProperty(Journal.Options.BUFFER_MODE,
                            Journal.Options.DEFAULT_BUFFER_MODE)) : bufferModeOverride;

            properties.setProperty(Journal.Options.BUFFER_MODE, bufferMode.name());

            final boolean isTransient = !bufferMode.isStable();

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
                            BigdataGASRunner.class.getSimpleName(),
                            Journal.Options.JNL);

                    // Set this on the Properties so it will be used by the jnl.
                    properties.setProperty(Journal.Options.FILE,
                            tmpFile.getAbsolutePath());

                } else {

                    // real file is named.
                    isTemporary = false;

                }

            }

            // The effective KB name.
            namespace = namespaceOverride == null ? properties
                    .getProperty(BigdataSail.Options.NAMESPACE,
                            BigdataSail.Options.DEFAULT_NAMESPACE) : namespaceOverride;

            properties.setProperty(BigdataSail.Options.NAMESPACE, namespace);

            // Open Journal.
            jnl = new Journal(properties);

            // Locate/create KB.
            {
                final BigdataSail sail;
                if (isTemporary) {

                    new BigdataSail(namespace, jnl).create(properties);
                    newKB = true;

                } else {

                    sail = new BigdataSail(namespace, jnl);
                    
                    if (!sail.exists()) {

                        // create.
                        sail.create(properties);
                        newKB = true;

                    } else {

                        // exists.
                        final BigdataSailConnection con = sail.getReadOnlyConnection();
                        try {
                            newKB = con.getTripleStore().getStatementCount() == 0L;
                        } finally {
                            con.close();
                        }

                    }

                }
            }
        }

        @Override
        public void shutdown() {

            if (jnl != null) {

                if (isTemporary) {

                    log.warn("Destroying temporary journal.");

                    jnl.destroy();

                } else {

                    jnl.close();

                }

            }

            super.shutdown();
            
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
        @Override
        public boolean handleArg(final AtomicInteger i, final String[] args) {
            if (super.handleArg(i, args)) {
                return true;
            }
            final String arg = args[i.get()];
            if (arg.equals("-bufferMode")) {
                final String s = args[i.incrementAndGet()];
                bufferModeOverride = BufferMode.valueOf(s);
            } else if (arg.equals("-namespace")) {
                final String s = args[i.incrementAndGet()];
                namespaceOverride = s;
            } else {
                return false;
            }
            return true;
        }
                
        /**
         * {@inheritDoc}
         * 
         * TODO report #of vertices (DISTINCT UNION (?s, ?o)
         * 
         * TODO What happened to the predicate summary/histogram/distribution
         * code?
         */
        @Override
        public void report(final StringBuilder sb) {
            sb.append(", edges(kb)=" + nedges);
            sb.append(", namespace=" + namespace);//
            sb.append(", bufferMode=" + jnl.getBufferStrategy().getBufferMode());
        }
    }

    /**
     * Factory for the {@link OptionData}.
     */
    @Override
    protected OptionData newOptionData() {
        
        return new BigdataOptionData();
        
    }

    @Override
    protected BigdataGASEngine newGASEngine() {
        
        final BigdataOptionData opt = getOptionData();

        return new BigdataGASEngine(opt.jnl, opt.nthreads);

    }
    
    @Override
    protected IGraphAccessor newGraphAccessor() {

        final BigdataOptionData opt = getOptionData();
        
        /*
         * Use a read-only view (sampling depends on access to the BTree rather
         * than the ReadCommittedIndex).
         */
        final BigdataGraphAccessor graphAccessor = new BigdataGraphAccessor(
                opt.jnl, opt.namespace, opt.jnl.getLastCommitTime());

        return graphAccessor;
        
    }

    @SuppressWarnings("unchecked")
    @Override
    protected BigdataOptionData getOptionData() {

        return (BigdataOptionData) super.getOptionData();
        
    }

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
     *            <dd>A java properties file for a standalone {@link Journal}.</dd>
     *            </dl>
     *            and <i>options</i> are any of the options defined for the
     *            {@link GASRunnerBase} PLUS any of:
     *            <dl>
     *            <dt>-bufferMode</dt>
     *            <dd>Overrides the {@link BufferMode} (if any) specified in the
     *            <code>propertyFile</code>.</dd>
     *            <dt>-namespace</dt>
     *            <dd>The namespace of the default SPARQL endpoint (the
     *            namespace will be <code>kb</code> if none was specified when
     *            the triple/quad store was created).</dd>
     *            </p>
     * @throws ClassNotFoundException
     */
    public BigdataGASRunner(final String[] args) throws ClassNotFoundException {

        super(args);

        Banner.banner();

    }

    /**
     * Return an instance of the {@link IGASProgram} to be evaluated.
     */
    protected IGASProgram<VS, ES, ST> newGASProgram() {

        final Class<IGASProgram<VS, ES, ST>> cls = getOptionData().analyticClass;

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

    @Override
    public void loadFiles() throws IOException {

        final BigdataOptionData opt = getOptionData();

        final Journal jnl = opt.jnl;
        final String namespace = opt.namespace;
        final String[] loadSet = opt.loadSet.toArray(new String[0]);

        // Load data using the unisolated view.
        final AbstractTripleStore kb = (AbstractTripleStore) jnl
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (opt.newKB && loadSet.length > 0) {

            final BigdataSail sail = new BigdataSail(kb);
            try {
                try {
                    sail.initialize();
                    loadFiles(sail, loadSet);
                } finally {
                    if (sail.isOpen())
                        sail.shutDown();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        // total #of edges in that graph.
        opt.nedges = kb.getStatementCount();

    }

    private void loadFiles(final BigdataSail sail, final String[] loadSet)
            throws Exception {
        boolean ok = false;
        final SailConnection cxn = sail.getUnisolatedConnection();
        try {
            final GraphLoader loader = new BigdataSailGraphLoader(cxn);
            for (String f : loadSet) {
                loader.loadGraph(null/* fallback */, f/* resource */);
            }
            cxn.commit();
            ok = true;
        } finally {
            if (!ok)
                cxn.rollback();
            cxn.close();
        }
    }
    
    /**
     * Performance testing harness.
     * 
     * @see #GASRunner(String[])
     */
    @SuppressWarnings("rawtypes")
    public static void main(final String[] args) throws Exception {

        new BigdataGASRunner(args).call();

    }

}
