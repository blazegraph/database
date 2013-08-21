package com.bigdata.rdf.graph.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Base class for running performance tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * TODO Need a different driver if the algorithm always visits all vertices.
 * 
 * TODO Parameterize for sampling of vertices.
 */
abstract public class PerformanceTest<VS, ES, ST> implements Callable<Void> {

    private static final Logger log = Logger.getLogger(PerformanceTest.class);
    
    private final String[] args;
    private final Random r;

    /*
     * TODO args[] are ignored. Should parse for [nsamples], [seed], the data
     * file (to load when run), memstore (must load when run) verus RWStore (for
     * preexisting data).
     */
    public PerformanceTest(final String[] args) {

        this.args = args;

        this.r = new Random(seed());

    }

    protected long seed() {

        return 217L;

    }

    protected int sampleSize() {

        return 100;

    }

    /**
     * 
     * @param kb
     */
    protected IV[] getRandomSamples(final AbstractTripleStore kb) {

        return GASGraphUtil.getRandomSample(r, kb, sampleSize());

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
                    
                    throw new IOException("Could not locate resource: " + resource);
                
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
     */
    public Void call() throws Exception {

        /*
         * The property file
         * 
         * TODO Use different files for MemStore and RWStore (command line arg)
         */
        final String propertyFile = "bigdata-rdf/src/test/com/bigdata/rdf/graph/impl/RWStore.properties";

        final Properties properties = getProperties(propertyFile);

        final BufferMode bufferMode = BufferMode.valueOf(properties
                .getProperty(Journal.Options.BUFFER_MODE,
                        Journal.Options.DEFAULT_BUFFER_MODE));

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
                        PerformanceTest.class.getSimpleName(),
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
        final String namespace = properties.getProperty(
                BigdataSail.Options.NAMESPACE,
                BigdataSail.Options.DEFAULT_NAMESPACE);
        
        /*
         * TODO Could start NSS and use SPARQL UPDATE "LOAD" to load the data.
         * That exposes the SPARQL end point for other purposes during the test.
         * Is this useful? It could also let us run the GASEngine on a remote
         * service (submit a callable to an HA server or define a REST API for
         * submitting these GAS algorithms).
         * 
         * TODO we need a safe pattern for when we load the data the journal is
         * destroyed afterwards and when the journal is pre-existing and we
         * neither load the data nor destroy the journal. This has to do with
         * the effective BufferMode (if transient) and whether the file is
         * specified and whether a temporary file is created (CREATE_TEMP_FILE).
         * If we do our own file create if the effective buffer mode is
         * non-transient, then we can get all this information.
         */
        final Journal jnl = new Journal(properties);

        try {

            // Locate/create KB.
            {
                final AbstractTripleStore kb;
                if (isTemporary) {

                    kb = BigdataSail.createLTS(jnl, properties);

                } else {

                    final AbstractTripleStore tmp = (AbstractTripleStore) jnl
                            .getResourceLocator().locate(namespace,
                                    ITx.UNISOLATED);

                    if (tmp == null) {

                        // create.
                        kb = BigdataSail.createLTS(jnl, properties);

                    } else {

                        kb = tmp;

                    }

                }
            }

            /*
             * Load data sets.
             */
            if (isTemporary) {

                final String path = "bigdata-rdf/src/resources/data/foaf";
                final String dataFile[] = new String[] {//
                path + "/data-0.nq.gz",//
                        path + "/data-1.nq.gz",//
                        path + "/data-2.nq.gz",//
                        path + "/data-3.nq.gz",//
                };
                final String baseUrl[] = new String[dataFile.length];
                for (int i = 0; i < dataFile.length; i++) {
                    baseUrl[i] = "file:" + dataFile[i];
                }
                final RDFFormat[] rdfFormat = new RDFFormat[] {//
                RDFFormat.NQUADS,//
                        RDFFormat.NQUADS,//
                        RDFFormat.NQUADS,//
                        RDFFormat.NQUADS,//
                };

                // Load data using the unisolated view.
                final AbstractTripleStore kb = (AbstractTripleStore) jnl
                        .getResourceLocator().locate(namespace, ITx.UNISOLATED);

                kb.getDataLoader().loadData(dataFile, baseUrl, rdfFormat);

            }

            final IGASEngine<VS, ES, ST> gasEngine = new GASEngine<VS, ES, ST>(
                    jnl, namespace, ITx.READ_COMMITTED, newGASProgram());

            @SuppressWarnings("rawtypes")
            final IV[] samples;
            {
                /*
                 * Use a read-only view (sampling depends on access to the BTree
                 * rather than the ReadCommittedIndex).
                 */
                final AbstractTripleStore kb = (AbstractTripleStore) jnl
                        .getResourceLocator().locate(namespace,
                                jnl.getLastCommitTime());

                samples = getRandomSamples(kb);

            }

            final GASStats total = new GASStats();
            
            for (int i = 0; i < samples.length; i++) {

                @SuppressWarnings("rawtypes")
                final IV startingVertex = samples[i];

                gasEngine.init(startingVertex);

                // TODO Pure interface for this.
                total.add((GASStats)gasEngine.call());

            }

            // Total over all sampled vertices.
            System.out.println("TOTAL: " + total);

            // Done.
            return null;

        } finally {

            jnl.close();

        }

    }

}
