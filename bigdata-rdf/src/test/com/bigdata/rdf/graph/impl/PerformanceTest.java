package com.bigdata.rdf.graph.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.internal.IV;
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
    
    /**
     * Run the test.
     */
    public Void call() throws Exception {

        // The property file (for an existing Journal).
        final File propertyFile = new File("RWStore.properties");

        // The namespace of the KB.
        final String namespace = "kb";

        final boolean quiet = false;

        final Properties properties = new Properties();
        {
            if (!quiet)
                System.out.println("Reading properties: " + propertyFile);
            final InputStream is = new FileInputStream(propertyFile);
            try {
                properties.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }

        final Journal jnl = new Journal(properties);

        try {

            final long timestamp = jnl.getLastCommitTime();

            final AbstractTripleStore kb = (AbstractTripleStore) jnl
                    .getResourceLocator().locate(namespace, timestamp);

            if (kb == null)
                throw new RuntimeException("No such KB: " + namespace);

            final IGASEngine<VS, ES, ST> gasEngine = new GASEngine<VS, ES, ST>(
                    kb, newGASProgram());

            @SuppressWarnings("rawtypes")
            final IV[] samples = getRandomSamples(kb);

            for (int i = 0; i < samples.length; i++) {

                @SuppressWarnings("rawtypes")
                final IV startingVertex = samples[i];

                gasEngine.init(startingVertex);

                gasEngine.call();

            }

            // Done.
            return null;

        } finally {

            jnl.close();

        }

    }

}
