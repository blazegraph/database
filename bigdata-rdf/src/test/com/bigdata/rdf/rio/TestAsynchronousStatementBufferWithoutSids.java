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
 * Created on Apr 18, 2009
 */

package com.bigdata.rdf.rio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AsynchronousStatementBufferWithoutSids.AsynchronousWriteConfiguration;
import com.bigdata.rdf.rio.AsynchronousStatementBufferWithoutSids.IAsynchronousWriteConfiguration;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.TestScaleOutTripleStoreWithEmbeddedFederation;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for {@link AsynchronousStatementBufferWithoutSids}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test w/ and w/o the full text index.
 * 
 * @todo variant to test async w/ sids (once written).
 * 
 * @todo The async API is only defined at this time for scale-out index views,
 *       so maybe move this into the scale-out proxy test suite.
 * 
 * @todo test with more than one document that is loaded concurrently.
 * 
 * {@link TestScaleOutTripleStoreWithEmbeddedFederation}
 */
public class TestAsynchronousStatementBufferWithoutSids extends
        AbstractRIOTestCase {

    /**
     * 
     */
    public TestAsynchronousStatementBufferWithoutSids() {
    }

    /**
     * @param name
     */
    public TestAsynchronousStatementBufferWithoutSids(String name) {
        super(name);
    }

    /**
     * Note: This is overriden to turn off features not supported by this
     * loader.
     */
    protected AbstractTripleStore getStore() {

        final Properties properties = new Properties(getProperties());

        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");

        properties.setProperty(
                AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");

        // no closure so we don't need the axioms either.
        properties.setProperty(
                AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());

        // no vocab required.
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        // @todo comment out or will fail during verify.
//        properties.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");

        return getStore(properties);

    }

    /**
     * Test with the "small.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_small() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf";

        doLoadAndVerifyTest(resource);

    }

    /**
     * Test with the "sample data.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_sampleData() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/sample data.rdf";

        doLoadAndVerifyTest( resource );
        
    }
   
    /**
     * @todo use some modest to largish RDF/XML that we can pack with the
     *       distribution.
     */
    public void test_loadAndVerify_modest() throws Exception {
        
//      final String file = "../rdf-data/nciOncology.owl";
        final String file = "../rdf-data/alibaba_v41.rdf";

        if (!new File(file).exists()) {

            fail("Resource not found: " + file + ", test skipped: " + getName());

            return;
            
        }

        doLoadAndVerifyTest( file );
        
    }

    /**
     * @todo modify verify to handle a directory of files and then change this
     *       test do use verify.
     * 
     * @todo this is triggering a scatter split (EDS)!
     */
    public void test_loadAndVerify_U1() throws Exception {
        
        if(true) {
            
            fail("test is non-terminating for EDS and does not verify output.");
            
        }
        
        final String file = "bigdata-rdf/src/resources/data/lehigh/U1";

        if (!new File(file).exists()) {

            fail("Resource not found: " + file + ", test skipped: " + getName());

            return;

        }

        final AbstractTripleStore store = getStore();
        
        try {

            load(store, file);

        } finally {

            store.closeAndDelete();

        }
        
    }

    /**
     * Test loads an RDF/XML resource into a database and then verifies by
     * re-parse that all expected statements were made persistent in the
     * database.
     * 
     * @param resource
     * 
     * @throws Exception
     */
    protected void doLoadAndVerifyTest(final String resource) throws Exception {

        AbstractTripleStore store = getStore();
        
        if(!(store instanceof ScaleOutTripleStore)) {
            
            fail("Test requires scale-out index views.");
            
        }
        
        try {

            load(store, resource);

            if (log.isDebugEnabled()) {
                log.debug("dumping store...");
                log.debug("\n---SPO---\n"
                        + store.dumpStore(store/* resolveTerms */,
                                true/* explicit */, true/* inferred */,
                                true/* axioms */, true/* justifications */,
                                SPOKeyOrder.SPO));
                log.debug("\n---POS---\n"
                        + store.dumpStore(store/* resolveTerms */,
                                true/* explicit */, true/* inferred */,
                                true/* axioms */, true/* justifications */,
                                SPOKeyOrder.POS));
                log.debug("\n---OSP---\n"
                        + store.dumpStore(store/* resolveTerms */,
                                true/* explicit */, true/* inferred */,
                                true/* axioms */, true/* justifications */,
                                SPOKeyOrder.OSP));
            }

            verify(store, resource);

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * Load the file using {@link AsynchronousStatementBufferWithoutSids}.
     * <p>
     * Note: Normally we disable closure for this test, but that is not
     * critical. If you compute the closure of the data set then there will
     * simply be additional statements whose self-consistency among the
     * statement indices will be verified, but it will not verify the
     * correctness of the closure.
     */
    protected void load(final AbstractTripleStore store, final String resource)
            throws Exception {

        final int chunkSize = 10000;

        final IAsynchronousWriteConfiguration<BigdataStatement> factory = new AsynchronousWriteConfiguration<BigdataStatement>(
                (ScaleOutTripleStore) store, chunkSize);

        try {

            loadOne(resource, factory);
            
            factory.awaitAll();
            
            // @todo dump write buffer statistics (stats for indices used by kb)
            
        } catch (Throwable t) {
            
            factory.cancelAll(true/* mayInterruptIfRunning */);
            
            // rethrow
            throw new RuntimeException(t);
            
        }
        
    }
    
    /**
     * Load a resource from the classpath or the file system.
     * 
     * @param resource
     *            A resource on the class path, a file, or a directory.
     *            
     * @param factory
     * 
     * @throws IOException
     * @throws URISyntaxException 
     */
    protected void loadOne(final String resource,
            IStatementBufferFactory<? extends BigdataStatement> factory)
            throws IOException, URISyntaxException {

        if (log.isInfoEnabled())
            log.info("Loading: " + resource + " using " + factory);

        String baseURI = null;
        
        InputStream rdfStream = null;
        try {

            // try the classpath
            rdfStream = getClass().getResourceAsStream(resource);

            if (rdfStream != null) {
                
                // set for resource on classpath.
                baseURI = getClass().getResource(resource).toURI().toString();
                
            } else {

                // try file system.
                final File file = new File(resource);

                if (file.exists()) {

                    if(file.isDirectory()) {

                        if(log.isInfoEnabled())
                            log.info("Loading directory: "+file);
                        
                        final File[] files = file.listFiles();
                        
                        for(File t : files) {
                            
                            loadOne(t.toString(), factory);
                            
                        }
                        
                        // done.
                        return;
                        
                    }
                    
                    rdfStream = new FileInputStream(file);

                    // set for file as URI.
                    baseURI = file.toURI().toString();
                    
                } else {
                    
                    fail("Could not locate resource: " + resource);
                
                }
                
            }

            /*
             * Obtain a buffered reader on the input stream.
             */
            final Reader reader = new BufferedReader(new InputStreamReader(
                    rdfStream));

            try {

                // guess at the RDF Format, assume RDF/XML.
                final RDFFormat rdfFormat = RDFFormat.forFileName(resource,
                        RDFFormat.RDFXML);
                
                // verify RDF/XML syntax.
                final boolean verifyData = true;
                
                // Setup the loader.
                final PresortRioLoader loader = new PresortRioLoader(factory
                        .newStatementBuffer());

                // add listener to log progress.
                loader.addRioLoaderListener(new RioLoaderListener() {

                    public void processingNotification(RioLoaderEvent e) {

                        if (log.isInfoEnabled())
                            log.info(e.getStatementsProcessed() + " stmts added in "
                                    + (e.getTimeElapsed() / 1000d) + " secs, rate= "
                                    + e.getInsertRate());

                    }

                });

                loader.loadRdf((Reader) reader, baseURI, rdfFormat, verifyData);

                if (log.isInfoEnabled())
                    log.info("Done: " + resource);
                
            } catch (Exception ex) {

                throw new RuntimeException("While loading: " + resource, ex);

            } finally {

                try {
                    reader.close();
                } catch (Throwable t) {
                    log.error(t);
                }

            }

        } finally {

            if (rdfStream != null) {

                try {
                    rdfStream.close();
                } catch (Throwable t) {
                    log.error(t);
                }

            }

        }

    }

}
