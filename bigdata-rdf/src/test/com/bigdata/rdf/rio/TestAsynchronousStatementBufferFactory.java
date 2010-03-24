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

import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.openrdf.rio.RDFFormat;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.load.RDFFileLoadTask;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.TestScaleOutTripleStoreWithEmbeddedFederation;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.EmbeddedFederation;

/**
 * Test suite for {@link AsynchronousStatementBufferFactory}. To run this test
 * by itself specify
 * <code>-DtestClass=com.bigdata.rdf.store.TestScaleOutTripleStoreWithEmbeddedFederation</code>
 * .
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME variant to test w/ and w/o the full text index (with lookup by
 *          tokens).
 * 
 *          FIXME variant to test async w/ sids (once written).
 * 
 * @todo The {@link AsynchronousStatementBufferFactory} works with either
 *       triples or quads. However, we are not running
 *       {@link EmbeddedFederation} proxy test suite for quads yet, so you have
 *       to explicitly turn on quads in {@link #getProperties()} in order to run
 *       this test suite for quads.
 * 
 * @todo The async API is only defined at this time for scale-out index views,
 *       so maybe move this into the scale-out proxy test suite.
 * 
 * @see TestScaleOutTripleStoreWithEmbeddedFederation
 * @see RDFFileLoadTask
 */
public class TestAsynchronousStatementBufferFactory extends
        AbstractRIOTestCase {

    /**
     * 
     */
    public TestAsynchronousStatementBufferFactory() {
    }

    /**
     * @param name
     */
    public TestAsynchronousStatementBufferFactory(String name) {
        super(name);
    }

    final int chunkSize = 20000;
    final int valuesInitialCapacity = 10000;
    final int bnodesInitialCapacity = 16;
    final long unbufferedStatementThreshold = 5000L;//Long.MAX_VALUE;
    final long rejectedExecutionDelay = 250L; // milliseconds.
    
    /**
     * SHOULD be <code>true</code> since the whole point of this is higher
     * concurrency. If you set this to <code>false</code> to explore some
     * issue, then change it back to <code>true</code> when you are done!
     */
    final boolean parallel = true;
    
    /**
     * Note: This is overridden to turn off features not supported by this
     * loader.
     */
    protected AbstractTripleStore getStore() {

        final Properties properties = new Properties(getProperties());

        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");

        properties.setProperty(
                AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");

//        properties.setProperty(
//                AbstractTripleStore.Options.QUADS, "true");

        // no closure so we don't need the axioms either.
        properties.setProperty(
                AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());

        // no vocab required.
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
                NoVocabulary.class.getName());

        {

            /*
             * FIXME We MUST specify the KB namespace so we can override this
             * property. [Another approach is to override the idle timeout and
             * have it be less than the chunk timeout such that the sink is
             * closed if it becomes idle (no new chunks appearing) but continues
             * to combine chunks as long as they are appearing before the idle
             * timeout.
             */
            final String namespace = "test1";

            final String pname = com.bigdata.config.Configuration
                    .getOverrideProperty(namespace + "."
                            + LexiconRelation.NAME_LEXICON_RELATION + "."
                            + LexiconKeyOrder.TERM2ID,
                            IndexMetadata.Options.SINK_IDLE_TIMEOUT_NANOS);

            final String pval = "" + TimeUnit.SECONDS.toNanos(1);
            
            System.out.println("Override: " + pname + "=" + pval);
            
            // Put an idle timeout on the sink of 1s.
            properties.setProperty(pname, pval);
            
        }
        
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
     * Test with the "broken.rdf" data set (does not contain valid RDF). This
     * tests that the factory will shutdown correctly if there are processing
     * errors.
     * 
     * @throws Exception
     */
    public void test_loadFails() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/broken.rdf";

        final AbstractTripleStore store = getStore();
        try {
            
            if(!(store.getIndexManager() instanceof AbstractScaleOutFederation)) {
                
                log.warn("Test requires scale-out index views.");
                
                return;
                
            }

            if (store.isQuads()) {

                log.warn("Quads not supported yet.");
                
                return;
                
            }

            // only do load since we expect an error to be reported.
            final AsynchronousStatementBufferFactory<BigdataStatement, File> factory = doLoad2(
                    store, new File(resource), parallel);
            
            assertEquals("errorCount", 1, factory.getDocumentErrorCount());
            
        } finally {
            
            store.destroy();
            
        }

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
     * Uses a modest file (~40k statements).
     */
    public void test_loadAndVerify_modest() throws Exception {
        
//      final String file = "../rdf-data/nciOncology.owl";
//        final String file = "../rdf-data/alibaba_v41.rdf";
        final String file = "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt";

        if (!new File(file).exists()) {

            fail("Resource not found: " + file + ", test skipped: " + getName());

            return;
            
        }

        doLoadAndVerifyTest( file );
        
    }

    /**
     * LUBM U(1)
     */
    public void test_loadAndVerify_U1() throws Exception {
        
        final String file = "bigdata-rdf/src/resources/data/lehigh/U1";

        doLoadAndVerifyTest(file);
        
    }

//    /**
//     * FIXME Do not leave this unit test in -- it takes too long to validate the
//     * loaded data: LUBM U(10)
//     */
//    public void test_loadAndVerify_U10() throws Exception {
//        
//        final String file = "../rdf-data/lehigh/U10";
//
//        doLoadAndVerifyTest(file);
//        
//    }

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

        final AbstractTripleStore store = getStore();
        
        if (!(store.getIndexManager() instanceof AbstractScaleOutFederation)) {

            log.warn("Test requires scale-out index views.");
            
            return;
            
        }
        
//        if (store.isQuads()) {
//
//            log.warn("Quads not supported yet.");
//            
//            return;
//            
//        }
        
        try {

            doLoad(store, resource, parallel);

            if (log.isDebugEnabled()) {
        
                log.debug("dumping store...");
                
                log.debug("LEXICON:\n"
                                + store.getLexiconRelation().dumpTerms());
                
                // raw statement indices.
                {
                    
                    final Iterator<SPOKeyOrder> itr = store.isQuads() ? SPOKeyOrder
                            .quadStoreKeyOrderIterator()
                            : SPOKeyOrder.tripleStoreKeyOrderIterator();

                    while (itr.hasNext()) {
                     
                        final SPOKeyOrder keyOrder = itr.next();
                        
                        log.debug("\n---"+keyOrder + "---\n"
                                + store.getSPORelation().dump(keyOrder));
                    
                    }

                }

                // resolved statement indices.
                {

                    final Iterator<SPOKeyOrder> itr = store.isQuads() ? SPOKeyOrder
                            .quadStoreKeyOrderIterator()
                            : SPOKeyOrder.tripleStoreKeyOrderIterator();

                    while (itr.hasNext()) {

                        final SPOKeyOrder keyOrder = itr.next();

                        log.debug("\n" + keyOrder + "\n"
                                + store.getSPORelation().dump(keyOrder));

                        log.debug("\n---"+keyOrder+"---\n"
                                + store.dumpStore(store/* resolveTerms */,
                                        true/* explicit */, true/* inferred */,
                                        true/* axioms */, true/* justifications */,
                                        keyOrder));
                        
                    }

                }

            }

            doVerify(store, resource, parallel);

        } finally {

            store.__tearDownUnitTest();

        }

    }

    /**
     * Load using {@link AsynchronousStatementBufferWithoutSids2}.
     */
    protected void doLoad(final AbstractTripleStore store,
            final String resource, final boolean parallel) throws Exception {

        doLoad2(store, new File(resource), parallel);
        
    }

    /**
     * Load using {@link AsynchronousStatementBufferFactory}.
     */
    protected AsynchronousStatementBufferFactory<BigdataStatement,File> doLoad2(
            final AbstractTripleStore store, final File resource,
            final boolean parallel) throws Exception {

        final AsynchronousStatementBufferFactory<BigdataStatement,File> statementBufferFactory = new AsynchronousStatementBufferFactory<BigdataStatement,File>(
                (ScaleOutTripleStore) store,//
                chunkSize, //
                valuesInitialCapacity,//
                bnodesInitialCapacity,//
                RDFFormat.RDFXML, // defaultFormat
                false, // verifyData
                false, // deleteAfter
                parallel?5:1,  // parserPoolSize,
                20, // parserQueueCapacity
                parallel?5:1,  // term2IdWriterPoolSize,
                parallel?5:1,  // otherWriterPoolSize
                parallel?5:1,  // notifyPoolSize
                unbufferedStatementThreshold
                );

//        final AsynchronousWriteBufferFactoryWithoutSids2<BigdataStatement, File> statementBufferFactory = new AsynchronousWriteBufferFactoryWithoutSids2<BigdataStatement, File>(
//                (ScaleOutTripleStore) store, chunkSize, valuesInitialCapacity,
//                bnodesInitialCapacity);

        try {

            // tasks to load the resource or file(s)
            if (resource.isDirectory()) {

                statementBufferFactory.submitAll(resource,
                        new com.bigdata.rdf.load.RDFFilenameFilter(),
                        rejectedExecutionDelay);
                
            } else {
                
                statementBufferFactory.submitOne(resource);
                
            }

            // wait for the async writes to complete.
            statementBufferFactory.awaitAll();

            // dump write statistics for indices used by kb.
//            System.err.println(((AbstractFederation) store.getIndexManager())
//                    .getServiceCounterSet().getPath("Indices").toString());

            // dump factory specific counters.
            System.err.println(statementBufferFactory.getCounters().toString());
            
        } catch (Throwable t) {

            statementBufferFactory.cancelAll(true/* mayInterruptIfRunning */);

            // rethrow
            throw new RuntimeException(t);

        }

        return statementBufferFactory;
        
    }

}
