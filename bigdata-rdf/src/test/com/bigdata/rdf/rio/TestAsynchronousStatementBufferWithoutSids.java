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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.load.RDFFileLoadTask;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.AsynchronousStatementBufferWithoutSids.AsynchronousWriteBufferFactoryWithoutSids;
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
 * FIXME varient to test w/ and w/o the full text index (with lookup by tokens).
 * 
 * FIXME variant to test async w/ sids (once written).
 * 
 * @todo The async API is only defined at this time for scale-out index views,
 *       so maybe move this into the scale-out proxy test suite.
 * 
 * @see TestScaleOutTripleStoreWithEmbeddedFederation
 * @see RDFFileLoadTask
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

    // FIXME chunkSize parameter.
    final int chunkSize = 20000;

    // FIXME syncRPCForTERM2ID parameter.
    final boolean syncRPCForTERM2ID = true;
    
    /**
     * SHOULD be <code>true</code> since the whole point of this is higher
     * concurrency. If you set this to <code>false</code> to explore some
     * issue, then change it back to <code>true</code> when you are done!
     */
    final boolean parallel = true;
    
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

        if(!syncRPCForTERM2ID) {

            /*
             * FIXME We MUST specify the KB namespace so we can override this
             * property.
             */
            final String namespace = "test1";

            // Put a timeout on the chunks of 1s.
            properties.setProperty(com.bigdata.config.Configuration
                    .getOverrideProperty(namespace + "."
                            + LexiconRelation.NAME_LEXICON_RELATION + "."
                            + LexiconKeyOrder.TERM2ID,
                            IndexMetadata.Options.SINK_CHUNK_TIMEOUT_NANOS), ""
                    + TimeUnit.SECONDS.toNanos(1));
            
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
     * LUBM U(1)
     */
    public void test_loadAndVerify_U1() throws Exception {
        
        final String file = "bigdata-rdf/src/resources/data/lehigh/U1";

        doLoadAndVerifyTest(file);
        
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

            doLoad(store, resource, parallel);

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

            doVerify(store, resource, parallel);

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * Load using {@link AsynchronousStatementBufferWithoutSids}.
     */
    protected void doLoad(final AbstractTripleStore store,
            final String resource, final boolean parallel)
            throws Exception {

        final AsynchronousWriteBufferFactoryWithoutSids<BigdataStatement> factory = new AsynchronousWriteBufferFactoryWithoutSids<BigdataStatement>(
                (ScaleOutTripleStore) store, chunkSize, syncRPCForTERM2ID);

        try {

            doLoad(store, resource, parallel, factory);
            
            // wait for the async writes to complete.
            factory.awaitAll();

            // dump write statistics for indices used by kb.
            System.err.println(factory.getCounters());

        } catch (Throwable t) {

            factory.cancelAll(true/* mayInterruptIfRunning */);

            // rethrow
            throw new RuntimeException(t);

        }

    }

}
