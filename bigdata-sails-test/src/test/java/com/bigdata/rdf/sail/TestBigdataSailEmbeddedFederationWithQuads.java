/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import java.io.File;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.tck.BigdataEmbeddedFederationSparqlTest;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.AbstractClient;
import com.bigdata.service.DistributedTransactionService;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;

/**
 * Test suite for the {@link BigdataSail} with quads enabled running against an
 * {@link EmbeddedFederation} with a single data service. The provenance mode is
 * disabled. Inference is disabled. This version of the test suite uses the
 * pipeline join algorithm.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBigdataSailEmbeddedFederationWithQuads extends
        AbstractBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataSailEmbeddedFederationWithQuads() {
    }

    public TestBigdataSailEmbeddedFederationWithQuads(String name) {
        super(name);
    }

    /**
     * Namespace used for the KB and the directory in which any data is written
     * for the test.
     */
    private static final String NAMESPACE = "TestBigdataSailEmbeddedFederationWithQuads";
    
    public static Test suite() {
        
        final TestBigdataSailEmbeddedFederationWithQuads delegate = new TestBigdataSailEmbeddedFederationWithQuads(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate, "SAIL with Quads (embedded federation)");

        // test rewrite of RDF Value => BigdataValue for binding set and tuple expr.
        suite.addTestSuite(TestBigdataValueReplacer.class);

        // test pruning of variables not required for downstream processing.
        suite.addTestSuite(TestPruneBindingSets.class);

        // misc named graph API stuff.
        suite.addTestSuite(TestQuadsAPI.class);
        
// Note: Ported to data driven test.
//        // SPARQL named graphs tests.
//        suite.addTestSuite(TestNamedGraphs.class);

        // test suite for optionals handling (left joins).
        suite.addTestSuite(TestOptionals.class);

        // test of the search magic predicate
        suite.addTestSuite(TestSearchQuery.class);
        
        // test of high-level query on a graph with statements about statements.
        suite.addTestSuite(TestProvenanceQuery.class);

        suite.addTestSuite(TestUnions.class);
        
		suite.addTestSuite(com.bigdata.rdf.sail.DavidsTestBOps.class);

        suite.addTestSuite(com.bigdata.rdf.sail.TestLexJoinOps.class);
        suite.addTestSuite(com.bigdata.rdf.sail.TestMaterialization.class);
 
        // The Sesame TCK, including the SPARQL test suite.
        {

            final TestSuite tckSuite = new TestSuite("Sesame 2.x TCK");

            /*
             * These test suites both rely on Sesame transaction semantics and
             * scale-out uses shard-wise group commit semantics, which are
             * different.
             */
//            tckSuite.addTestSuite(BigdataStoreTest.LTSWithPipelineJoins.class);
//
//            tckSuite.addTestSuite(BigdataConnectionTest.LTSWithPipelineJoins.class);

            try {

                /*
                 * suite() will call suiteLTSWithPipelineJoins() and then
                 * filter out the dataset tests, which we don't need right now
                 */
//                tckSuite.addTest(BigdataSparqlTest.suiteLTSWithPipelineJoins());
                tckSuite.addTest(BigdataEmbeddedFederationSparqlTest.suite());

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

            suite.addTest(tckSuite);

        }
        
        return suite;
        
    }

    @Override
    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());

        properties.setProperty(Options.QUADS_MODE, "true");
        
        properties.setProperty(Options.TRUTH_MAINTENANCE, "false");

        // Note: uses transient mode for tests.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        /*
         * Properties which are specific to the (embedded) federation.
         */

        /*
         * Only one data service. The EmbeddedFederation can not support more
         * than one FederatedQueryEngine peer because the EmbeddedClient gets
         * only a single ServiceID assigned.
         */
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");

        // when the data are persistent use the test to name the data directory.
        properties.setProperty(EmbeddedClient.Options.DATA_DIR, NAMESPACE);
        
        // when the data are persistent use the test to name the data directory.
        properties.setProperty(DistributedTransactionService.Options.DATA_DIR,
                new File(NAMESPACE, "txService").toString());
        
        /*
         * Disable the o/s specific statistics collection for the test run.
         * 
         * Note: You only need to enable this if you are trying to track the
         * statistics or if you are testing index partition moves, since moves
         * rely on the per-host counters collected from the o/s.
         */
        properties.setProperty(
                AbstractClient.Options.COLLECT_PLATFORM_STATISTICS, "false");

        // disable moves.
        properties.setProperty(
                ResourceManager.Options.MAXIMUM_MOVES_PER_TARGET, "0");

        return properties;
        
    }
    
    private Properties properties = null;
        
    @Override
    protected BigdataSail getSail(final Properties properties) {

        this.properties = properties;
        
		return new BigdataSail(openTripleStore(NAMESPACE, properties));

    }

    @Override
    protected BigdataSail reopenSail(final BigdataSail sail) {

//        final Properties properties = sail.getProperties();

        if (sail.isOpen()) {

            try {

                sail.shutDown();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }

        return getSail(properties);
        
    }

    /*
     * Embedded Federation Setup.
     */
    
    private IBigdataClient<?> client;
    private IBigdataFederation<?> fed;

    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    @SuppressWarnings("rawtypes")
    @Override
    protected void setUp(final ProxyBigdataSailTestCase testCase)
            throws Exception {

        final File dataDir = new File(NAMESPACE);

        if (dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete(dataDir);

        }

        client = new EmbeddedClient(getProperties());

        fed = client.connect();

    }

    @Override
    protected void tearDown(final ProxyBigdataSailTestCase testCase)
            throws Exception {

        if (fed != null) {

            fed.destroy();

            fed = null;

        }

        if (client != null) {

            client.disconnect(true/* immediateShutdown */);

            client = null;

        }
        
        properties = null;
        
        super.tearDown(testCase);

    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f A file or directory.
     */
    private void recursiveDelete(final File f) {
        
        if(f.isDirectory()) {
            
            final File[] children = f.listFiles();
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        if(log.isInfoEnabled())
            log.info("Removing: "+f);
        
        if (!f.delete())
            throw new RuntimeException("Could not remove: " + f);

    }

    /*
     * KB Setup.
     */

    /**
     * Create/re-open the repository.
     */
    private AbstractTripleStore openTripleStore(final String namespace,
            final Properties properties) {

        // locate the resource declaration (aka "open").
        AbstractTripleStore tripleStore = (AbstractTripleStore) fed
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            /*
             * Does not exist, so create it now.
             */
            tripleStore = new ScaleOutTripleStore(fed, namespace,
                    ITx.UNISOLATED, properties);

            // create the triple store.
            tripleStore.create();

        }

        return tripleStore;

    }

}
