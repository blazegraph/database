/*

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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.manifest.ManifestTest;
import org.openrdf.query.parser.sparql.manifest.SPARQL11ManifestTest;
import org.openrdf.query.parser.sparql.manifest.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
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
 * Test harness for running the SPARQL test suites against an
 * {@link EmbeddedFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataEmbeddedFederationSparqlTest extends BigdataSparqlTest {

    private static final Logger log = Logger
            .getLogger(BigdataEmbeddedFederationSparqlTest.class);

    public BigdataEmbeddedFederationSparqlTest(String testURI, String name,
            String queryFileURL, String resultFileURL, Dataset dataSet,
            boolean laxCardinality, boolean checkOrder) {

        super(testURI, name, queryFileURL, resultFileURL, dataSet,
                laxCardinality, checkOrder);

    }
    
    /**
     * Skip the dataset tests for now until we can figure out what is wrong with
     * them.
     * 
     * FIXME Fix the dataset tests. There is some problem in how the data to be
     * loaded into the fixture is being resolved in these tests.
     */
    public static Test suite() throws Exception {
        
        return suite(true /*hideDatasetTests*/);
        
    }
    
    public static Test suite(final boolean hideDatasetTests) throws Exception {
        
        TestSuite suite1 = suiteEmbeddedFederation();

        // Only run the specified tests?
        if (!testURIs.isEmpty()) {
            final TestSuite suite = new TestSuite();
            for (String s : testURIs) {
                suite.addTest(getSingleTest(suite1, s));
            }
            return suite;
        }
        
        if(hideDatasetTests)
            suite1 = filterOutTests(suite1,"dataset");

//        suite1 = filterOutTests(suite1, "property-paths");
        
        /**
         * BSBM BI use case query 5
         * 
         * bsbm-bi-q5
         * 
         * We were having a problem with this query which I finally tracked this
         * down to an error in the logic to decide on a merge join. The story is
         * documented at the trac issue below. However, even after all that the
         * predicted result for openrdf differs at the 4th decimal place. I have
         * therefore filtered out this test from the openrdf TCK.
         * 
         * <pre>
         * Missing bindings: 
         * [product=http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product63;
         * nrOfReviews="3"^^<http://www.w3.org/2001/XMLSchema#integer>;
         * avgPrice="4207.426"^^<http://www.w3.org/2001/XMLSchema#float>;
         * country=http://downlode.org/rdf/iso-3166/countries#RU]
         * ====================================================
         * Unexpected bindings: 
         * [country=http://downlode.org/rdf/iso-3166/countries#RU;
         * product=http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product63;
         * nrOfReviews="3"^^<http://www.w3.org/2001/XMLSchema#integer>;
         * avgPrice="4207.4263"^^<http://www.w3.org/2001/XMLSchema#float>]
         * </pre>
         * 
         * @see <a
         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/534#comment:2">
         *      BSBM BI Q5 Error when using MERGE JOIN </a>
         */
        suite1 = BigdataSparqlTest.filterOutTests(
                suite1,
                "bsbm"
                );

        return suite1;
        
    }
    
    /**
     * Return the test suite. 
     */
    public static TestSuite suiteEmbeddedFederation() throws Exception {
       
        final SPARQLQueryTest.Factory factory = new SPARQLQueryTest.Factory() {

            @Override
            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {
                
                return createSPARQLQueryTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality, true/* checkOrder */);
                
            }

            @Override
            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality, boolean checkOrder) {

                return new BigdataEmbeddedFederationSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality, checkOrder) {

                    @Override
                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

//                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
//                                "false");

                        return p;

                    }
                    
                };

            }
        };
        
        final TestSuite suite = new TestSuite();

        // SPARQL 1.0
        suite.addTest(ManifestTest.suite(factory));

        // SPARQL 1.1
        suite.addTest(SPARQL11ManifestTest.suite(factory, true, true, false));

        return suite;
        
    }

    private final String NAMESPACE = getName();
    
    @Override
    protected Properties getProperties() {

        final Properties properties = super.getProperties();
        
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
        
        // disable read/write transactions (not supported in scale-out).
        properties.setProperty(Options.ISOLATABLE_INDICES, "false");

        return properties;
        
    }
    
    @Override
    protected Repository newRepository() throws RepositoryException {

        /*
         * Data files are placed into a directory named by the test. If the
         * directory exists, then it is removed before the federation is set up.
         */
        final File dataDir = new File(NAMESPACE);

        if (dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete(dataDir);

        }

        final Properties properties = getProperties();

        if (cannotInlineTests.contains(testURI)) {
        	properties.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");
        	properties.setProperty(Options.INLINE_DATE_TIMES, "false");
        }
        
        if(unicodeStrengthIdentical.contains(testURI)) {
            // Force identical Unicode comparisons.
            properties.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
            properties.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        }

        client = new EmbeddedClient(properties);

        fed = client.connect();

        final BigdataSail sail;

        sail = new BigdataSail(openTripleStore(NAMESPACE, properties));
            
        // See #1196 (Enable BigdataEmbeddedFederationSparqlTest tests in CI)
//        return new DatasetRepository(new BigdataSailRepository(sail));
        return new BigdataSailRepository(sail);

    }

    @Override
    protected void tearDownBackend(final IIndexManager backend) {
        
        backend.destroy();
        
        if (client != null) {

            client.disconnect(true/* immediateShutdown */);

            client = null;
            
        }
        
        fed = null;

    }
    
    /*
     * Embedded Federation Setup.
     */
    
    private IBigdataClient<?> client;
    private IBigdataFederation<?> fed;
    
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
