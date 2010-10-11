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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;
import net.jini.config.ConfigurationException;

import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.dataset.DatasetRepository;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSail.Options;
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
 * @version $Id$
 */
public class BigdataEmbeddedFederationSparqlTest extends BigdataSparqlTest {

    public BigdataEmbeddedFederationSparqlTest(String testURI, String name, String queryFileURL,
            String resultFileURL, Dataset dataSet, boolean laxCardinality) {

        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality);
        
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
            suite1 = filterOutDataSetTests(suite1);
        
        return suite1;
        
    }
    
    /**
     * Return the test suite. 
     */
    public static TestSuite suiteEmbeddedFederation() throws Exception {
       
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return new BigdataEmbeddedFederationSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality) {

                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

//                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
//                                "false");

                        return p;

                    }
                    
                };

            }
        });
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

        if (cannotInlineTests.contains(testURI))
            properties.setProperty(Options.INLINE_LITERALS, "false");

        client = new EmbeddedClient(properties);

        fed = client.connect();

        final BigdataSail sail;
        try {

            sail = new BigdataSail(openTripleStore(NAMESPACE, properties));
            
        } catch (ConfigurationException e) {
            
            throw new RuntimeException(e);
            
        }

        return new DatasetRepository(new BigdataSailRepository(sail));

    }

    protected void tearDownBackend(IIndexManager backend) {
        
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
        
        System.err.println("Removing: "+f);
        
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
            final Properties properties) throws ConfigurationException {

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
