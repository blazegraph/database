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

import info.aduna.io.IOUtil;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;

import junit.framework.Test;

import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.relation.AbstractResource;

/**
 * Test harness for running the SPARQL test suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSparqlTest extends SPARQLQueryTest {

    /**
     * Use the {@link #suiteLTSWithNestedSubquery()} test suite by default.
     */
    public static Test suite() throws Exception {
        
        return suiteLTSWithNestedSubquery();
        
    }
    
    /**
     * Return a test suite using the {@link LocalTripleStore} and nested
     * subquery joins.
     */
    public static Test suiteLTSWithNestedSubquery() throws Exception {
        
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet) {

                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
                                "true");

                        return p;

                    }

                };

            }
        });
    }

    /**
     * Return a test suite using the {@link LocalTripleStore} and pipeline joins. 
     */
    public static Test suiteLTSWithPipelineJoins() throws Exception {
       
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet) {

                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
                                "false");

                        return p;

                    }
                    
                };

            }
        });
    }

    public BigdataSparqlTest(String testURI, String name, String queryFileURL,
            String resultFileURL, Dataset dataSet) {

        super(testURI, name, queryFileURL, resultFileURL, dataSet);
        
    }
    
    public String getTestURI() {
        return testURI;
    }
    
    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    public void tearDown()
        throws Exception
    {
        
        final IIndexManager backend = dataRep == null ? null
                : ((BigdataSailRepository) dataRep).getDatabase()
                        .getIndexManager();

        super.tearDown();

        if (backend != null)
            backend.destroy();

        /*
         * Note: this field MUST be cleared to null or the backing database
         * instance will be held by a hard reference throughout the execution of
         * all unit tests in this test suite!
         */

        dataRep = null;
    
    }

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     * 
     * @see #suiteLTSWithNestedSubquery()
     * @see #suiteLTSWithPipelineJoins()
     */
    protected Properties getProperties() {

        final Properties props = new Properties();
        
        final File journal = BigdataStoreTest.createTempFile();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());
/*        
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        
        props.setProperty(Options.QUADS, "true");
        
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
*/
        props.setProperty(Options.QUADS_MODE, "true");

        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        props.setProperty(Options.EXACT_SIZE, "true");
        
        return props;
        
    }
    
    @Override
    protected Repository newRepository() throws RepositoryException {

        final BigdataSail sail = new BigdataSail(getProperties());

        return new BigdataSailRepository(sail);
        
    }

    @Override
    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    public void runTest() throws Exception {
        super.runTest();
    }
    
    public Repository getRepository() {
        return dataRep;
    }
    
    private String queryString = null;
    public String getQueryString() throws Exception {
        if (queryString == null) {
            InputStream stream = new URL(queryFileURL).openStream();
            try {
                return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
            }
            finally {
                stream.close();
            }
        }
        return queryString;
    }
    
    

}
