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
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.dataset.DatasetRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;
import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.IIndexManager;
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
     * We cannot use inlining for these test because we do normalization on
     * numeric values and these tests test for syntatic differences, i.e.
     * 01 != 1.
     */
    private static Collection<String> cannotInlineTests = Arrays.asList(new String[] {
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-01",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-03",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-04",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-datatype-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-simple",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-eq",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-not-eq",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#no-distinct-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#no-distinct-9",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-9",
    });
    
    /**
     * Use the {@link #suiteLTSWithPipelineJoins()} test suite by default.
     * <p>
     * Skip the dataset tests for now until we can figure out what is wrong
     * with them.
     * 
     * @todo FIXME fix the dataset tests 
     */
    public static Test suite() throws Exception {
        
        return suite(false /*hideDatasetTests*/);
        
    }
    
    public static Test suite(final boolean hideDatasetTests) throws Exception {
        
        TestSuite suite1 = suiteLTSWithPipelineJoins();

        if (!hideDatasetTests) {
            
            return suite1;
            
        }
        
        TestSuite suite2 = new TestSuite(suite1.getName());
        
        Enumeration<TestSuite> e = suite1.tests();
        
        while (e.hasMoreElements()) {
            
            TestSuite suite3 = e.nextElement();
            
            if (suite3.getName().equals("dataset") == false) {
                
                suite2.addTest(suite3);
                
            }
        }
        
        return suite2;
        
    }
    
    /**
     * Return a test suite using the {@link LocalTripleStore} and nested
     * subquery joins.
     */
    public static TestSuite suiteLTSWithNestedSubquery() throws Exception {
        
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality) {

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
    public static TestSuite suiteLTSWithPipelineJoins() throws Exception {
       
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality) {

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
            String resultFileURL, Dataset dataSet, boolean laxCardinality) {

        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality);
        
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
/*
        StringBuilder message = new StringBuilder();
        message.append("data:\n");

        RepositoryConnection cxn = dataRep.getConnection();
        try {
            RepositoryResult<Statement> stmts = cxn.getStatements(null, null, null, true);
            while (stmts.hasNext()) {
                Statement stmt = stmts.next();
                message.append(stmt+"\n");
            }
        } finally {
            cxn.close();
        }
        SPARQLQueryTest.logger.error(message.toString());
*/        
        IIndexManager backend = null;
        
        Repository delegate = dataRep == null ? null : ((DatasetRepository) dataRep).getDelegate();
        
        if (delegate != null && delegate instanceof BigdataSailRepository) {
            
            backend = ((BigdataSailRepository) delegate).getDatabase()
                            .getIndexManager();
            
        }

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

        // quads mode: quads=true, sids=false, axioms=NoAxioms, vocab=NoVocabulary
        props.setProperty(Options.QUADS_MODE, "true");

        // no justifications
        props.setProperty(Options.JUSTIFY, "false");
        
        // no query time inference
        props.setProperty(Options.QUERY_TIME_EXPANDER, "false");
        
        // auto-commit only there for TCK
        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        // exact size only there for TCK
        props.setProperty(Options.EXACT_SIZE, "true");
        
        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
        // enable read/write transactions
        props.setProperty(Options.ISOLATABLE_INDICES, "true");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        return props;
        
    }
    
    @Override
    protected Repository newRepository() throws RepositoryException {

        if (true) {
            final Properties props = getProperties();
            
            if (cannotInlineTests.contains(testURI))
                props.setProperty(Options.INLINE_LITERALS, "false");
            
            final BigdataSail sail = new BigdataSail(props);
            return new DatasetRepository(new BigdataSailRepository(sail));
        } else {
            return new DatasetRepository(new SailRepository(new MemoryStore()));
        }
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
