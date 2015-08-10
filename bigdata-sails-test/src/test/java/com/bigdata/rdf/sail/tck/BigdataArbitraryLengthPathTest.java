/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2012.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.tck;

import java.util.Properties;

import junit.framework.TestCase;

import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Extreme test for problem with arbitrary length paths. This test is apparently
 * not intended to run during normal CI but instead looks as if it were
 * developed to address a specific openrdf issue.
 * 
 * @see <a href="http://www.openrdf.org/issues/browse/SES-956">
 *      StackOverflowError for ArbitraryLengthPath </a>
 * 
 * @author james
 * 
 * @see ArbitraryLengthPathTest
 * @since openrdf 2.6.10
 */
public class BigdataArbitraryLengthPathTest extends TestCase {

    private Repository repo;

    private RepositoryConnection con;

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     */
    protected Properties getProperties() {

        final Properties props = new Properties();
        
//        final File journal = BigdataStoreTest.createTempFile();
//        
//        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

        props.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
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
        
//        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
//        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
        /*
         * disable read/write transactions since this class runs against the
         * unisolated connection.
         */
        props.setProperty(Options.ISOLATABLE_INDICES, "false");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        return props;
        
    }
    
//    @Override
    protected SailRepository newRepository() throws RepositoryException {

        if (true) {
            final Properties props = getProperties();
            
//            if (cannotInlineTests.contains(testURI)){
//                // The test can not be run using XSD inlining.
//                props.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");
//                props.setProperty(Options.INLINE_DATE_TIMES, "false");
//            }
//            
//            if(unicodeStrengthIdentical.contains(testURI)) {
//                // Force identical Unicode comparisons.
//                props.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
//                props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
//            }
            
            final BigdataSail sail = new BigdataSail(props);
            return new BigdataSailRepository(sail);
        } else {
            /*
             * Run against openrdf.
             */
            SailRepository repo = new SailRepository(new MemoryStore());

            return repo;
        }
    }
    
//    @Before
    public void setUp()
        throws Exception
    {
        repo = newRepository();
        repo.initialize();
        con = repo.getConnection();
    }

//    @After
    public void tearDown()
        throws Exception
    {
        con.close();
        repo.shutDown();
        con = null;
        repo = null;
    }

//    @Test
    public void test10()
        throws Exception
    {
        populate(10);
        String sparql = "ASK { <urn:test:root> <urn:test:hasChild>* <urn:test:node-end> }";
        assertTrue(con.prepareBooleanQuery(QueryLanguage.SPARQL, sparql).evaluate());
    }

//    @Test
    public void test100()
        throws Exception
    {
        populate(100);
        String sparql = "ASK { <urn:test:root> <urn:test:hasChild>* <urn:test:node-end> }";
        assertTrue(con.prepareBooleanQuery(QueryLanguage.SPARQL, sparql).evaluate());
    }

//    @Test
    public void test1000()
        throws Exception
    {
        populate(1000);
        String sparql = "ASK { <urn:test:root> <urn:test:hasChild>* <urn:test:node-end> }";
        assertTrue(con.prepareBooleanQuery(QueryLanguage.SPARQL, sparql).evaluate());
    }

//    @Test
    public void test10000()
        throws Exception
    {
        populate(10000);
        String sparql = "ASK { <urn:test:root> <urn:test:hasChild>* <urn:test:node-end> }";
        assertTrue(con.prepareBooleanQuery(QueryLanguage.SPARQL, sparql).evaluate());
    }

//    @Test
    public void test100000()
        throws Exception
    {
        populate(100000);
        String sparql = "ASK { <urn:test:root> <urn:test:hasChild>* <urn:test:node-end> }";
        assertTrue(con.prepareBooleanQuery(QueryLanguage.SPARQL, sparql).evaluate());
    }

    private void populate(int n)
        throws RepositoryException
    {
        ValueFactory vf = con.getValueFactory();
        for (int i = 0; i < n; i++) {
            con.add(vf.createURI("urn:test:root"), vf.createURI("urn:test:hasChild"),
                    vf.createURI("urn:test:node" + i));
        }
        con.add(vf.createURI("urn:test:root"), vf.createURI("urn:test:hasChild"),
                vf.createURI("urn:test:node-end"));
    }

}
