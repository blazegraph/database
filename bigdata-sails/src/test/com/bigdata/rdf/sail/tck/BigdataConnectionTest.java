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
/* Note: Portions of this file are copyright by Aduna.
 * 
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnectionTest;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.contextaware.ContextAwareConnection;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Bigdata uses snapshot isolation for transactions while openrdf assumes that
 * any writes committed by a transaction become immediately visible to
 * transactions which are already running. Several unit tests from the base
 * class have to be overridden bigdata has stronger semantics for transactional
 * isolation.
 * 
 * @author mrpersonick
 * @author thompsonbry
 */
public class BigdataConnectionTest extends RepositoryConnectionTest {

    /**
     * When <code>true</code>, the unit tests for setDataset() with SPARQL
     * UPDATE are enabled.
     * 
     * FIXME setDataset() does not work correctly for UPDATE
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/593" >
     *      openrdf 2.6.9 </a>
     */
    private static boolean DATASET = false;

	@Override
	public void testDefaultContext()
		throws Exception
	{
		if (DATASET)
			super.testDefaultContext();
	}

	public void testDefaultInsertContext()
		throws Exception
	{
		if (DATASET)
			super.testDefaultInsertContext();
	}

	public void testExclusiveNullContext()
		throws Exception
	{
		if (DATASET)
			super.testExclusiveNullContext();
	}

    private static final Logger log = Logger.getLogger(BigdataConnectionTest.class);
    
	public BigdataConnectionTest(String name) {
		super(name);
	}
    
    /**
     * Return a test suite using the {@link LocalTripleStore} and pipeline
     * joins.
     */
    public static class LTSWithPipelineJoins extends BigdataConnectionTest {

        public LTSWithPipelineJoins(String name) {
            
            super(name);
            
        }
        
        @Override
        protected Properties getProperties() {
            
            final Properties p = new Properties(super.getProperties());
            
            return p;
            
        }

    }
    
	protected Properties getProperties() {
	    
        final Properties props = new Properties();
        
        final File journal = BigdataStoreTest.createTempFile();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

		/*
		 * WORM supports full tx right now. RW has tx issue which we plan to
		 * resolve shortly (5/5/2011).
		 */
		props.setProperty(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskWORM
				.toString());

/*        
        props.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
        
        props.setProperty(Options.QUADS, "true");
        
        props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
*/
        props.setProperty(BigdataSail.Options.STORE_BLANK_NODES,"true");
        
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
	protected Repository createRepository()
		throws IOException
	{
        
        final BigdataSail sail = new BigdataSail(getProperties());

        return new BigdataSailRepository(sail);
        
	}
		
    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    protected void tearDown()
        throws Exception
    {

        final IIndexManager backend = testRepository == null ? null
                : ((BigdataSailRepository) testRepository).getDatabase()
                        .getIndexManager();

        /*
         * Note: The code in the block below was taken verbatim from
         * super.testDown() in order to explore a tear down issue in testOpen().
         */
        super.tearDown();
//        {
//            
//            testCon2.close();
//            testCon2 = null;
//
//            testCon.close();
//            testCon = null;
//
//            testRepository.shutDown();
//            testRepository = null;
//
//            vf = null;
//
//        }

        if (backend != null)
            backend.destroy();

    }

    /**
	 * This test has been overridden because Sesame assumes "read-committed"
	 * transaction semantics while bidata uses snapshot isolation for its
	 * transactions.
	 */
    @Override
    public void testEmptyCommit()
        throws Exception
    {
        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
//      super.testEmptyCommit();
		assertTrue(testCon.isEmpty());
		assertTrue(testCon2.isEmpty());
		testCon.setAutoCommit(false);
		testCon.add(vf.createBNode(), vf.createURI("urn:pred"), vf.createBNode());
		assertFalse(testCon.isEmpty());
		assertTrue(testCon2.isEmpty());
		testCon.commit();
		assertFalse(testCon.isEmpty());
//		assertFalse(testCon2.isEmpty()); // No. This is read-committed semantics.
		assertTrue(testCon2.isEmpty()); // Yes. This is snapshot isolation semantics.
    }
    
	/**
	 * This test has been overridden because Sesame assumes "read-committed"
	 * transaction semantics while bidata uses snapshot isolation for its
	 * transactions.
	 */
    @Override
    public void testSizeCommit()
        throws Exception
    {
        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
//        super.testSizeCommit();
		assertEquals(0, testCon.size());
		assertEquals(0, testCon2.size());
		testCon.setAutoCommit(false);
		testCon.add(vf.createBNode(), vf.createURI("urn:pred"), vf.createBNode());
		assertEquals(1, testCon.size());
		assertEquals(0, testCon2.size());
		testCon.add(vf.createBNode(), vf.createURI("urn:pred"), vf.createBNode());
		assertEquals(2, testCon.size());
		assertEquals(0, testCon2.size());
		testCon.commit();
		assertEquals(2, testCon.size());
//		assertEquals(2, testCon2.size()); // No. read-committed semantics.
		assertEquals(0, testCon2.size()); // Yes. snapshot isolation.
    }

	/**
	 * This test has been overridden because Sesame assumes "read-committed"
	 * transaction semantics while bidata uses snapshot isolation for its
	 * transactions.
	 */
    @Override
    public void testTransactionIsolation()
        throws Exception
    {
        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
//        super.testTransactionIsolation();

        testCon.setAutoCommit(false);
		testCon.add(bob, name, nameBob);

		assertTrue(testCon.hasStatement(bob, name, nameBob, false));
		assertFalse(testCon2.hasStatement(bob, name, nameBob, false));

		testCon.commit();

		assertTrue(testCon.hasStatement(bob, name, nameBob, false));
//		assertTrue(testCon2.hasStatement(bob, name, nameBob, false)); // No. This is read-committed semantics.
		assertFalse(testCon2.hasStatement(bob, name, nameBob, false)); // Yes. This is snapshot isolation semantics.

    }

//    /**
//     * Copied into the local test suite unchanged in order to debug with this
//     * test.
//     */
//    @Override
//    public void testOpen() throws Exception {
//        assertTrue(testCon.isOpen());
//        assertTrue(testCon2.isOpen());
//        testCon.close();
//        assertFalse(testCon.isOpen());
//        assertTrue(testCon2.isOpen());
//    }
    
    /**
     * Modified to test SPARQL instead of Serql.
     */
    @Override
	public void testSimpleTupleQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
	
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" SELECT name, mbox");
//		queryBuilder.append(" FROM {} foaf:name {name};");
//		queryBuilder.append("         foaf:mbox {mbox}");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?name ?mbox");
		queryBuilder.append(" WHERE {");
		queryBuilder.append(" ?x foaf:name ?name .");
		queryBuilder.append(" ?x foaf:mbox ?mbox .");
		queryBuilder.append(" }");
		
		
	
		TupleQueryResult result = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertTrue(solution.hasBinding("name"));
				assertTrue(solution.hasBinding("mbox"));
	
				Value nameResult = solution.getValue("name");
				Value mboxResult = solution.getValue("mbox");
	
				assertTrue((nameAlice.equals(nameResult) || nameBob.equals(nameResult)));
				assertTrue((mboxAlice.equals(mboxResult) || mboxBob.equals(mboxResult)));
			}
		}
		finally {
			result.close();
		}
	}

    /**
     * This is a test of simply preparing a SeRQL query into a TupleExpr, no
     * data, no evaluation.  Since we don't support SeRQL anymore, it does
     * not seem worthwhile to port this one.
     */
    @Override
	public void testPrepareSeRQLQuery()
		throws Exception
	{
	}
    
    /**
     * Modified to test SPARQL instead of Serql.
     */
	public void testSimpleTupleQueryUnicode()
		throws Exception
	{
/*		
 This is commented out until we fix the unicode problem.
 
		testCon.add(alexander, name, <UNICODE VAR FROM SUPER>);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" SELECT person");
//		queryBuilder.append(" FROM {person} foaf:name {").append(<UNICODE VAR FROM SUPER>.getLabel()).append("}");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?person");
		queryBuilder.append(" where { ?person foaf:name \"").append(<UNICODE VAR FROM SUPER>.getLabel()).append("\" . }");
	
		
		
		TupleQueryResult result = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertTrue(solution.hasBinding("person"));
				assertEquals(alexander, solution.getValue("person"));
			}
		}
		finally {
			result.close();
		}
*/
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	public void testPreparedTupleQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
	
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" SELECT name, mbox");
//		queryBuilder.append(" FROM {} foaf:name {name};");
//		queryBuilder.append("         foaf:mbox {mbox}");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?name ?mbox");
		queryBuilder.append(" WHERE { ?x foaf:name ?name .");
		queryBuilder.append("         ?x foaf:mbox ?mbox . }");

		
		
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("name", nameBob);
	
		TupleQueryResult result = query.evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertTrue(solution.hasBinding("name"));
				assertTrue(solution.hasBinding("mbox"));
	
				Value nameResult = solution.getValue("name");
				Value mboxResult = solution.getValue("mbox");
	
				assertEquals("unexpected value for name: " + nameResult, nameBob, nameResult);
				assertEquals("unexpected value for mbox: " + mboxResult, mboxBob, mboxResult);
			}
		}
		finally {
			result.close();
		}
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	public void testPreparedTupleQuery2()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
	
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" SELECT name, mbox");
//		queryBuilder.append(" FROM {p} foaf:name {name};");
//		queryBuilder.append("         foaf:mbox {mbox}");
//		queryBuilder.append(" WHERE p = VAR");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?name ?mbox");
		queryBuilder.append(" WHERE { ?VAR foaf:name ?name .");
		queryBuilder.append("         ?VAR foaf:mbox ?mbox . }");

		
		
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("VAR", bob);
	
		TupleQueryResult result = query.evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertTrue(solution.hasBinding("name"));
				assertTrue(solution.hasBinding("mbox"));
	
				Value nameResult = solution.getValue("name");
				Value mboxResult = solution.getValue("mbox");
	
				assertEquals("unexpected value for name: " + nameResult, nameBob, nameResult);
				assertEquals("unexpected value for mbox: " + mboxResult, mboxBob, mboxResult);
			}
		}
		finally {
			result.close();
		}
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	public void testPreparedTupleQueryUnicode()
		throws Exception
	{
/*		
 This is commented out until we fix the unicode problem.
		 
		testCon.add(alexander, name, <UNICODE VAR FROM SUPER>);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" SELECT person");
//		queryBuilder.append(" FROM {person} foaf:name {name}");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" SELECT ?person");
		queryBuilder.append(" WHERE { ?person foaf:name ?name . }");

		
		
		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("name", <UNICODE VAR FROM SUPER>);
	
		TupleQueryResult result = query.evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertTrue(solution.hasBinding("person"));
				assertEquals(alexander, solution.getValue("person"));
			}
		}
		finally {
			result.close();
		}
*/
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	public void testSimpleGraphQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
	
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" CONSTRUCT *");
//		queryBuilder.append(" FROM {} foaf:name {name};");
//		queryBuilder.append("         foaf:mbox {mbox}");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" CONSTRUCT { ?x foaf:name ?name .");
		queryBuilder.append("             ?x foaf:mbox ?mbox . }");
		queryBuilder.append(" WHERE { ?x foaf:name ?name .");
		queryBuilder.append("         ?x foaf:mbox ?mbox . }");

		
		
		GraphQueryResult result = testCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				Statement st = result.next();
				if (name.equals(st.getPredicate())) {
					assertTrue(nameAlice.equals(st.getObject()) || nameBob.equals(st.getObject()));
				}
				else {
					assertTrue(mbox.equals(st.getPredicate()));
					assertTrue(mboxAlice.equals(st.getObject()) || mboxBob.equals(st.getObject()));
				}
			}
		}
		finally {
			result.close();
		}
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	public void testPreparedGraphQuery()
		throws Exception
	{
		testCon.add(alice, name, nameAlice, context2);
		testCon.add(alice, mbox, mboxAlice, context2);
		testCon.add(context2, publisher, nameAlice);
	
		testCon.add(bob, name, nameBob, context1);
		testCon.add(bob, mbox, mboxBob, context1);
		testCon.add(context1, publisher, nameBob);
	
		StringBuilder queryBuilder = new StringBuilder();
//		queryBuilder.append(" CONSTRUCT *");
//		queryBuilder.append(" FROM {} foaf:name {name};");
//		queryBuilder.append("         foaf:mbox {mbox}");
//		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
		queryBuilder.append(" CONSTRUCT { ?x foaf:name ?name .");
		queryBuilder.append("             ?x foaf:mbox ?mbox . }");
		queryBuilder.append(" WHERE { ?x foaf:name ?name .");
		queryBuilder.append("         ?x foaf:mbox ?mbox . }");

		
		
		GraphQuery query = testCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("name", nameBob);
	
		GraphQueryResult result = query.evaluate();
	
		try {
			assertTrue(result != null);
			assertTrue(result.hasNext());
	
			while (result.hasNext()) {
				Statement st = result.next();
				assertTrue(name.equals(st.getPredicate()) || mbox.equals(st.getPredicate()));
				if (name.equals(st.getPredicate())) {
					assertTrue("unexpected value for name: " + st.getObject(), nameBob.equals(st.getObject()));
				}
				else {
					assertTrue(mbox.equals(st.getPredicate()));
					assertTrue("unexpected value for mbox: " + st.getObject(), mboxBob.equals(st.getObject()));
				}
	
			}
		}
		finally {
			result.close();
		}
	}

    

}
