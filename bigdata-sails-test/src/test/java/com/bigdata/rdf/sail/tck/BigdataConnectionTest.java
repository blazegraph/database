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
/* Note: Portions of this file are copyright by Aduna.
 * 
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnectionTest;

import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Bigdata uses snapshot isolation for transactions while openrdf assumes that
 * any writes committed by a transaction become immediately visible to
 * transactions which are already running. Several unit tests from the base
 * class have to be overridden bigdata has stronger semantics for transactional
 * isolation.
 * 
 * @author mrpersonick
 * @author thompsonbry
 * @openrdf
 */
public class BigdataConnectionTest extends RepositoryConnectionTest {

//    /**
//     * When <code>true</code>, the unit tests for setDataset() with SPARQL
//     * UPDATE are enabled.
//     * 
//     * FIXME setDataset() does not work correctly for UPDATE
//     * 
//     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/593" >
//     *      openrdf 2.6.9 </a>
//     */
//    private static boolean DATASET = false;
//
//	@Override
//	public void testDefaultContext()
//		throws Exception
//	{
//		if (DATASET)
//			super.testDefaultContext();
//	}
//
//	public void testDefaultInsertContext()
//		throws Exception
//	{
//		if (DATASET)
//			super.testDefaultInsertContext();
//	}
//
//	public void testExclusiveNullContext()
//		throws Exception
//	{
//		if (DATASET)
//			super.testExclusiveNullContext();
//	}
//
    private static final Logger log = Logger.getLogger(BigdataConnectionTest.class);
    
	public BigdataConnectionTest(String name) {
		super(name);
	}
    
//    /**
//     * Return a test suite using the {@link LocalTripleStore} and pipeline
//     * joins.
//     */
//    public static class LTSWithPipelineJoins extends BigdataConnectionTest {
//
//        public LTSWithPipelineJoins(String name) {
//            
//            super(name);
//            
//        }
//        
//        @Override
//        protected Properties getProperties() {
//            
//            final Properties p = new Properties(super.getProperties());
//            
//            return p;
//            
//        }
//
//    }
    
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
    public void tearDown() throws Exception
    {

        final IIndexManager backend = testRepository == null ? null
                : ((BigdataSailRepository) testRepository).getSail()
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

        if (backend != null) {
            if(log.isInfoEnabled() && backend instanceof Journal)
                log.info(QueryEngineFactory.getInstance().getExistingQueryController((Journal)backend).getCounters());
            backend.destroy();
        }

    }
//
//    /**
//	 * This test has been overridden because Sesame assumes "read-committed"
//	 * transaction semantics while bidata uses snapshot isolation for its
//	 * transactions.
//	 */
//    @Override
//    public void testEmptyCommit()
//        throws Exception
//    {
//        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
////      super.testEmptyCommit();
//		assertTrue(testCon.isEmpty());
//		assertTrue(testCon2.isEmpty());
//		testCon.setAutoCommit(false);
//		testCon.add(vf.createBNode(), vf.createURI("urn:pred"), vf.createBNode());
//		assertFalse(testCon.isEmpty());
//		assertTrue(testCon2.isEmpty());
//		testCon.commit();
//		assertFalse(testCon.isEmpty());
////		assertFalse(testCon2.isEmpty()); // No. This is read-committed semantics.
//		assertTrue(testCon2.isEmpty()); // Yes. This is snapshot isolation semantics.
//    }
//    
//	/**
//	 * This test has been overridden because Sesame assumes "read-committed"
//	 * transaction semantics while bidata uses snapshot isolation for its
//	 * transactions.
//	 */
//    @Override
//    public void testSizeCommit()
//        throws Exception
//    {
//        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
////        super.testSizeCommit();
//		assertEquals(0, testCon.size());
//		assertEquals(0, testCon2.size());
//		testCon.setAutoCommit(false);
//		testCon.add(vf.createBNode(), vf.createURI("urn:pred"), vf.createBNode());
//		assertEquals(1, testCon.size());
//		assertEquals(0, testCon2.size());
//		testCon.add(vf.createBNode(), vf.createURI("urn:pred"), vf.createBNode());
//		assertEquals(2, testCon.size());
//		assertEquals(0, testCon2.size());
//		testCon.commit();
//		assertEquals(2, testCon.size());
////		assertEquals(2, testCon2.size()); // No. read-committed semantics.
//		assertEquals(0, testCon2.size()); // Yes. snapshot isolation.
//    }
//
//	/**
//	 * This test has been overridden because Sesame assumes "read-committed"
//	 * transaction semantics while bidata uses snapshot isolation for its
//	 * transactions.
//	 */
//    @Override
//    public void testTransactionIsolation()
//        throws Exception
//    {
//        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
////        super.testTransactionIsolation();
//
//        testCon.setAutoCommit(false);
//		testCon.add(bob, name, nameBob);
//
//		assertTrue(testCon.hasStatement(bob, name, nameBob, false));
//		assertFalse(testCon2.hasStatement(bob, name, nameBob, false));
//
//		testCon.commit();
//
//		assertTrue(testCon.hasStatement(bob, name, nameBob, false));
////		assertTrue(testCon2.hasStatement(bob, name, nameBob, false)); // No. This is read-committed semantics.
//		assertFalse(testCon2.hasStatement(bob, name, nameBob, false)); // Yes. This is snapshot isolation semantics.
//
//    }
//
////    /**
////     * Copied into the local test suite unchanged in order to debug with this
////     * test.
////     */
////    @Override
////    public void testOpen() throws Exception {
////        assertTrue(testCon.isOpen());
////        assertTrue(testCon2.isOpen());
////        testCon.close();
////        assertFalse(testCon.isOpen());
////        assertTrue(testCon2.isOpen());
////    }
//    
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//    @Override
//	public void testSimpleTupleQuery()
//		throws Exception
//	{
//		testCon.add(alice, name, nameAlice, context2);
//		testCon.add(alice, mbox, mboxAlice, context2);
//		testCon.add(context2, publisher, nameAlice);
//	
//		testCon.add(bob, name, nameBob, context1);
//		testCon.add(bob, mbox, mboxBob, context1);
//		testCon.add(context1, publisher, nameBob);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" SELECT name, mbox");
////		queryBuilder.append(" FROM {} foaf:name {name};");
////		queryBuilder.append("         foaf:mbox {mbox}");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" SELECT ?name ?mbox");
//		queryBuilder.append(" WHERE {");
//		queryBuilder.append(" ?x foaf:name ?name .");
//		queryBuilder.append(" ?x foaf:mbox ?mbox .");
//		queryBuilder.append(" }");
//		
//		
//	
//		TupleQueryResult result = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				BindingSet solution = result.next();
//				assertTrue(solution.hasBinding("name"));
//				assertTrue(solution.hasBinding("mbox"));
//	
//				Value nameResult = solution.getValue("name");
//				Value mboxResult = solution.getValue("mbox");
//	
//				assertTrue((nameAlice.equals(nameResult) || nameBob.equals(nameResult)));
//				assertTrue((mboxAlice.equals(mboxResult) || mboxBob.equals(mboxResult)));
//			}
//		}
//		finally {
//			result.close();
//		}
//	}
//
//    /**
//     * This is a test of simply preparing a SeRQL query into a TupleExpr, no
//     * data, no evaluation.  Since we don't support SeRQL anymore, it does
//     * not seem worthwhile to port this one.
//     */
//    @Override
//	public void testPrepareSeRQLQuery()
//		throws Exception
//	{
//	}
//    
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//	public void testSimpleTupleQueryUnicode()
//		throws Exception
//	{
///*		
// This is commented out until we fix the unicode problem.
// 
//		testCon.add(alexander, name, <UNICODE VAR FROM SUPER>);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" SELECT person");
////		queryBuilder.append(" FROM {person} foaf:name {").append(<UNICODE VAR FROM SUPER>.getLabel()).append("}");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" SELECT ?person");
//		queryBuilder.append(" where { ?person foaf:name \"").append(<UNICODE VAR FROM SUPER>.getLabel()).append("\" . }");
//	
//		
//		
//		TupleQueryResult result = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				BindingSet solution = result.next();
//				assertTrue(solution.hasBinding("person"));
//				assertEquals(alexander, solution.getValue("person"));
//			}
//		}
//		finally {
//			result.close();
//		}
//*/
//	}
//	
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//	public void testPreparedTupleQuery()
//		throws Exception
//	{
//		testCon.add(alice, name, nameAlice, context2);
//		testCon.add(alice, mbox, mboxAlice, context2);
//		testCon.add(context2, publisher, nameAlice);
//	
//		testCon.add(bob, name, nameBob, context1);
//		testCon.add(bob, mbox, mboxBob, context1);
//		testCon.add(context1, publisher, nameBob);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" SELECT name, mbox");
////		queryBuilder.append(" FROM {} foaf:name {name};");
////		queryBuilder.append("         foaf:mbox {mbox}");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" SELECT ?name ?mbox");
//		queryBuilder.append(" WHERE { ?x foaf:name ?name .");
//		queryBuilder.append("         ?x foaf:mbox ?mbox . }");
//
//		
//		
//		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
//		query.setBinding("name", nameBob);
//	
//		TupleQueryResult result = query.evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				BindingSet solution = result.next();
//				assertTrue(solution.hasBinding("name"));
//				assertTrue(solution.hasBinding("mbox"));
//	
//				Value nameResult = solution.getValue("name");
//				Value mboxResult = solution.getValue("mbox");
//	
//				assertEquals("unexpected value for name: " + nameResult, nameBob, nameResult);
//				assertEquals("unexpected value for mbox: " + mboxResult, mboxBob, mboxResult);
//			}
//		}
//		finally {
//			result.close();
//		}
//	}
//	
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//	public void testPreparedTupleQuery2()
//		throws Exception
//	{
//		testCon.add(alice, name, nameAlice, context2);
//		testCon.add(alice, mbox, mboxAlice, context2);
//		testCon.add(context2, publisher, nameAlice);
//	
//		testCon.add(bob, name, nameBob, context1);
//		testCon.add(bob, mbox, mboxBob, context1);
//		testCon.add(context1, publisher, nameBob);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" SELECT name, mbox");
////		queryBuilder.append(" FROM {p} foaf:name {name};");
////		queryBuilder.append("         foaf:mbox {mbox}");
////		queryBuilder.append(" WHERE p = VAR");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" SELECT ?name ?mbox");
//		queryBuilder.append(" WHERE { ?VAR foaf:name ?name .");
//		queryBuilder.append("         ?VAR foaf:mbox ?mbox . }");
//
//		
//		
//		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
//		query.setBinding("VAR", bob);
//	
//		TupleQueryResult result = query.evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				BindingSet solution = result.next();
//				assertTrue(solution.hasBinding("name"));
//				assertTrue(solution.hasBinding("mbox"));
//	
//				Value nameResult = solution.getValue("name");
//				Value mboxResult = solution.getValue("mbox");
//	
//				assertEquals("unexpected value for name: " + nameResult, nameBob, nameResult);
//				assertEquals("unexpected value for mbox: " + mboxResult, mboxBob, mboxResult);
//			}
//		}
//		finally {
//			result.close();
//		}
//	}
//	
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//	public void testPreparedTupleQueryUnicode()
//		throws Exception
//	{
///*		
// This is commented out until we fix the unicode problem.
//		 
//		testCon.add(alexander, name, <UNICODE VAR FROM SUPER>);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" SELECT person");
////		queryBuilder.append(" FROM {person} foaf:name {name}");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" SELECT ?person");
//		queryBuilder.append(" WHERE { ?person foaf:name ?name . }");
//
//		
//		
//		TupleQuery query = testCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
//		query.setBinding("name", <UNICODE VAR FROM SUPER>);
//	
//		TupleQueryResult result = query.evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				BindingSet solution = result.next();
//				assertTrue(solution.hasBinding("person"));
//				assertEquals(alexander, solution.getValue("person"));
//			}
//		}
//		finally {
//			result.close();
//		}
//*/
//	}
//	
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//	public void testSimpleGraphQuery()
//		throws Exception
//	{
//		testCon.add(alice, name, nameAlice, context2);
//		testCon.add(alice, mbox, mboxAlice, context2);
//		testCon.add(context2, publisher, nameAlice);
//	
//		testCon.add(bob, name, nameBob, context1);
//		testCon.add(bob, mbox, mboxBob, context1);
//		testCon.add(context1, publisher, nameBob);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" CONSTRUCT *");
////		queryBuilder.append(" FROM {} foaf:name {name};");
////		queryBuilder.append("         foaf:mbox {mbox}");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" CONSTRUCT { ?x foaf:name ?name .");
//		queryBuilder.append("             ?x foaf:mbox ?mbox . }");
//		queryBuilder.append(" WHERE { ?x foaf:name ?name .");
//		queryBuilder.append("         ?x foaf:mbox ?mbox . }");
//
//		
//		
//		GraphQueryResult result = testCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString()).evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				Statement st = result.next();
//				if (name.equals(st.getPredicate())) {
//					assertTrue(nameAlice.equals(st.getObject()) || nameBob.equals(st.getObject()));
//				}
//				else {
//					assertTrue(mbox.equals(st.getPredicate()));
//					assertTrue(mboxAlice.equals(st.getObject()) || mboxBob.equals(st.getObject()));
//				}
//			}
//		}
//		finally {
//			result.close();
//		}
//	}
//	
//    /**
//     * Modified to test SPARQL instead of Serql.
//     */
//	public void testPreparedGraphQuery()
//		throws Exception
//	{
//		testCon.add(alice, name, nameAlice, context2);
//		testCon.add(alice, mbox, mboxAlice, context2);
//		testCon.add(context2, publisher, nameAlice);
//	
//		testCon.add(bob, name, nameBob, context1);
//		testCon.add(bob, mbox, mboxBob, context1);
//		testCon.add(context1, publisher, nameBob);
//	
//		StringBuilder queryBuilder = new StringBuilder();
////		queryBuilder.append(" CONSTRUCT *");
////		queryBuilder.append(" FROM {} foaf:name {name};");
////		queryBuilder.append("         foaf:mbox {mbox}");
////		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
//		queryBuilder.append(" PREFIX foaf: <" + FOAF_NS + ">");
//		queryBuilder.append(" CONSTRUCT { ?x foaf:name ?name .");
//		queryBuilder.append("             ?x foaf:mbox ?mbox . }");
//		queryBuilder.append(" WHERE { ?x foaf:name ?name .");
//		queryBuilder.append("         ?x foaf:mbox ?mbox . }");
//
//		
//		
//		GraphQuery query = testCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString());
//		query.setBinding("name", nameBob);
//	
//		GraphQueryResult result = query.evaluate();
//	
//		try {
//			assertTrue(result != null);
//			assertTrue(result.hasNext());
//	
//			while (result.hasNext()) {
//				Statement st = result.next();
//				assertTrue(name.equals(st.getPredicate()) || mbox.equals(st.getPredicate()));
//				if (name.equals(st.getPredicate())) {
//					assertTrue("unexpected value for name: " + st.getObject(), nameBob.equals(st.getObject()));
//				}
//				else {
//					assertTrue(mbox.equals(st.getPredicate()));
//					assertTrue("unexpected value for mbox: " + st.getObject(), mboxBob.equals(st.getObject()));
//				}
//	
//			}
//		}
//		finally {
//			result.close();
//		}
//	}
//
    /**
     * {@inheritDoc}
     * <p>
     * This test was failing historically for two reasons. First, it would
     * sometimes encounter a full GC pause that would suspend the JVM for longer
     * than the query timeout. This would fail the test. Second, the query
     * engine code used to only check for a deadline when a query operator would
     * start or stop. This meant that a compute bound operator would not be
     * interrupted if there was no other concurrent operators for that query
     * that were starting and stoping. This was fixed in #722.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
     *      Query timeout only checked at operator start/stop. </a>
     */
    @Override
    public void testOrderByQueriesAreInterruptable() throws Exception {

        /*
         * Note: Test failures arise from length GC pauses. Such GC pauses
         * suspend the application for longer than the query should run and
         * cause it to miss its deadline. In order to verify that the deadline
         * is being applied correctly, we can only rely on those test trials
         * where the GC pause was LT the target query time. Other trials need to
         * be thrown out. We do this using a Sun specific management API. The
         * test will throw a ClassNotFoundException for other JVMs.
         */
        final Class cls1 = Class
                .forName("com.sun.management.GarbageCollectorMXBean");

        final Class cls2 = Class.forName("com.sun.management.GcInfo");

        final Method method1 = cls1.getMethod("getLastGcInfo", new Class[] {});

        final Method method2 = cls2.getMethod("getDuration", new Class[] {});

        /*
         * Load data.
         */
        testCon.setAutoCommit(false);
        for (int index = 0; index < 512; index++) {
            testCon.add(RDFS.CLASS, RDFS.COMMENT, testCon.getValueFactory()
                    .createBNode());
        }
        testCon.setAutoCommit(true);
        testCon.commit();

        final long MAX_QUERY_TIME = 2000;
        final long MAX_TIME_MILLIS = 5000;
        final int NTRIALS = 20;
        int nok = 0, ngcfail = 0;

        for (int i = 0; i < NTRIALS; i++) {
        
            if (log.isInfoEnabled())
                log.info("RUN-TEST-PASS #" + i);
            
            final TupleQuery query = testCon
                    .prepareTupleQuery(
                            QueryLanguage.SPARQL,
                            "SELECT * WHERE { ?s ?p ?o . ?s1 ?p1 ?o1 . ?s2 ?p2 ?o2 . ?s3 ?p3 ?o3 . } ORDER BY ?s1 ?p1 ?o1 LIMIT 1000");
            
            query.setMaxQueryTime((int) (MAX_QUERY_TIME / 1000));

            final long startTime = System.currentTimeMillis();
            
            final TupleQueryResult result = query.evaluate();
            
            if (log.isInfoEnabled())
                log.info("Query evaluation has begin");
            
            try {
            
                result.hasNext();
                fail("Query should have been interrupted on pass# " + i);
                
            } catch (QueryInterruptedException e) {
                
                // Expected
                final long duration = System.currentTimeMillis() - startTime;
                
                if (log.isInfoEnabled())
                    log.info("Actual query duration: " + duration
                            + "ms on pass#" + i);
                
                final boolean ok = duration < MAX_TIME_MILLIS;

                if (ok) {
                    
                    nok++;
                    
                } else {
                    
                    boolean failedByGCPause = false;

                    final List<GarbageCollectorMXBean> mbeans = ManagementFactory
                            .getGarbageCollectorMXBeans();

                    for (GarbageCollectorMXBean m : mbeans) {
                        /*
                         * Note: This relies on a sun specific interface.
                         * 
                         * Note: This test is not strickly diagnostic. We should
                         * really be comparing the full GC time since we started
                         * to evaluate the query. However, in practice this is a
                         * pretty good proxy for that (as observed with
                         * -verbose:gc when you run this test).
                         */
                        if (cls1.isAssignableFrom(m.getClass())) {
                            // Information from the last GC.
                            final Object lastGcInfo = method1.invoke(m,
                                    new Object[] {});
                            // Duration of that last GC.
                            final long lastDuration = (Long) method2.invoke(
                                    lastGcInfo, new Object[] {});
                            if (lastDuration >= MAX_QUERY_TIME) {
                                log.warn("Large GC pause caused artifical liveness problem: duration="
                                        + duration + "ms");
                                failedByGCPause = true;
                                break;
                            }
                        }
                    }
                    
                    if (!failedByGCPause)
                        fail("Query not interrupted quickly enough, should have been ~2s, but was "
                                + (duration / 1000) + "s on pass#" + i);
                    
                    ngcfail++;

                }
            }
        }

        /*
         * Fail the test if we do not get enough good trials.
         */
        final String msg = "NTRIALS=" + NTRIALS + ", nok=" + nok + ", ngcfail="
                + ngcfail;

        log.warn(msg);
        
        if (nok < 5) {

            fail(msg);

        }

    }

}
