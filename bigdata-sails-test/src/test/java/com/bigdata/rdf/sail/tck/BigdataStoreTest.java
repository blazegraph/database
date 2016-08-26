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

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * @openrdf test suite extenstion/override.
 */
public class BigdataStoreTest extends RDFStoreTest {

    private static final Logger log = Logger.getLogger(BigdataStoreTest.class);
 
//    /**
//     * Return a test suite using the {@link LocalTripleStore} and pipeline
//     * joins.
//     */
//    public static class LTSWithPipelineJoins extends BigdataStoreTest {
//
////        public LTSWithPipelineJoins(String name) {
////            
////            super(name);
////            
////        }
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
    
    static File createTempFile() {
        
        try {
            return File.createTempFile("bigdata-tck", ".jnl");
            
        } catch (IOException e) {
            
            throw new AssertionError(e);
            
        }
        
    }

    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    public void tearDown()
        throws Exception
    {
        
        final IIndexManager backend = sail == null ? null
                : ((BigdataSail) sail).getIndexManager();

        super.tearDown();

        if (backend != null)
            backend.destroy();

    }
    
    public BigdataStoreTest() {
    }
    
    protected Properties getProperties() {
        
        final Properties props = new Properties();
        
        final File journal = createTempFile();
        
        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

		/*
		 * WORM supports full tx right now. RW has tx issue which we plan to
		 * resolve shortly (5/5/2011).
		 */
		props.setProperty(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskWORM
				.toString());

		// use told bnode mode
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
        
        // can't handle normalization
        props.setProperty(Options.INLINE_DATE_TIMES, "false");
        
        return props;
        
    }
    
    @Override
    protected Sail createSail() throws SailException {
        
        final Sail sail = new BigdataSail(getProperties());
        
        sail.initialize();
        
        final SailConnection conn = sail.getConnection();
        
        try {
            
            conn.clear();
            
            conn.clearNamespaces();
            
            conn.commit();
            
        } finally {
            
            conn.close();
            
        }

        return sail;
        
    }
    
	/**
	 * Bigdata uses snapshot isolation for transactions while openrdf assumes
	 * that any writes committed by a transaction become immediately visible to
	 * transactions which are already running. This unit test from the base
	 * class has been overridden since bigdata has stronger semantics for
	 * transactional isolation.
	 * 
	 * Also modified to use SPARQL instead of SeRQL.
	 */
    @Override
    public void testDualConnections()
        throws Exception
    {
        log.warn("Test overridden since bigdata uses full snapshot tx isolation.");
//    	super.testDualConnections();
		SailConnection con2 = sail.getConnection();
		try {
			assertEquals(0, countAllElements());
			con.addStatement(painter, RDF.TYPE, RDFS.CLASS);
			con.addStatement(painting, RDF.TYPE, RDFS.CLASS);
			con.addStatement(picasso, RDF.TYPE, painter, context1);
			con.addStatement(guernica, RDF.TYPE, painting, context1);
			con.commit();
			assertEquals(4, countAllElements());
			con2.addStatement(RDF.NIL, RDF.TYPE, RDF.LIST);
			String query = "SELECT ?S ?P ?O WHERE { ?S ?P ?O }";
//			ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SERQL, query, null);

	        final CloseableIteration<? extends BindingSet, QueryEvaluationException> queryResult =
	        	evaluate(query, con2);
	        
//			final int nexpected = 5; // No. This is openrdf "read-committed" semantics.
			final int nexpected = 1; // Yes. This is bigdata snapshot isolation semantics.
			assertEquals(nexpected, countElements(queryResult));
			Runnable clearer = new Runnable() {

				public void run() {
					try {
						con.clear();
						con.commit();
					}
					catch (SailException e) {
						throw new RuntimeException(e);
					}
				}
			};
			Thread thread = new Thread(clearer);
			thread.start();
			Thread.yield();
			Thread.yield();
			con2.commit();
			thread.join();
		}
		finally {
			con2.close();
		}
    }
    
    private CloseableIteration<? extends BindingSet, QueryEvaluationException> 
    		evaluate(final String query, final SailConnection con) 
    			throws Exception {
    	
		return evaluate(query, con, EmptyBindingSet.getInstance());
		
    }
    
    private CloseableIteration<? extends BindingSet, QueryEvaluationException> 
			evaluate(final String query, final SailConnection con, final BindingSet bs) 
				throws Exception {
		
		// new pattern
		((BigdataSailConnection) con).flush();
		final AbstractTripleStore db = ((BigdataSailConnection) con).getTripleStore();
		final ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);
		final QueryRoot originalQuery = astContainer.getOriginalAST();
		originalQuery.setIncludeInferred(false);
		final TupleQueryResult queryResult = ASTEvalHelper.evaluateTupleQuery(
		        db, astContainer, new QueryBindingSet(
		        		bs), null /* dataset */);
		
		return queryResult;
		
	}

	private int countElements(Iteration<?, ?> iter) throws Exception {
		int count = 0;

		try {
			while (iter.hasNext()) {
				iter.next();
				count++;
			}
		} finally {
			Iterations.closeCloseable(iter);
		}

		return count;
	}

	@Override
    protected void testValueRoundTrip(Resource subj, URI pred, Value obj)
            throws Exception {
        con.begin();
        con.addStatement(subj, pred, obj);
        con.commit();

        CloseableIteration<? extends Statement, SailException> stIter = con
                .getStatements(null, null, null, false);

        try {
            assertTrue(stIter.hasNext());

            Statement st = stIter.next();
            assertEquals(subj, st.getSubject());
            assertEquals(pred, st.getPredicate());
            assertEquals(obj, st.getObject());
            assertTrue(!stIter.hasNext());
        } finally {
            stIter.close();
        }

        final String query = "SELECT ?S ?P ?O WHERE { ?S ?P ?O filter(?P = <"
                + pred.stringValue() + ">) }";

        CloseableIteration<? extends BindingSet, QueryEvaluationException> iter;
        iter = evaluate(query, con);

        try {
            assertTrue(iter.hasNext());

            BindingSet bindings = iter.next();
            assertEquals(subj, bindings.getValue("S"));
            assertEquals(pred, bindings.getValue("P"));
            assertEquals(obj, bindings.getValue("O"));
            assertTrue(!iter.hasNext());
        } finally {
            iter.close();
        }
    }

    /**
     * Modified to test SPARQL instead of Serql.
     */
	@Override
	public void testEmptyRepository()
		throws Exception
	{
		// repository should be empty
		assertEquals("Empty repository should not return any statements", 0, countAllElements());
	
		assertEquals("Named context should be empty", 0, countContext1Elements());
	
		assertEquals("Empty repository should not return any context identifiers", 0,
				countElements(con.getContextIDs()));
	
		assertEquals("Empty repository should not return any query results", 0,
				countQueryResults("select * where { ?S ?P ?O }"));
	}

    /**
     * Modified to test SPARQL instead of Serql.
     */
   @Override
	public void testAddData()
		throws Exception
	{
		// Add some data to the repository
		con.addStatement(painter, RDF.TYPE, RDFS.CLASS);
		con.addStatement(painting, RDF.TYPE, RDFS.CLASS);
		con.addStatement(picasso, RDF.TYPE, painter, context1);
		con.addStatement(guernica, RDF.TYPE, painting, context1);
		con.addStatement(picasso, paints, guernica, context1);
		con.commit();
	
		assertEquals("Repository should contain 5 statements in total", 5, countAllElements());
	
		assertEquals("Named context should contain 3 statements", 3, countContext1Elements());
	
		assertEquals("Repository should have 1 context identifier", 1, countElements(con.getContextIDs()));
	
		assertEquals("Repository should contain 5 statements in total", 5,
				countQueryResults("select * where { ?S ?P ?O }"));
	
		// Check for presence of the added statements
		assertEquals("Statement (Painter, type, Class) should be in the repository", 1,
				countQueryResults("select * where { ex:Painter rdf:type rdfs:Class }"));
	
		assertEquals("Statement (picasso, type, Painter) should be in the repository", 1,
				countQueryResults("select * where { ex:picasso rdf:type ex:Painter}"));
	
		// Check for absense of non-added statements
		assertEquals("Statement (Painter, paints, Painting) should not be in the repository", 0,
				countQueryResults("select * where {ex:Painter ex:paints ex:Painting}"));
	
		assertEquals("Statement (picasso, creates, guernica) should not be in the repository", 0,
				countQueryResults("select * where {ex:picasso ex:creates ex:guernica}"));
	
		// Various other checks
		assertEquals("Repository should contain 2 statements matching (picasso, _, _)", 2,
				countQueryResults("select * where {ex:picasso ?P ?O}"));
	
		assertEquals("Repository should contain 1 statement matching (picasso, paints, _)", 1,
				countQueryResults("select * where {ex:picasso ex:paints ?O}"));
	
		assertEquals("Repository should contain 4 statements matching (_, type, _)", 4,
				countQueryResults("select * where {?S rdf:type ?O}"));
	
		assertEquals("Repository should contain 2 statements matching (_, _, Class)", 2,
				countQueryResults("select * where {?S ?P rdfs:Class}"));
	
		assertEquals("Repository should contain 0 statements matching (_, _, type)", 0,
				countQueryResults("select * where {?S ?P rdf:type}"));
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	@Override
	public void testAddWhileQuerying()
		throws Exception
	{
		// Add some data to the repository
		con.addStatement(painter, RDF.TYPE, RDFS.CLASS);
		con.addStatement(painting, RDF.TYPE, RDFS.CLASS);
		con.addStatement(picasso, RDF.TYPE, painter);
		con.addStatement(guernica, RDF.TYPE, painting);
		con.addStatement(picasso, paints, guernica);
		con.commit();
	
//		ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SERQL,
//				"SELECT C FROM {} rdf:type {C}", null);
	
		CloseableIteration<? extends BindingSet, QueryEvaluationException> iter;
//		iter = con.evaluate(tupleQuery.getTupleExpr(), null, EmptyBindingSet.getInstance(), false);
		
		iter = evaluate("SELECT ?C where { ?s <"+RDF.TYPE+">  ?C}", con);
	
		while (iter.hasNext()) {
			BindingSet bindings = iter.next();
			Value c = bindings.getValue("C");
			if (c instanceof Resource) {
				con.addStatement((Resource)c, RDF.TYPE, RDFS.CLASS);
			}
		}
	
		con.commit();
	
		// Simulate auto-commit
	
		assertEquals(3, countElements(con.getStatements(null, RDF.TYPE, RDFS.CLASS, false)));
	
//		tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SERQL, "SELECT P FROM {} P {}", null);
//		iter = con.evaluate(tupleQuery.getTupleExpr(), null, EmptyBindingSet.getInstance(), false);
		
		iter = evaluate("SELECT ?P where {?s ?P ?o}", con);
	
		while (iter.hasNext()) {
			BindingSet bindings = iter.next();
			Value p = bindings.getValue("P");
			if (p instanceof URI) {
				con.addStatement((URI)p, RDF.TYPE, RDF.PROPERTY);
				con.commit();
			}
		}
	
		assertEquals(2, countElements(con.getStatements(null, RDF.TYPE, RDF.PROPERTY, false)));
	}
	
    /**
     * Modified to test SPARQL instead of Serql.
     */
	@Override
	public void testRemoveAndClear()
		throws Exception
	{
		// Add some data to the repository
		con.addStatement(painter, RDF.TYPE, RDFS.CLASS);
		con.addStatement(painting, RDF.TYPE, RDFS.CLASS);
		con.addStatement(picasso, RDF.TYPE, painter, context1);
		con.addStatement(guernica, RDF.TYPE, painting, context1);
		con.addStatement(picasso, paints, guernica, context1);
		con.commit();
	
		// Test removal of statements
		con.removeStatements(painting, RDF.TYPE, RDFS.CLASS);
		con.commit();
	
		assertEquals("Repository should contain 4 statements in total", 4, countAllElements());
	
		assertEquals("Named context should contain 3 statements", 3, countContext1Elements());
	
		assertEquals("Statement (Painting, type, Class) should no longer be in the repository", 0,
				countQueryResults("select * where {ex:Painting rdf:type rdfs:Class}"));
	
		con.removeStatements(null, null, null, context1);
		con.commit();
	
		assertEquals("Repository should contain 1 statement in total", 1, countAllElements());
	
		assertEquals("Named context should be empty", 0, countContext1Elements());
	
		con.clear();
		con.commit();
	
		assertEquals("Repository should no longer contain any statements", 0, countAllElements());
	}

    /**
     * Modified to test SPARQL instead of Serql.
     */
	@Override
	public void testQueryBindings()
		throws Exception
	{
		// Add some data to the repository
		con.addStatement(painter, RDF.TYPE, RDFS.CLASS);
		con.addStatement(painting, RDF.TYPE, RDFS.CLASS);
		con.addStatement(picasso, RDF.TYPE, painter, context1);
		con.addStatement(guernica, RDF.TYPE, painting, context1);
		con.addStatement(picasso, paints, guernica, context1);
		con.commit();
	
		// Query 1
		MapBindingSet bindings = new MapBindingSet(2);
		CloseableIteration<? extends BindingSet, QueryEvaluationException> iter;
	
		iter = evaluate("select ?X where { ?X <"+RDF.TYPE+"> ?Y . ?Y <"+RDF.TYPE+"> <"+RDFS.CLASS+">}",con, bindings);
		
		int resultCount = verifyQueryResult(iter, 1);
		assertEquals("Wrong number of query results", 2, resultCount);
	
		bindings.addBinding("Y", painter);
		iter = evaluate("select ?X where { ?X <"+RDF.TYPE+"> ?Y . ?Y <"+RDF.TYPE+"> <"+RDFS.CLASS+">}",con, bindings);
		resultCount = verifyQueryResult(iter, 1);
		assertEquals("Wrong number of query results", 1, resultCount);
	
		bindings.addBinding("Z", painting);
		iter = evaluate("select ?X where { ?X <"+RDF.TYPE+"> ?Y . ?Y <"+RDF.TYPE+"> <"+RDFS.CLASS+">}",con, bindings);
		resultCount = verifyQueryResult(iter, 1);
		assertEquals("Wrong number of query results", 1, resultCount);
	
		bindings.removeBinding("Y");
		iter = evaluate("select ?X where { ?X <"+RDF.TYPE+"> ?Y . ?Y <"+RDF.TYPE+"> <"+RDFS.CLASS+">}",con, bindings);
		resultCount = verifyQueryResult(iter, 1);
		assertEquals("Wrong number of query results", 2, resultCount);
	
		// Query 2
		bindings.clear();
	
		iter = evaluate("select ?X where { ?X <"+RDF.TYPE+"> ?Y . ?Y <"+RDF.TYPE+"> <"+RDFS.CLASS+"> . filter( ?Y = ?Z) }",con, bindings);
		resultCount = verifyQueryResult(iter, 1);
		assertEquals("Wrong number of query results", 0, resultCount);
	
		bindings.addBinding("Z", painter);
		iter = evaluate("select ?X where { ?X <"+RDF.TYPE+"> ?Y . ?Y <"+RDF.TYPE+"> <"+RDFS.CLASS+"> . filter( ?Y = ?Z) }",con, bindings);
		resultCount = verifyQueryResult(iter, 1);
		assertEquals("Wrong number of query results", 1, resultCount);
	}


	@Override
	protected int countQueryResults(String query)
		throws Exception
	{
		query = "PREFIX ex: <http://example.org/> PREFIX rdf: <"+RDF.NAMESPACE+"> PREFIX rdfs: <"+RDFS.NAMESPACE+"> " + query;
		
		return countElements(evaluate(query, con));
	}


}
