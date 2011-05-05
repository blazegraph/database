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

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.sail.RDFStoreTest;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.LocalTripleStore;

public class BigdataStoreTest extends RDFStoreTest {

    private static final Logger log = Logger.getLogger(BigdataStoreTest.class);
 
    /**
     * Return a test suite using the {@link LocalTripleStore} and pipeline
     * joins.
     */
    public static class LTSWithPipelineJoins extends BigdataStoreTest {

        public LTSWithPipelineJoins(String name) {
            
            super(name);
            
        }
        
        @Override
        protected Properties getProperties() {
            
            final Properties p = new Properties(super.getProperties());
       
            return p;
            
        }

    }
    
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
    protected void tearDown()
        throws Exception
    {
        
        final IIndexManager backend = sail == null ? null
                : ((BigdataSail) sail).getDatabase().getIndexManager();

        super.tearDown();

        if (backend != null)
            backend.destroy();

    }
    
    public BigdataStoreTest(String name) {

        super(name);
        
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
     * FIXME This one is failing because of this code:
     * <code>
     * bindings.addBinding("Y", painter);
     * iter = con.evaluate(tupleExpr, null, bindings, false);
     * resultCount = verifyQueryResult(iter, 1);
     * </code>
     * Adding a binding for the "Y" variable causes that binding to appear in
     * the result set, even though "Y" is not one of the selected variables. This
     * is a bigdata bug and should be fixed.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/254
     */
    @Override
    public void testQueryBindings()
        throws Exception
    {
        log.warn("FIXME");
        super.testQueryBindings();
    }

	/**
	 * FIXME This one is failing because we cannot handle literals longer than
	 * 65535 characters. This is a known issue.
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/109
	 */
    @Override
    public void testReallyLongLiteralRoundTrip()
        throws Exception
    {
        log.warn("FIXME");
        super.testReallyLongLiteralRoundTrip();
    }

	/**
	 * Bigdata uses snapshot isolation for transactions while openrdf assumes
	 * that any writes committed by a transaction become immediately visible to
	 * transactions which are already running. This unit test from the base
	 * class has been overridden since bigdata has stronger semantics for
	 * transactional isolation.
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
			String query = "SELECT S, P, O FROM {S} P {O}";
			ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SERQL, query, null);
//			final int nexpected = 5; // No. This is openrdf "read-committed" semantics.
			final int nexpected = 1; // Yes. This is bigdata snapshot isolation semantics.
			assertEquals(nexpected, countElements(con2.evaluate(tupleQuery.getTupleExpr(), null,
					EmptyBindingSet.getInstance(), false)));
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

}
