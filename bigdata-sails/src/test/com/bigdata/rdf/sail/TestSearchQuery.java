/*
 * Copyright SYSTAP, LLC 2006-2008.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Apr 15, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.BNS;

/**
 * Test suite for high-level query against a graph containing statements about
 * statements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSearchQuery extends AbstractBigdataSailTestCase {

    private void ___test_query() throws SailException, IOException, RDFHandlerException, QueryEvaluationException {

        if (!((BigdataSail) sail).database.getStatementIdentifiers()) {

            log.warn("Statement identifiers are not enabled");

            return;

        }

        /*
         * Load data into the sail.
         */
        {
 
            StatementBuffer sb = new StatementBuffer(sail.database,100/*capacity*/);
            
            sb.add(new URIImpl("http://www.bigdata.com/A"), RDFS.LABEL,
                    new LiteralImpl("Yellow Rose"));

            sb.add(new URIImpl("http://www.bigdata.com/B"), RDFS.LABEL,
                    new LiteralImpl("Red Rose"));

            sb.add(new URIImpl("http://www.bigdata.com/C"), RDFS.LABEL,
                    new LiteralImpl("Old Yellow House"));

            sb.flush();
            
        }
        
        SailConnection conn = sail.getConnection();

        try {

            /*
             * This is the hand-coded query.
             * 
             * FIXME write the query, can be very simple to just get values, e.g.,
             * the matching literals.  It can be made more complex to explore how
             * to make the joins efficient, but that is also a story for another
             * day.
             * 
select ?evidence
where
{ ?evidence rdf:type <the type> .
?evidence ?anypredicate ?label .
?label bigdata:search "the query" .
}             */

            TupleExpr tupleExpr = new StatementPattern(//
                    new Var("X"),//
                    new Var("1", new URIImpl(BNS.SEARCH)),//
                    new Var("2", new LiteralImpl("Yellow"))//
            );

            /*
             * Create a data set consisting of the contexts to be queried.
             * 
             * Note: a [null] DataSet will cause context to be ignored when the
             * query is processed.
             */
            DatasetImpl dataSet = null; //new DatasetImpl();

            BindingSet bindingSet = new QueryBindingSet();

            CloseableIteration<? extends BindingSet, QueryEvaluationException> itr = conn
                    .evaluate(tupleExpr, dataSet, bindingSet, true/* includeInferred */);

            log.info("Verifying query.");

            /*
             * These are the expected results for the query (the bindings for X).
             */

            final Set<Value> expected = new HashSet<Value>();

//            expected.add(new URIImpl("http://www.bigdata.com/A"));
//            
//            expected.add(new URIImpl("http://www.bigdata.com/C"));

            expected.add(new LiteralImpl("Yellow Rose"));
            
            expected.add(new LiteralImpl("Old Yellow House"));
            
            /*
             * Verify that the query results is the correct solutions.
             */

            final int nresults = expected.size();
            
            try {

                int i = 0;

                while (itr.hasNext()) {

                    BindingSet solution = itr.next();

                    System.out.println("solution[" + i + "] : " + solution);

                    Value actual = solution.getValue("X");

                    System.out.println("X[" + i + "] = " + actual +" ("+actual.getClass().getName()+")");

                    assertTrue("Not expecting X=" + actual, expected
                            .remove(actual));

                    i++;

                }

                assertEquals("#results", nresults, i);

            } finally {

                itr.close();

            }

        } finally {

            conn.close();

        }

    }

    public void test_restart() throws Exception {

        final boolean doYouWantMeToBreak = true;
        
        URI SYSTAP =
            new URIImpl(
                        "http://bigdata.com/elm#a479c37c-407e-4f4a-be30-5a643a54561f");
        URI ORGANIZATION = new URIImpl("http://bigdata.com/domain#Organization");
        URI ENTITY = new URIImpl("http://bigdata.com/system#Entity");
        Graph test_restart_1 = new GraphImpl();
        test_restart_1.add(new StatementImpl(ORGANIZATION, RDFS.SUBCLASSOF, ENTITY));
        Graph test_restart_2 = new GraphImpl();
        test_restart_2.add(new StatementImpl(SYSTAP, RDF.TYPE, ORGANIZATION));
        test_restart_2.add(new StatementImpl(SYSTAP, RDFS.LABEL, new LiteralImpl("SYSTAP")));
        
        String journal =
                System.getProperty("java.io.tmpdir") + "test-restart.jnl";
        new File(journal).delete();
        log.info(journal);
        
        Properties sailProps = new Properties();
        sailProps.setProperty(BigdataSail.Options.FILE, journal);
        BigdataSail sail = new BigdataSail(sailProps);
        BigdataSailRepository repo = new BigdataSailRepository(sail);
        repo.initialize();
        
        { // initialize
            final RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            try {
                boolean includeInferred = false;
                if (cxn.hasStatement(ORGANIZATION, RDFS.SUBCLASSOF, ENTITY, includeInferred) == false) {
                    log.info("creating graph new graph");
                    log.info("loading ontology");
                    cxn.add(test_restart_1);
                    if (!doYouWantMeToBreak) {
                        cxn.add(test_restart_2);
                    }
                    cxn.commit();
                } else {
                    log.info("loading existing graph");
                }
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
        }
        
        if (doYouWantMeToBreak) {
            final RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            try {
                boolean includeInferred = false;
                if (cxn.hasStatement(SYSTAP, RDF.TYPE, ORGANIZATION, includeInferred) == false) {
                    log.info("loading entity data");
                    cxn.add(test_restart_2);
                    cxn.commit();
                }
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                cxn.close();
            }
        }
        
        { // run the query
            final String query = 
                "construct { ?s <"+RDF.TYPE+"> <"+ENTITY+"> . } " +
                "where     { ?s <"+RDF.TYPE+"> <"+ENTITY+"> . ?s ?p ?lit . ?lit <"+BNS.SEARCH+"> \"systap\" . }";
            final RepositoryConnection cxn = repo.getConnection();
            try {
                // silly construct queries, can't guarantee distinct results
                final Set<Statement> results = new LinkedHashSet<Statement>();
                final GraphQuery graphQuery = 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                // final boolean includeInferred = true;
                graphQuery
                        // .evaluate(new StatementCollector(results, includeInferred));
                        .evaluate(new StatementCollector(results));
                for(Statement stmt : results) {
                    log.info(stmt);
                }
                assertTrue(results.contains(new StatementImpl(SYSTAP, RDF.TYPE, ENTITY)));
            } finally {
                cxn.close();
            }
        }
        
        repo.shutDown();
        
        { // reopen and run the query again
            sail = new BigdataSail(sailProps);
            repo = new BigdataSailRepository(sail);
            repo.initialize();
            { // run the query
                final String query = 
                    "construct { ?s <"+RDF.TYPE+"> <"+ENTITY+"> . } " +
                    "where     { ?s <"+RDF.TYPE+"> <"+ENTITY+"> . ?s ?p ?lit . ?lit <"+BNS.SEARCH+"> \"systap\" . }";
                final RepositoryConnection cxn = repo.getConnection();
                try {
                    // silly construct queries, can't guarantee distinct results
                    final Set<Statement> results = new LinkedHashSet<Statement>();
                    final GraphQuery graphQuery = 
                        cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                    // final boolean includeInferred = true;
                    graphQuery
                            // .evaluate(new StatementCollector(results, includeInferred));
                            .evaluate(new StatementCollector(results));
                    for(Statement stmt : results) {
                        log.info(stmt);
                    }
                    assertTrue(results.contains(new StatementImpl(SYSTAP, RDF.TYPE, ENTITY)));
                } finally {
                    cxn.close();
                }
            }
        }        
        
    }
}
