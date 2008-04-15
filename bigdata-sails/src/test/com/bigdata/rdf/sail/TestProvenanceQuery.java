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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.StatementIterator;
import com.bigdata.rdf.store.StatementWithType;

/**
 * Test suite for high-level query against a graph containing statements about
 * statements.
 * 
 * FIXME test result bindings
 * 
 * FIXME test construct
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestProvenanceQuery extends AbstractBigdataSailTestCase {

    public void test_query() throws SailException, IOException, RDFHandlerException, QueryEvaluationException {

        if (!((BigdataSail) sail).database.getStatementIdentifiers()) {

            log.warn("Statement identifiers are not enabled");

            return;

        }

        /*
         * Load data into the sail.
         */
        {
 
            DataLoader dataLoader = sail.database.getDataLoader();

            dataLoader.loadData("src/test/com/bigdata/rdf/sail/provenance01.rdf", ""/*baseURL*/, RDFFormat.RDFXML);
            
        }
        
        /*
         * Serialize as RDF/XML using a vendor specific extension to represent
         * the statement identifiers and statements about statements.
         * 
         * Note: This is just for debugging.
         */
        {
         
            final StatementIterator itr = sail.database.getStatements(null, null, null);
            final String rdfXml;
            try {

                Writer w = new StringWriter();

                RDFXMLWriter rdfWriter = new RDFXMLWriter(w);

                rdfWriter.startRDF();

                while (itr.hasNext()) {

                    StatementWithType stmt = (StatementWithType)itr.next();

                    // only write the explicit statements.
                    if(!stmt.isExplicit()) continue;
                    
                    rdfWriter.handleStatement(stmt);

                }

                rdfWriter.endRDF();

                rdfXml = w.toString();

            } finally {

                try {
                    itr.close();
                } catch (SailException e) {
                    throw new RuntimeException(e);
                }

            }

            // write the rdf/xml on the console.
            System.err.println(rdfXml);

        }
        
        SailConnection conn = sail.getConnection();

        try {

            final URI y = new URIImpl("http://www.foo.org/y");
            
            final URI B = new URIImpl("http://www.foo.org/B");

            final URI dcCreator = new URIImpl("http://purl.org/dc/terms/creator");

            final Literal bryan = new LiteralImpl("bryan");
            
            final Literal mike = new LiteralImpl("mike");

            /*
             * This is the hand-coded query.
             * 
             * FIXME Try changing the evaluation order in the hand-coded join
             * and verify that the join is evaluated correctly. When statement
             * identifiers are enabled, the only way to bind the context
             * position is to already have a statement on hand - there is no
             * index which can be used to look up a statement by its context and
             * the context is always a blank node.
             */

            TupleExpr tupleExpr = new Join(//
                    new StatementPattern(//
                            new Var("X", y),//
                            new Var("1", RDF.TYPE),//
                            new Var("2", B),//
                            new Var("SID")),// unbound.
                    new StatementPattern(//
                            new Var("SID"),//
                            new Var("3", dcCreator),//
                            new Var("Y")));

            /*
             * Create a data set consisting of the contexts to be queried.
             * 
             * @todo We a fake context to the data set so that Sesame will run
             * the query. However this needs to be changed once the database is
             * actually a quad store.
             */
            DatasetImpl dataSet = new DatasetImpl();

            // a fake context.
            dataSet.addDefaultGraph(new URIImpl("http://www.bigdata.com"));

            BindingSet bindingSet = new QueryBindingSet();

            CloseableIteration<? extends BindingSet, QueryEvaluationException> itr = conn
                    .evaluate(tupleExpr, dataSet, bindingSet, true/* includeInferred */);

            log.info("Verifying query.");

            /*
             * These are the expected results for the query (the bindings for Y).
             */

            final Set<Value> expected = new HashSet<Value>();

            expected.add(bryan);
            
            expected.add(mike);

            /*
             * Verify that the query results in the correct solutions.
             */

            final int nresults = expected.size();
            
            try {

                int i = 0;

                while (itr.hasNext()) {

                    BindingSet solution = itr.next();

                    System.out.println("solution[" + i + "] : " + solution);

                    Value actual = solution.getValue("Y");

                    assertTrue("Not expecting Y=" + actual, expected
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

}
