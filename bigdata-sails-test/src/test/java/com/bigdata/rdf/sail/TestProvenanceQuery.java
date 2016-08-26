/**
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
 * Created on Apr 15, 2008
 */

package com.bigdata.rdf.sail;

import java.io.StringWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.sail.SailConnection;

import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.DataLoader;

/**
 * Test suite for high-level query against a graph containing statements about
 * statements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestProvenanceQuery extends ProxyBigdataSailTestCase {

	private final transient static Logger log = Logger.getLogger(TestProvenanceQuery.class);
	
    public TestProvenanceQuery() {
        
    }
    
    public TestProvenanceQuery(String name) {
        
        super(name);
        
    }

    public void test_query() throws Exception {

        final BigdataSail sail = getSail();
        
        try {
        
            sail.initialize();
            
            /*
             * Load data into the sail.
             */
            {

                boolean ok = false;

                final BigdataSailConnection conn = sail.getUnisolatedConnection();
                
                try {

                    if (!conn.getTripleStore().getStatementIdentifiers()) {

                        log.warn("Statement identifiers are not enabled");

                        return;

                    }

                    final DataLoader dataLoader = conn.getTripleStore().getDataLoader();

                    dataLoader.loadData("/com/bigdata/rdf/sail/provenance01.ttlx", ""/* baseURL */,
                            ServiceProviderHook.TURTLE_RDR);

                    conn.commit();

                    ok = true;

                } finally {

                    if (!ok)
                        conn.rollback();

                    conn.close();

                }
            }
    
            /*
             * Serialize as RDF/XML.
             * 
             * Note: This is just for debugging.
             */
    		if (log.isInfoEnabled()) {

                    final BigdataSailConnection conn = sail.getReadOnlyConnection();
                    
                    try {

                        final BigdataStatementIterator itr = conn.getTripleStore().getStatements(null, null, null);
                        final String rdfXml;
                        try {
            
                            final Writer w = new StringWriter();
            
            //                final RDFXMLWriter rdfWriter = new RDFXMLWriter(w);
                            final RDFWriterFactory writerFactory = RDFWriterRegistry
                                    .getInstance().get(RDFFormat.RDFXML);
                            
                            assertNotNull(writerFactory);
                            
                            final RDFWriter rdfWriter = writerFactory.getWriter(w);
            
                            rdfWriter.startRDF();
            
                            while (itr.hasNext()) {
            
                                final BigdataStatementImpl stmt = (BigdataStatementImpl) itr
                                        .next();
            
                                // only write the explicit statements.
                                if (!stmt.isExplicit())
                                    continue;
            
                                rdfWriter.handleStatement(stmt);
            
                            }
            
                            rdfWriter.endRDF();
            
                            rdfXml = w.toString();
            
                        } finally {
            
                            itr.close();
            
                        }
            
                        // write the rdf/xml
                        log.info(rdfXml);
    
                    } finally {

                        conn.close();
                        
                    }
                
            }
            
            final SailConnection conn = sail.getConnection();
    
            try {
    
                final URI y = new URIImpl("http://www.foo.org/y");
                
                final URI B = new URIImpl("http://www.foo.org/B");
    
                final URI dcCreator = new URIImpl("http://purl.org/dc/terms/creator");
    
                final Literal bryan = new LiteralImpl("bryan", XMLSchema.STRING);
                
                final Literal mike = new LiteralImpl("mike", XMLSchema.STRING);
    
                /*
                 * This is a hand-coded query.
                 * 
                 * Note: When statement identifiers are enabled, the only way to
                 * bind the context position is to already have a statement on hand -
                 * there is no index which can be used to look up a statement by its
                 * context and the context is always a blank node.
                 */
    
    //            final TupleExpr tupleExpr = 
    //                new Projection(
    //                new Join(//
    //                    new StatementPattern(//
    //                            new Var("X", y),//
    //                            new Var("1", RDF.TYPE),//
    //                            new Var("2", B),//
    //                            new Var("SID")),// unbound.
    //                    new StatementPattern(//
    //                            new Var("SID"),//
    //                            new Var("3", dcCreator),//
    //                            new Var("Y"))),
    //                new ProjectionElemList(new ProjectionElem[] { new ProjectionElem( "Y" )}));
    
    //            final String q = "select ?Y where { ?SID <"+dcCreator+"> ?Y . graph ?SID { <"+y+"> <"+RDF.TYPE+"> <"+B+"> . } }";
                final String q = "select ?Y where { <<<"+y+"> <"+RDF.TYPE+"> <"+B+">>> <"+dcCreator+"> ?Y . }";
                
                /*
                 * Create a data set consisting of the contexts to be queried.
                 * 
                 * Note: a [null] DataSet will cause context to be ignored when the
                 * query is processed.
                 */
    //            final DatasetImpl dataSet = null; //new DatasetImpl();
    //
    //            final BindingSet bindingSet = new QueryBindingSet();
    //
    //            final CloseableIteration<? extends BindingSet, QueryEvaluationException> itr = conn
    //                    .evaluate(tupleExpr, dataSet, bindingSet, true/* includeInferred */);
    
                final TupleQuery tq = new BigdataSailRepository(sail).getReadOnlyConnection().prepareTupleQuery(QueryLanguage.SPARQL, q);
                
                final TupleQueryResult itr = tq.evaluate();
                
                if (log.isInfoEnabled())
                    log.info("Verifying query.");
    
                /*
                 * These are the expected results for the query (the bindings for Y).
                 */
    
                final Set<Value> expected = new HashSet<Value>();
    
                expected.add(bryan);
                
                expected.add(mike);
    
                /*
                 * Verify that the query results is the correct solutions.
                 */
    
                final int nresults = expected.size();
                
                try {
    
                    int i = 0;
    
                    while (itr.hasNext()) {
    
                        final BindingSet solution = itr.next();
    
                        if (log.isInfoEnabled())
                            log.info("solution[" + i + "] : " + solution);
    
                        final Value actual = solution.getValue("Y");
    
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

        } finally {
            
            sail.__tearDownUnitTest();
            
        }
        
    }

}
