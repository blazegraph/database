/**
Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIteratorIteration;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Test suite for
 * {@link BigdataSailQuery#setBindingSets(info.aduna.iteration.CloseableIteration)}
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/267
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSetBindingSets extends ProxyBigdataSailTestCase {

    private static final Logger log = Logger.getLogger(TestSetBindingSets.class);

    @Override
    public Properties getProperties() {
        
        final Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestSetBindingSets() {
    }

    /**
     * @param arg0
     */
    public TestSetBindingSets(String arg0) {
        super(arg0);
    }

    public void testSetBindingSets() throws Exception {

        final BigdataSail sail = getSail();

        try {

            sail.initialize();

            final BigdataSailRepository repo = new BigdataSailRepository(sail);

            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();

            try {

                cxn.setAutoCommit(false);

                final BigdataValueFactory vf = cxn.getValueFactory();

                // First step, load data.
                final String data = "@prefix ns:<http://localhost/pets#>. "
                        + "@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>. "
                        + "    ns:snowball rdfs:label \"Snowball\"; "
                        + "                ns:weight \"10\". "
                        + "    ns:buffy rdfs:label \"Buffy\"; "
                        + "                ns:weight \"8\".";

                if (log.isInfoEnabled())
                    log.info("Loading data");

                cxn.add(new StringReader(data), "", RDFFormat.TURTLE,
                        new Resource[0]);

                cxn.commit();

                if (log.isInfoEnabled()) {
                    
                    // Dump the database.
                    
                    final RepositoryResult<Statement> stmts = cxn
                            .getStatements(null, null, null, false);

                    while (stmts.hasNext()) {

                        final Statement tmp = stmts.next();
                        
                        log.info(tmp);
                        
                    }
                    
                }

                // Resolve some Values that we will need below.
                final BigdataLiteral buffy = vf.createLiteral("Buffy");
                final BigdataLiteral snowball = vf.createLiteral("Snowball");
//                final BigdataLiteral blizzard = vf.createLiteral("Blizzard");
                final BigdataLiteral w1 = vf.createLiteral("8");
                final BigdataLiteral w2 = vf.createLiteral("10");
//                final BigdataLiteral w3 = vf.createLiteral("0");
                sail.getDatabase().addTerms(new BigdataValue[]{
                        buffy,
                        snowball,
//                        blizzard,
                        w1,
                        w2,
//                        w3
                });
                if(log.isInfoEnabled())
                    log.info(sail.getDatabase().dumpStore());

                // Second step, query data. Load query from resource and
                // execute.
                final String query = "PREFIX ns:<http://localhost/pets#> "
                        + "\nPREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"
                        + "\nSELECT ?name ?weight WHERE {"
                        + "\n?uri rdfs:label ?name."
                        + "\n?uri ns:weight ?weight." + "\n}";


                // Verify result w/o binding.
                {

                    final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                            QueryLanguage.SPARQL, query);

                    final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                    answer.add(createBindingSet(new BindingImpl("name", buffy),
                            new BindingImpl("weight", w1)));
                    
                    answer.add(createBindingSet(new BindingImpl("name",
                            snowball), new BindingImpl("weight", w2)));

                    if (log.isInfoEnabled())
                        log.info("Executing query: " + query);

                    final TupleQueryResult result = tupleQuery.evaluate();

                    compare(result, answer);
                }

                /*
                 * Verify result w/ caller suppled initial binding sets.
                 */
                {

                    final Collection<BindingSet> answer = new LinkedList<BindingSet>();

                    answer.add(createBindingSet(new BindingImpl("name",
                            buffy), new BindingImpl("weight", w1)));

                    // Setup the additional inputs.
                    final Collection<BindingSet> bindingSets = new LinkedList<BindingSet>();
                    {

                        // This input binding set is consistent with the data.
                        bindingSets.add(createBindingSet(//
                                new BindingImpl("name", buffy), //
                                new BindingImpl("weight", w1)//
                                ));

                        // This input binding set is NOT consistent with the
                        // data.
                        bindingSets.add(createBindingSet(//
                                new BindingImpl("name", buffy), //
                                new BindingImpl("weight", w2)//
                                ));

                    }
                    
                    final CloseableIteration<BindingSet, QueryEvaluationException> bindingSetsItr = new CloseableIteratorIteration<BindingSet, QueryEvaluationException>(
                            bindingSets.iterator());

                    final Dataset dataset = null;
                    
                    final BindingSet bindingSet = new QueryBindingSet();

                    if (log.isInfoEnabled())
                        log.info("Executing query: " + query);

                    final TupleQuery tupleQuery = cxn.prepareTupleQuery(
                            QueryLanguage.SPARQL, query);

                    final TupleExpr tupleExpr = ((BigdataSailTupleQuery) tupleQuery)
                            .getTupleExpr();

                    final CloseableIteration<? extends BindingSet, QueryEvaluationException> bindingsIter = cxn
                            .getSailConnection().evaluate(tupleExpr, dataset,
                                    bindingSet, bindingSetsItr,
                                    false/* includeInferred */, null/* queryHints */
                            );

                    final TupleQueryResult result = new TupleQueryResultImpl(
                            new ArrayList<String>(tupleExpr.getBindingNames()),
                            bindingsIter);

                    compare(result, answer);

                }

            } finally {
                cxn.close();
            }

        } finally {
            sail.__tearDownUnitTest();
        }

    }

}
