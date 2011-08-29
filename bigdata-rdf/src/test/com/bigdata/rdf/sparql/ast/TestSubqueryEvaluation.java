/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Unit tests for subquery evaluation based on an AST input model.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSubqueryEvaluation extends AbstractASTEvaluationTestCase {

    public TestSubqueryEvaluation() {
    }

    public TestSubqueryEvaluation(final String name) {
        super(name);
    }

    /**
     * Unit test for an AST modeling a sub-select. A sub-select may be evaluated
     * at any point in the pipeline. Only variables which are projected by the
     * sub-select are in scope when it runs. Those variables are projected into
     * the subquery.
     * 
     * <pre>
     * SELECT ?s
     *  WHERE {
     *    ?s ?x ?o .
     *    {
     *      SELECT ?x WHERE {?x rdf:subPropertyOf ?x}
     *    }
     * }
     * </pre>
     * @throws Exception 
     */
    public void test_subSelect() throws Exception {

        final String sparql = ""//
                + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"//
                + "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"//
                + "select ?s ?o \n" //
                + "where {\n"//
                + " ?x rdfs:label ?o " + " {\n"//
                + "   select ?x where { ?x rdf:type foaf:Person }\n" //
                + " }\n"//
                + "}"//
        ;

        final AbstractTripleStore store = getStore(getProperties());

        try {
        
            final BigdataValueFactory f = store.getValueFactory();

            // Add some data.
            {

                final BigdataStatement[] stmts = new BigdataStatement[] {//

                        new BigdataStatementImpl(
                                f.createURI("http:/www.bigdata.com/Mike"),
                                f.createURI(RDF.TYPE.toString()),
                                f.createURI(FOAFVocabularyDecl.Person.toString()),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http:/www.bigdata.com/Bryan"),
                                f.createURI(RDF.TYPE.toString()),
                                f.createURI(FOAFVocabularyDecl.Person.toString()),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http:/www.bigdata.com/Mike"),
                                f.createURI(RDFS.LABEL.toString()),
                                f.createLiteral("Mike"),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http:/www.bigdata.com/Bryan"),
                                f.createURI(RDFS.LABEL.toString()),
                                f.createLiteral("Bryan"),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                };

                final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                        store, 10/* capacity */);

                for(BigdataStatement stmt : stmts) {
                
                    buf.add(stmt);
                    
                }
                
                // write on the database.
                buf.flush();
                
            }

            final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

            final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

            final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                    .toString());
            
            final BigdataValue[] values = new BigdataValue[] { rdfType,
                    rdfsLabel, foafPerson };

            // resolve IVs.
            store.getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);
            
            /*
             * Build up the AST for the query.
             */
            final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
            final SubqueryRoot subSelect;
            {

                final VarNode s = new VarNode("s");

                final VarNode o = new VarNode("o");

                final VarNode x = new VarNode("x");

                final ConstantNode rdfsLabelConst = new ConstantNode(
                        rdfsLabel.getIV());

                final ConstantNode rdfTypeConst = new ConstantNode(
                        rdfType.getIV());

                final ConstantNode foafPersonConst = new ConstantNode(
                        foafPerson.getIV());

                queryRoot.setQueryHints(new Properties());
                
                {
                    final ProjectionNode projection = new ProjectionNode();
                    projection.addProjectionVar(s);
                    projection.addProjectionVar(o);
                    queryRoot.setProjection(projection);

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    queryRoot.setWhereClause(whereClause);

                    whereClause.addChild(new StatementPatternNode(x, rdfsLabelConst,
                            o, null/* c */,
                            StatementPattern.Scope.DEFAULT_CONTEXTS));

                    /*
                     * TODO Test with the sub-select in its own join group and
                     * with it in the top-level group. I assume that the
                     * sub-select in its own join group will translate into a
                     * sub-sub-query unless that otherwise empty join group is
                     * detected by an optimizer.
                     */
                    subSelect = new SubqueryRoot(QueryType.SELECT);
                    // whereClause.addChild(subSelect);

                    final JoinGroupNode wrapperGroup = new JoinGroupNode();
                    whereClause.addChild(wrapperGroup);
                    wrapperGroup.addChild(subSelect);
                }
                {

                    final ProjectionNode projection2 = new ProjectionNode();
                    projection2.addProjectionVar(x);
                    subSelect.setProjection(projection2);

                    final JoinGroupNode whereClause2 = new JoinGroupNode();
                    subSelect.setWhereClause(whereClause2);

                    whereClause2.addChild(new StatementPatternNode(x, rdfTypeConst,
                            foafPersonConst, null/* c */, Scope.DEFAULT_CONTEXTS));

                }
            
            }

            log.error("AST: " + queryRoot);

            final Properties queryHints = queryRoot.getQueryHints();
            final AtomicInteger idFactory = new AtomicInteger(0);
            final QueryEngine queryEngine = QueryEngineFactory
                    .getQueryController(store.getIndexManager());

            final AST2BOpContext ctx = new AST2BOpContext(queryRoot, idFactory,
                    store, queryEngine, queryHints);

            // Generate the query plan.
            final PipelineOp queryPlan = AST2BOpUtility.convert(ctx);

            log.error("plan:\n" + BOpUtility.toString(queryPlan));

            final IRunningQuery runningQuery = queryEngine.eval(queryPlan);
            
            final Iterator<IBindingSet[]> itr = runningQuery.iterator();
            
            while(itr.hasNext()) {
                
                final IBindingSet[] chunk = itr.next();
                
                for(IBindingSet bs : chunk) {
                    
                    log.error("bset: "+bs);
                    
                }
                
            }

            // Check Future for errors.
            runningQuery.get();
            
            /*
             * FIXME Add some triples which would not be selected and verify
             * that the expected solutions were reported (assert same solutions
             * any order).
             */

            fail("write test");

        } finally {

            store.__tearDownUnitTest();
            
        }

    }

    /**
     * Unit test for an AST modeling an EXISTS filter. The EXISTS filter is
     * modeled as an ASK sub-query which projects an anonymous variable and a
     * simple test of the truth state of that anonymous variable.
     */
    public void test_existsSubquery() {
        fail("write test");
    }

    /**
     * Unit test for an AST modeling a named subquery. A named subquery is
     * introduced by a {@link NamedSubqueryRoot}, which is attached to the
     * {@link QueryRoot}. The named subquery always run before the WHERE clause
     * of the query. When it runs, the named subquery produces a named temporary
     * solution set. The temporary solution set is scoped by the name on the
     * {@link IQueryAttributes} of the {@link IRunningQuery}. A
     * {@link NamedSubqueryInclude} is used to join against the temporary
     * solution set elsewhere in the query.
     * 
     * TODO Filter to remove any named subquery whose result set is not used?
     */
    public void test_namedSubquery() {
        fail("write test");
    }

}
