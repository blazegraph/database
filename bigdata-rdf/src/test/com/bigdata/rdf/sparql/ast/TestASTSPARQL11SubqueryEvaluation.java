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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.model.BigdataLiteral;
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
public class TestASTSPARQL11SubqueryEvaluation extends AbstractASTEvaluationTestCase {

    public TestASTSPARQL11SubqueryEvaluation() {
    }

    public TestASTSPARQL11SubqueryEvaluation(final String name) {
        super(name);
    }

    /**
     * Unit test for an AST modeling a sub-select. A sub-select may be evaluated
     * at any point in the pipeline. Only variables which are projected by the
     * sub-select are in scope when it runs. Those variables are projected into
     * the subquery.
     * 
     * <pre>
     * SELECT ?x ?o
     *  WHERE {
     *    ?x rdfs:label ?o .
     *    {
     *      SELECT ?x WHERE {?x rdf:type foaf:Person}
     *    }
     * }
     * </pre>
     * 
     * Data:
     * 
     * <pre>
     * http://www.bigdata.com/Mike rdf:type foaf:Person
     * http://www.bigdata.com/Bryan rdf:type foaf:Person
     * http://www.bigdata.com/Mike rdfs:label "Mike"
     * http://www.bigdata.com/Bryan rdfs:label "Bryan"
     * http://www.bigdata.com/DC rdfs:label "DC"
     * </pre>
     * 
     * Subquery solutions:
     * 
     * <pre>
     * {x=http://www.bigdata.com/Mike}
     * {x=http://www.bigdata.com/Bryan}
     * </pre>
     * 
     * Solutions:
     * 
     * <pre>
     * {x=http://www.bigdata.com/Mike; o="Mike"}
     * {x=http://www.bigdata.com/Bryan; o="Bryan"}
     * </pre>
     * 
     * @throws Exception
     */
    public void test_subSelect() throws Exception {

        final String sparql = ""//
                + "PREFIX rdf: http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"//
                + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"//
                + "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"//
                + "select ?x ?o \n" //
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
                                f.createURI("http://www.bigdata.com/Mike"),
                                f.createURI(RDF.TYPE.toString()),
                                f.createURI(FOAFVocabularyDecl.Person.toString()),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http://www.bigdata.com/Bryan"),
                                f.createURI(RDF.TYPE.toString()),
                                f.createURI(FOAFVocabularyDecl.Person.toString()),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http://www.bigdata.com/Mike"),
                                f.createURI(RDFS.LABEL.toString()),
                                f.createLiteral("Mike"),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http://www.bigdata.com/Bryan"),
                                f.createURI(RDFS.LABEL.toString()),
                                f.createLiteral("Bryan"),
                                null, // context
                                StatementEnum.Explicit,//
                                false// userFlag
                        ),

                        new BigdataStatementImpl(
                                f.createURI("http://www.bigdata.com/DC"),
                                f.createURI(RDFS.LABEL.toString()),
                                f.createLiteral("DC"),
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

            final BigdataURI mikeURI = f.createURI("http://www.bigdata.com/Mike");

            final BigdataURI bryanURI = f.createURI("http://www.bigdata.com/Bryan");

            final BigdataLiteral mikeLabel = f.createLiteral("Mike");

            final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

            final BigdataValue[] values = new BigdataValue[] { rdfType,
                    rdfsLabel, foafPerson, mikeURI, bryanURI, mikeLabel,
                    bryanLabel };

            // resolve IVs.
            store.getLexiconRelation()
                    .addTerms(values, values.length, true/* readOnly */);
            
            /*
             * Build up the AST for the query.
             */
            final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
            {

                final VarNode o = new VarNode("o");

                final VarNode x = new VarNode("x");

                final ConstantNode rdfsLabelConst = new ConstantNode(
                        rdfsLabel.getIV());

                final ConstantNode rdfTypeConst = new ConstantNode(
                        rdfType.getIV());

                final ConstantNode foafPersonConst = new ConstantNode(
                        foafPerson.getIV());

                final SubqueryRoot subSelect;

                queryRoot.setQueryHints(new Properties());
                
                {

                    final ProjectionNode projection = new ProjectionNode();
                    projection.addProjectionVar(x);
                    projection.addProjectionVar(o);
                    queryRoot.setProjection(projection);

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    queryRoot.setWhereClause(whereClause);

                    whereClause.addChild(new StatementPatternNode(x, rdfsLabelConst,
                            o, null/* c */,
                            StatementPattern.Scope.DEFAULT_CONTEXTS));

                    subSelect = new SubqueryRoot(QueryType.SELECT);
                    whereClause.addChild(subSelect);

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

            if(log.isInfoEnabled())
                log.info("AST: " + queryRoot);

            final Properties queryHints = queryRoot.getQueryHints();

            final AtomicInteger idFactory = new AtomicInteger(0);
            
            final QueryEngine queryEngine = QueryEngineFactory
                    .getQueryController(store.getIndexManager());

            final AST2BOpContext ctx = new AST2BOpContext(queryRoot, store);

            // Generate the query plan.
            final PipelineOp queryPlan = AST2BOpUtility.convert(ctx);

            if(log.isInfoEnabled())
                log.info("plan:\n" + BOpUtility.toString(queryPlan));

            final IBindingSet[] expected = new IBindingSet[] {
                    new ListBindingSet(//
                            new IVariable[] { Var.var("x"), Var.var("o") },//
                            new IConstant[] { new Constant(mikeURI.getIV()),
                                    new Constant(mikeLabel.getIV()) }),//
                    new ListBindingSet(//
                            new IVariable[] { Var.var("x"), Var.var("o") },//
                            new IConstant[] { new Constant(bryanURI.getIV()),
                                    new Constant(bryanLabel.getIV()) }),//
           // { x=TermId(1U), o=TermId(3L) }
            // { x=TermId(2U), o=TermId(4L) }
            };
            assertSameSolutionsAnyOrder(expected, queryEngine.eval(queryPlan));

        } finally {

            store.__tearDownUnitTest();
            
        }

    }

}
