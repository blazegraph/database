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
 * Created on Sep 13, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for the {@link ASTNamedSubqueryOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME Add unit test where there are no join variables because there
 *          is nothing which must be bound in one context or the other.
 * 
 *          FIXME Add unit test where there are no join variables because the
 *          named solution set is included from multiple locations without any
 *          overlap in the incoming bindings.
 * 
 *          FIXME Add unit test where there are join variables, but they are
 *          only those which overlap for the different locations at which the
 *          named solution set is joined into the query.
 */
public class TestASTNamedSubqueryOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTNamedSubqueryOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTNamedSubqueryOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for computing the join variables for a named subquery based on
     * the analysis of the bindings which MUST be produced by the subquery and
     * those which MUST be bound on entry into the group in which the subquery
     * solution set is included within the main query.
     * <p>
     * The join should be on <code>?x</code> in this example.
     * 
     * FIXME Actually, whether we use a join on <code>?x</code> or whether there
     * are NO join variables depends on the expected cardinality of the named
     * solution set and <code>?x rdfs:label ?o</code>. We should only use
     * <code>?x</code> as a join variable if the expected cardinality of
     * <code>?x rdfs:label ?o</code> is LT the expected cardinality of the named
     * solution set. In fact, for this case it probably won't be.
     * 
     * FIXME Write unit tests where the INCLUDE is embedded into a child group.
     * In that location we know that some things are already bound by the parent
     * group so we can be assured that we will use the available join variables.
     * [This really depends on running the INCLUDE after the non-optional joins
     * in the parent, which is itself just a heuristic. In fact, the entire
     * notion of
     * {@link StaticAnalysis#getIncomingBindings(IBindingProducerNode, Set)}
     * depends on this heuristic!]
     */
    public void test_static_analysis_join_vars() throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x ?o \n"+
                " with {"+
                "   select ?x where { ?x rdf:type foaf:Person }\n"+
                " } AS %namedSet1 \n"+
                "where { \n" +
                "   ?x rdfs:label ?o \n" +
                "   INCLUDE %namedSet1 \n"+
                "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        // Run the optimizers to determine the join variables.
        queryRoot = (QueryRoot) new DefaultOptimizerList().optimize(context,
                queryRoot, null/* bindingSets */);

        // The expected join variables.
        final IVariable[] joinVars = new IVariable[] { Var.var("x") };

        final NamedSubqueryRoot nsr = (NamedSubqueryRoot) queryRoot
                .getNamedSubqueries().get(0);

        final NamedSubqueryInclude nsi = (NamedSubqueryInclude) queryRoot
                .getWhereClause().get(1);

        /*
         * TODO This is failing for the reasons documented above. I've left this
         * test case here as a place holder for the issue.
         */
        assertEquals(joinVars, nsr.getJoinVars());

        assertEquals(joinVars, nsi.getJoinVars());

    }

    /**
     * This unit test is a based on
     * <code>bigdata-perf/CI/govtrack/queries/query10.rq</code>
     * <p>
     * Rewrite:
     * 
     * <pre>
     * SELECT ?_var1
     *  WHERE {
     *         ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
     *         OPTIONAL {
     *                 ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
     *         }.
     *         OPTIONAL {
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
     *         }.
     *         OPTIONAL {
     *                 ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
     *                 ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?_var10
     *         }
     * }
     * </pre>
     * 
     * as:
     * 
     * <pre>
     * SELECT  ?_var1
     * WITH {
     *         SELECT ?_var1 ?_var6
     *         WHERE {
     *                 ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
     *                 OPTIONAL {
     *                      ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
     *                 }.
     *         }
     * } as %_set1
     * WITH {
     *         SELECT ?_var1 ?_var4 ?_var12
     *         WHERE {
     *            INCLUDE %_set1
     *            OPTIONAL {
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
     *                 ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
     *            }.
     *         }
     * } as %_set2
     * WITH {
     *      SELECT ?_var1 ?_var10 ?_var13
     *      WHERE {
     *         INCLUDE %_set1
     *         OPTIONAL {
     *                 ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
     *                 ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?_var10
     *         }
     *     }
     * } as %_set3
     *  WHERE {
     *         INCLUDE %_set2 .
     *         INCLUDE %_set3 JOIN ON (?_var1) .
     * }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
     * 
     * TODO We need a unit test where there are some filters which should
     * move and some which should not.
     */
    public void test_rewriteComplexOptional() {
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV polititian = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/Politician"));
        @SuppressWarnings("rawtypes")
        final IV name = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/name"));
        @SuppressWarnings("rawtypes")
        final IV sponsor = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/sponsor"));
        @SuppressWarnings("rawtypes")
        final IV title = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/title"));
        @SuppressWarnings("rawtypes")
        final IV N = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#N"));
        @SuppressWarnings("rawtypes")
        final IV family = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#Family"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
            }

            final String set1 = "--nsr-1";
            final String set2 = "--nsr-2";
            final String set3 = "--nsr-3";

            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
            given.setNamedSubqueries(namedSubqueries);

            // Required joins and simple optionals lifted into a named subquery.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set1);
                namedSubqueries.add(nsr);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                
                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
                whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
                        new ConstantNode(a), new ConstantNode(polititian),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
                {
                    final StatementPatternNode sp = new StatementPatternNode(
                            new VarNode("var1"), new ConstantNode(name),
                            new VarNode("var6"), null/* c */,
                            Scope.DEFAULT_CONTEXTS);
                    sp.setSimpleOptional(true);
                    whereClause.addChild(sp);
                }
                
            }

            // First complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set2);
                namedSubqueries.add(nsr);

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                projection.addProjectionVar(new VarNode("var12"));
                projection.addProjectionVar(new VarNode("var4"));
                
                whereClause.addChild(new NamedSubqueryInclude(set1));

                {
                    
                    final JoinGroupNode optionalGroup1 = new JoinGroupNode(true/* optional */);
                    whereClause.addChild(optionalGroup1);

                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                            "var12"), new ConstantNode(sponsor), new VarNode(
                            "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
                    
                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                            "var12"), new ConstantNode(title), new VarNode(
                            "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
                    
                }

            }

            // Second complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set3);
                namedSubqueries.add(nsr);

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                projection.addProjectionVar(new VarNode("var13"));
                projection.addProjectionVar(new VarNode("var10"));
                
                whereClause.addChild(new NamedSubqueryInclude(set1));

                {

                    final JoinGroupNode optionalGroup2 = new JoinGroupNode(true/* optional */);
                    whereClause.addChild(optionalGroup2);

                    // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
                    optionalGroup2.addChild(new StatementPatternNode(
                            new VarNode("var1"), new ConstantNode(N),
                            new VarNode("var13"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family>
                    // ?_var10
                    optionalGroup2.addChild(new StatementPatternNode(
                            new VarNode("var13"), new ConstantNode(family),
                            new VarNode("var10"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                }
            }
            
            // WHERE
            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);
                whereClause.addChild(new NamedSubqueryInclude(set2));
                whereClause.addChild(new NamedSubqueryInclude(set3));
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode complexOpt1;
        final JoinGroupNode complexOpt2;
        {

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
            }

            final String set1 = "--nsr-1";
            final String set2 = "--nsr-2";
            final String set3 = "--nsr-3";

            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
            expected.setNamedSubqueries(namedSubqueries);

            // Required joins and simple optionals lifted into a named subquery.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set1);
                namedSubqueries.add(nsr);
                nsr.setJoinVars(new VarNode[]{/*none*/});
                nsr.setProperty(NamedSubqueryRoot.Annotations.DEPENDS_ON,new String[]{});

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                
                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
                whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
                        new ConstantNode(a), new ConstantNode(polititian),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
                {
                    final StatementPatternNode sp = new StatementPatternNode(
                            new VarNode("var1"), new ConstantNode(name),
                            new VarNode("var6"), null/* c */,
                            Scope.DEFAULT_CONTEXTS);
                    sp.setSimpleOptional(true);
                    whereClause.addChild(sp);
                }
                
            }

            // First complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set2);
                namedSubqueries.add(nsr);
                nsr.setJoinVars(new VarNode[]{/*none*/});
                nsr.setProperty(NamedSubqueryRoot.Annotations.DEPENDS_ON,new String[]{set1});

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                projection.addProjectionVar(new VarNode("var12"));
                projection.addProjectionVar(new VarNode("var4"));
                
                final NamedSubqueryInclude nsi1 = new NamedSubqueryInclude(set1);
                nsi1.setJoinVars(new VarNode[]{/*none*/});
                whereClause.addChild(nsi1);

                {
                    
                    final JoinGroupNode optionalGroup1 = new JoinGroupNode(true/* optional */);
                    whereClause.addChild(optionalGroup1);

                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                            "var12"), new ConstantNode(sponsor), new VarNode(
                            "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
                    
                    // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
                    optionalGroup1.addChild(new StatementPatternNode(new VarNode(
                            "var12"), new ConstantNode(title), new VarNode(
                            "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
                    
                    complexOpt1 = optionalGroup1;
                }

            }

            // Second complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set3);
                namedSubqueries.add(nsr);
                nsr.setJoinVars(new VarNode[]{new VarNode("var1")});
                nsr.setProperty(NamedSubqueryRoot.Annotations.DEPENDS_ON,new String[]{set1});

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                projection.addProjectionVar(new VarNode("var13"));
                projection.addProjectionVar(new VarNode("var10"));
                
                final NamedSubqueryInclude nsi1 = new NamedSubqueryInclude(set1);
                nsi1.setJoinVars(new VarNode[]{/*none*/});
                whereClause.addChild(nsi1);

                {

                    final JoinGroupNode optionalGroup2 = new JoinGroupNode(true/* optional */);
                    whereClause.addChild(optionalGroup2);

                    // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
                    optionalGroup2.addChild(new StatementPatternNode(
                            new VarNode("var1"), new ConstantNode(N),
                            new VarNode("var13"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family>
                    // ?_var10
                    optionalGroup2.addChild(new StatementPatternNode(
                            new VarNode("var13"), new ConstantNode(family),
                            new VarNode("var10"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    complexOpt2 = optionalGroup2;
                }
            }
            
            // WHERE
            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
                
                final NamedSubqueryInclude nsi2 = new NamedSubqueryInclude(set2);
                nsi2.setJoinVars(new VarNode[]{/*none*/});
                whereClause.addChild(nsi2);
                
                final NamedSubqueryInclude nsi3 = new NamedSubqueryInclude(set3);
                nsi3.setJoinVars(new VarNode[]{new VarNode("var1")});
                whereClause.addChild(nsi3);
            }

        }

        final IASTOptimizer rewriter = new ASTNamedSubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

        final StaticAnalysis sa = new StaticAnalysis(expected);

        // Test "incoming" bindings.
        assertEquals(asSet(new Var[] { Var.var("var1") }),
                sa.getDefinitelyIncomingBindings(complexOpt1,
                        new LinkedHashSet<IVariable<?>>()));

        // Test "incoming" bindings.
        assertEquals(asSet(new Var[] { Var.var("var1") }),
                sa.getDefinitelyIncomingBindings(complexOpt2,
                        new LinkedHashSet<IVariable<?>>()));

    }

}
