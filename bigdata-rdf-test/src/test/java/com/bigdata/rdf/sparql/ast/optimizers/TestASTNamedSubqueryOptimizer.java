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
 * Created on Sep 13, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for the {@link ASTNamedSubqueryOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestASTNamedSubqueryOptimizer.java 6281 2012-04-12 17:27:36Z
 *          thompsonbry $
 * 
 *          FIXME Add unit test where there are no join variables because there
 *          is nothing which must be bound in one context or the other.
 * 
 *          FIXME Add unit test where there are no join variables because the
 *          named solution set is included from multiple locations without any
 *          overlap in the incoming bindings (consider doing a hash index build
 *          for each include location other than the most specific location (the
 *          one with the most possible join variables)).
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
     * <pre>
     * select ?x ?o
     * with {
     *  select ?x where { ?x rdf:type foaf:Person }
     * } AS %namedSet1
     * where {
     *  ?x rdfs:label ?o
     *  INCLUDE %namedSet1
     * }
     * </pre>
     * 
     * The join should be on <code>?x</code> in this example.
     * <p>
     * Note: Whether we use a join on <code>?x</code> or whether there are NO
     * join variables depends on the order imposed on the named subquery include
     * versus the statement pattern, which depends on the expected cardinality
     * of the named solution set and <code>?x rdfs:label ?o</code>. We should
     * only use <code>?x</code> as a join variable if the expected cardinality
     * of <code>?x rdfs:label ?o</code> is LT the expected cardinality of the
     * named solution set. In fact, for this case it probably won't be.
     */
    public void test_static_analysis_join_vars() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV rdfsLabel = makeIV(RDFS.LABEL);
        @SuppressWarnings("rawtypes")
        final IV person = makeIV(FOAFVocabularyDecl.Person);

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        final String namedSet1 = "namedSet1";
        final VarNode[] joinVars = new VarNode[]{new VarNode("x")};
        {
            
            // NamedSubqueryRoot
            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet1);
                given.getNamedSubqueriesNotNull().add(nsr);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));
                }

                {
                    final SubqueryRoot subqueryRoot = new SubqueryRoot(
                            QueryType.SELECT);
                    nsr.setWhereClause(new JoinGroupNode(subqueryRoot));

                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    nsr.setWhereClause(whereClause);

                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "x"), new ConstantNode(a),
                            new ConstantNode(person), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                }

            }
            
            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new NamedSubqueryInclude(namedSet1));
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            // NamedSubqueryRoot
            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet1);
                expected.getNamedSubqueriesNotNull().add(nsr);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));
                }

                {
                    final SubqueryRoot subqueryRoot = new SubqueryRoot(
                            QueryType.SELECT);
                    nsr.setWhereClause(new JoinGroupNode(subqueryRoot));

                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    nsr.setWhereClause(whereClause);

                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "x"), new ConstantNode(a),
                            new ConstantNode(person), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                }
                
                // No dependencies.
                nsr.setDependsOn(new String[]{});

                nsr.setJoinVars(joinVars);

            }
            
            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final NamedSubqueryInclude nsi = new NamedSubqueryInclude(namedSet1);
                whereClause.addChild(nsi);

                nsi.setJoinVars(joinVars);

            }

        }

        final IASTOptimizer rewriter = new ASTNamedSubqueryOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
               new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * Variant of {@link #test_static_analysis_join_vars()} where the order of
     * the {@link StatementPatternNode} and the {@link NamedSubqueryInclude} in
     * the main WHERE clause is reversed such that there are no join variables
     * for the INCLUDE.
     * <pre>
     * select ?x ?o
     * with {
     *  select ?x where { ?x rdf:type foaf:Person }
     * } AS %namedSet1
     * where {
     *  INCLUDE %namedSet1
     *  ?x rdfs:label ?o
     * }
     * </pre>
     */
    public void test_static_analysis_no_join_vars() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV rdfsLabel = makeIV(RDFS.LABEL);
        @SuppressWarnings("rawtypes")
        final IV person = makeIV(FOAFVocabularyDecl.Person);

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        final String namedSet1 = "namedSet1";
        final VarNode[] joinVars = new VarNode[]{};
        {
            
            // NamedSubqueryRoot
            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet1);
                given.getNamedSubqueriesNotNull().add(nsr);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));
                }

                {
                    final SubqueryRoot subqueryRoot = new SubqueryRoot(
                            QueryType.SELECT);
                    nsr.setWhereClause(new JoinGroupNode(subqueryRoot));

                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    nsr.setWhereClause(whereClause);

                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "x"), new ConstantNode(a),
                            new ConstantNode(person), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                }

            }
            
            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new NamedSubqueryInclude(namedSet1));
                
                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            // NamedSubqueryRoot
            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet1);
                expected.getNamedSubqueriesNotNull().add(nsr);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));
                }

                {
                    final SubqueryRoot subqueryRoot = new SubqueryRoot(
                            QueryType.SELECT);
                    nsr.setWhereClause(new JoinGroupNode(subqueryRoot));

                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("x"));

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    nsr.setWhereClause(whereClause);

                    whereClause.addChild(new StatementPatternNode(new VarNode(
                            "x"), new ConstantNode(a),
                            new ConstantNode(person), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                }
                
                // No dependencies.
                nsr.setDependsOn(new String[]{});

                nsr.setJoinVars(joinVars);

            }
            
            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final NamedSubqueryInclude nsi = new NamedSubqueryInclude(namedSet1);
                whereClause.addChild(nsi);

                nsi.setJoinVars(joinVars);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        final IASTOptimizer rewriter = new ASTNamedSubqueryOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * This unit test is a based on
     * <code>bigdata-perf/CI/govtrack/queries/query10.rq</code>
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
     *         INCLUDE %_set2 . # JOIN ON ()
     *         INCLUDE %_set3 . # JOIN ON (?_var1)
     * }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
     */
    public void test_govtrack_query10() {
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
                    sp.setOptional(true);
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
//                projection.addProjectionVar(new VarNode("var12"));
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
//                projection.addProjectionVar(new VarNode("var13"));
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
//                nsr.setJoinVars(new VarNode[]{new VarNode("var1")});
                nsr.setDependsOn(new String[]{});

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
                    sp.setOptional(true);
                    whereClause.addChild(sp);
                }
                
            }

            // First complex optional group.
            {
                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, set2);
                namedSubqueries.add(nsr);
                nsr.setJoinVars(new VarNode[]{/*none*/});
//                nsr.setJoinVars(new VarNode[]{new VarNode("var1")});
                nsr.setDependsOn(new String[]{set1});

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
//                projection.addProjectionVar(new VarNode("var12"));
                projection.addProjectionVar(new VarNode("var4"));
                
                final NamedSubqueryInclude nsi1 = new NamedSubqueryInclude(set1);
                nsi1.setJoinVars(new VarNode[]{/*none*/});
//                nsi1.setJoinVars(new VarNode[]{new VarNode("var1")});
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
                nsr.setDependsOn(new String[]{set1});

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
//                projection.addProjectionVar(new VarNode("var13"));
                projection.addProjectionVar(new VarNode("var10"));
                
                final NamedSubqueryInclude nsi1 = new NamedSubqueryInclude(set1);
                nsi1.setJoinVars(new VarNode[]{/*none*/});
//                nsi1.setJoinVars(new VarNode[]{new VarNode("var1")});
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
//                nsi2.setJoinVars(new VarNode[]{new VarNode("var1")});
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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

        final StaticAnalysis sa = new StaticAnalysis(expected, context);

        // Test "incoming" bindings.
        assertEquals(asSet(new Var[] { Var.var("var1") }),
                sa.getDefinitelyIncomingBindings(complexOpt1,
                        new LinkedHashSet<IVariable<?>>()));

        // Test "incoming" bindings.
        assertEquals(asSet(new Var[] { Var.var("var1") }),
                sa.getDefinitelyIncomingBindings(complexOpt2,
                        new LinkedHashSet<IVariable<?>>()));

//        System.err.println(new ASTSubGroupJoinVarOptimizer().optimize(context,
//                actual/* queryNode */, bsets));

    }
    

    
}
