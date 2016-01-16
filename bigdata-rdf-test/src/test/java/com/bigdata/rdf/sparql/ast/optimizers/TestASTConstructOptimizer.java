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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for the {@link ASTConstructOptimizer}. This is applied for both
 * DESCRIBE and CONSTRUCT queries.  It generates the {@link ProjectionNode},
 * populating it with all of the variables in the {@link ConstructNode}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTConstructOptimizer extends AbstractASTEvaluationTestCase {

    public TestASTConstructOptimizer() {
        super();
    }

    public TestASTConstructOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for the AST rewrite of a CONSTRUCT query based on the
     * hand-coded rewrite of a simple DESCRIBE query involving an IRI and a
     * variable bound by a WHERE clause. The IRI in this case was chosen such
     * that it would not be selected by the WHERE clause.
     * 
     * <pre>
     * describe <http://www.bigdata.com/DC> ?x 
     * where {
     *   ?x rdf:type foaf:Person
     * }
     * </pre>
     * 
     * This test verifies that the correct CONSTRUCT clause is generated and
     * that a WHERE clause is added which will capture the necessary bindings to
     * support that CONSTUCT while preserving the original WHERE clause.
     */
    public void test_construct_rewrite() {

        final BigdataValueFactory f = store.getValueFactory();

        // Add some data.
        {

            final BigdataURI g = f.createURI("http://www.bigdata.com");

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person.toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

            };

            final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                    store, 10/* capacity */);

            for (BigdataStatement stmt : stmts) {

                buf.add(stmt);

            }

            // write on the database.
            buf.flush();

        }

        final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

        final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

        final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                .toString());

        final BigdataURI mikeURI = f
                .createURI("http://www.bigdata.com/Mike");

        final BigdataURI bryanURI = f
                .createURI("http://www.bigdata.com/Bryan");

        final BigdataLiteral mikeLabel = f.createLiteral("Mike");

        final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

        final BigdataURI dcURI = f.createURI("http://www.bigdata.com/DC");

        final BigdataValue[] values = new BigdataValue[] { rdfType,
                rdfsLabel, foafPerson, mikeURI, bryanURI, mikeLabel,
                bryanLabel, dcURI };

        // resolve IVs.
        store.getLexiconRelation()
                .addTerms(values, values.length, true/* readOnly */);

        /*
         * Setup the starting point for the AST model.
         */
        final QueryRoot queryRoot = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            // term1 ?p1a ?o1 .
            // ?s1 ?p1b term1 .
            final ConstructNode constructNode = new ConstructNode();
            queryRoot.setConstruct(constructNode);

            // DC
            final ConstantNode term0 = new ConstantNode(dcURI.getIV());
            final VarNode p0a = new VarNode("p0a");
            final VarNode p0b = new VarNode("p0b");
            final VarNode o0 = new VarNode("o0");
            final VarNode s0 = new VarNode("s0");
            constructNode
                    .addChild(new StatementPatternNode(term0, p0a, o0));
            constructNode
                    .addChild(new StatementPatternNode(s0, p0b, term0));

            // ?x
            final VarNode term1 = new VarNode("x");
            final VarNode p1a = new VarNode("p1a");
            final VarNode p1b = new VarNode("p1b");
            final VarNode o1 = new VarNode("o1");
            final VarNode s1 = new VarNode("s1");
            constructNode
                    .addChild(new StatementPatternNode(term1, p1a, o1));
            constructNode
                    .addChild(new StatementPatternNode(s1, p1b, term1));

            /*
             * Setup the WHERE clause.
             * 
             * Note: The original WHERE clause is preserved and we add a
             * union of two statement patterns for each term (variable or
             * constant) in the DESCRIBE clause.
             */
            // {
            // term1 ?p1a ?o1 .
            // } union {
            // ?s1 ?p1b term1 .
            // }
            final JoinGroupNode whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // union of 2 statement patterns per described term.
            final UnionNode union = new UnionNode();
            whereClause.addChild(union);

            union.addChild(new JoinGroupNode(new StatementPatternNode(term0, p0a, o0)));
            union.addChild(new JoinGroupNode(new StatementPatternNode(s0, p0b, term0)));

            union.addChild(new JoinGroupNode(new StatementPatternNode(term1, p1a, o1)));
            union.addChild(new JoinGroupNode(new StatementPatternNode(s1, p1b, term1)));

        }

        /*
         * Setup the expected AST model.
         */
        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            // term1 ?p1a ?o1 .
            // ?s1 ?p1b term1 .
            final ConstructNode constructNode = new ConstructNode();
            expected.setConstruct(constructNode);

            // DC
            final ConstantNode term0 = new ConstantNode(dcURI.getIV());
            final VarNode p0a = new VarNode("p0a");
            final VarNode p0b = new VarNode("p0b");
            final VarNode o0 = new VarNode("o0");
            final VarNode s0 = new VarNode("s0");
            constructNode
                    .addChild(new StatementPatternNode(term0, p0a, o0));
            constructNode
                    .addChild(new StatementPatternNode(s0, p0b, term0));

            // ?x
            final VarNode term1 = new VarNode("x");
            final VarNode p1a = new VarNode("p1a");
            final VarNode p1b = new VarNode("p1b");
            final VarNode o1 = new VarNode("o1");
            final VarNode s1 = new VarNode("s1");
            constructNode
                    .addChild(new StatementPatternNode(term1, p1a, o1));
            constructNode
                    .addChild(new StatementPatternNode(s1, p1b, term1));

            /*
             * Setup the WHERE clause.
             * 
             * Note: The original WHERE clause is preserved and we add a
             * union of two statement patterns for each term (variable or
             * constant) in the DESCRIBE clause.
             */
            // {
            // term1 ?p1a ?o1 .
            // } union {
            // ?s1 ?p1b term1 .
            // }
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // union of 2 statement patterns per described term.
            final UnionNode union = new UnionNode();
            whereClause.addChild(union);

            union.addChild(new JoinGroupNode(new StatementPatternNode(term0, p0a, o0)));
            union.addChild(new JoinGroupNode(new StatementPatternNode(s0, p0b, term0)));

            union.addChild(new JoinGroupNode(new StatementPatternNode(term1, p1a, o1)));
            union.addChild(new JoinGroupNode(new StatementPatternNode(s1, p1b, term1)));

            /*
             * Setup the PROJECTION node. 
             * 
             * This is the only thing which should differ from the code
             * block above. 
             */
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.setReduced(true);
            
            projection.addProjectionVar(p0a);
            projection.addProjectionVar(p0b);
            projection.addProjectionVar(s0);
            projection.addProjectionVar(o0);
            
            projection.addProjectionVar(term1);
            projection.addProjectionVar(p1a);
            projection.addProjectionVar(p1b);
            projection.addProjectionVar(s1);
            projection.addProjectionVar(o1);

            // Sort the args for comparison.
            final BOp[] args = projection.args().toArray(new BOp[0]);
            Arrays.sort(args);
            projection.setArgs(args);
            
        }

        /*
         * Rewrite the query and verify that the expected AST was produced.
         */
        {

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            final QueryRoot actual = (QueryRoot) new ASTConstructOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, null))
                    .getQueryNode();

            // Sort the args for comparison.
            {
                final ProjectionNode projection = actual.getProjection();
                assertNotNull(projection);
                final BOp[] args = projection.args().toArray(new BOp[0]);
                Arrays.sort(args);
                projection.setArgs(args);
            }
            
            assertSameAST(expected, actual);

        }
        
    }

    /**
     * Unit test for a CONSTRUCT query with NO solution modifiers. This
     * is the base case for the next several unit tests.
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *   ?x rdf:type foaf:Person
     * }
     * </pre>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/577" >
     *      DESCRIBE with OFFSET/LIMIT must use sub-SELECT </a>
     */
    public void test_construct_withSolutionModifiers_none() {

        final BigdataValueFactory f = store.getValueFactory();

        // Add some data.
        {

            final BigdataURI g = f.createURI("http://www.bigdata.com");

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

            };

            final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                    store, 10/* capacity */);

            for (BigdataStatement stmt : stmts) {

                buf.add(stmt);

            }

            // write on the database.
            buf.flush();

        }

        final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

        final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

        final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                .toString());

        final BigdataURI mikeURI = f
                .createURI("http://www.bigdata.com/Mike");

        final BigdataURI bryanURI = f
                .createURI("http://www.bigdata.com/Bryan");

        final BigdataLiteral mikeLabel = f.createLiteral("Mike");

        final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

        final BigdataURI dcURI = f.createURI("http://www.bigdata.com/DC");

        final BigdataValue[] values = new BigdataValue[] { rdfType,
                rdfsLabel, foafPerson, mikeURI, bryanURI, mikeLabel,
                bryanLabel, dcURI };

        // resolve IVs.
        store.getLexiconRelation()
                .addTerms(values, values.length, true/* readOnly */);

        /*
         * Setup the starting point for the AST model.
         */
        final QueryRoot queryRoot = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            queryRoot.setConstruct(constructNode);

            // ?x rdf:type foaf:Person
            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the WHERE clause.
             * 
             * Note: The original WHERE clause is preserved and we add a
             * union of two statement patterns for each term (variable or
             * constant) in the DESCRIBE clause.
             */
            final JoinGroupNode whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        /*
         * Setup the expected AST model.
         */
        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            expected.setConstruct(constructNode);

            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the WHERE clause.
             */

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            /*
             * Projection is added for the top-level query .
             */
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.setReduced(true);

                projection.addProjectionVar(new VarNode("x"));
            }
            
        }

        /*
         * Rewrite the query and verify that the expected AST was produced.
         */
        {

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            final QueryRoot actual = (QueryRoot) new ASTConstructOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, null))
                    .getQueryNode();
            
            assertSameAST(expected, actual);

        }
        
    }

    /**
     * Unit test verifies that a CONSTRUCT query (DESCRIBE is rewritten as
     * CONSTRUCT) with an OFFSET/LIMIT pushes the original WHERE clause and the
     * OFFSET/LIMIT into a sub-SELECT. This is necessary in order for the slice
     * to be applied to the WHERE clause rather than the triples in the
     * CONSTRUCTed graph.
     * 
     * For example:
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *   ?x rdf:type foaf:Person
     * }
     * OFFSET 1
     * LIMIT 1
     * </pre>
     * 
     * is turned into the following:
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *    SELECT ?x WHERE {
     *      ?x rdf:type foaf:Person
     *    }
     *    OFFSET 1
     *    LIMIT 1
     * }
     * </pre>
     * 
     * Note: If an ORDER BY clause is also present, then it must also be pushed
     * into the sub-SELECT. However, if only an ORDER BY clause is present
     * (without an OFFSET/LIMIT), then the ORDER BY clause should simply be
     * dropped.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/577" >
     *      DESCRIBE with OFFSET/LIMIT must use sub-SELECT </a>
     */
    public void test_construct_withSolutionModifiers_offsetLimit() {

        final BigdataValueFactory f = store.getValueFactory();

        // Add some data.
        {

            final BigdataURI g = f.createURI("http://www.bigdata.com");

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

            };

            final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                    store, 10/* capacity */);

            for (BigdataStatement stmt : stmts) {

                buf.add(stmt);

            }

            // write on the database.
            buf.flush();

        }

        final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

        final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

        final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                .toString());

        final BigdataURI mikeURI = f
                .createURI("http://www.bigdata.com/Mike");

        final BigdataURI bryanURI = f
                .createURI("http://www.bigdata.com/Bryan");

        final BigdataLiteral mikeLabel = f.createLiteral("Mike");

        final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

        final BigdataURI dcURI = f.createURI("http://www.bigdata.com/DC");

        final BigdataValue[] values = new BigdataValue[] { rdfType,
                rdfsLabel, foafPerson, mikeURI, bryanURI, mikeLabel,
                bryanLabel, dcURI };

        // resolve IVs.
        store.getLexiconRelation()
                .addTerms(values, values.length, true/* readOnly */);

        /*
         * Setup the starting point for the AST model.
         */
        final QueryRoot queryRoot = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            queryRoot.setConstruct(constructNode);

            // ?x rdf:type foaf:Person
            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the WHERE clause.
             * 
             * Note: The original WHERE clause is preserved and we add a
             * union of two statement patterns for each term (variable or
             * constant) in the DESCRIBE clause.
             */
            final JoinGroupNode whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            /*
             * The OFFSET/LIMIT.
             */
            queryRoot.setSlice(new SliceNode(1L/* offset */, 1L/* limit */));

        }

        /*
         * Setup the expected AST model.
         */
        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            expected.setConstruct(constructNode);

            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the sub-SELECT.
             */
            final SubqueryRoot subqueryRoot = new SubqueryRoot(QueryType.SELECT);
            {

                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                // original WHERE clause.
                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfType.getIV()), new ConstantNode(
                                foafPerson.getIV()), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                /*
                 * The OFFSET/LIMIT.
                 */
                subqueryRoot
                        .setSlice(new SliceNode(1L/* offset */, 1L/* limit */));

                /*
                 * Projection for the sub-SELECT is the same as for the
                 * top-level query.
                 */
                {
                    final ProjectionNode projection = new ProjectionNode();
                    subqueryRoot.setProjection(projection);
                    
                    projection.setReduced(true);
                    
                    projection.addProjectionVar(new VarNode("x"));
                }
                
            }

            /*
             * Setup the top-level WHERE clause.
             */
            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(subqueryRoot);
            }

            /*
             * Setup the PROJECTION node on the top-level query. This should be
             * identical to the projection on the sub-SELECT.
             */
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.setReduced(true);

                projection.addProjectionVar(new VarNode("x"));
            }
            
        }

        /*
         * Rewrite the query and verify that the expected AST was produced.
         */
        {

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            final QueryRoot actual = (QueryRoot) new ASTConstructOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, null))
                    .getQueryNode();
            
            assertSameAST(expected, actual);

        }
        
    }

    /**
     * Unit test verifies that a CONSTRUCT query (DESCRIBE is rewritten as
     * CONSTRUCT) with an OFFSET/LIMIT and an ORDER BY pushes the original WHERE
     * clause, the OFFSET/LIMIT, and the ORDER BY into a sub-SELECT. This is
     * necessary in order for the SLICE and the ORDER BY to be applied to the
     * WHERE clause rather than the triples in the CONSTRUCTed graph.
     * 
     * For example:
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *   ?x rdf:type foaf:Person
     * }
     * ORDER BY ?x
     * OFFSET 1
     * LIMIT 1
     * </pre>
     * 
     * is turned into the following:
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *    SELECT ?x WHERE {
     *      ?x rdf:type foaf:Person
     *    }
     *    ORDER BY ?x
     *    OFFSET 1
     *    LIMIT 1
     * }
     * </pre>
     * 
     * Note: If an ORDER BY clause is also present, then it must also be pushed
     * into the sub-SELECT. However, if only an ORDER BY clause is present
     * (without an OFFSET/LIMIT), then the ORDER BY clause should simply be
     * dropped.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/577" >
     *      DESCRIBE with OFFSET/LIMIT must use sub-SELECT </a>
     */
    public void test_construct_withSolutionModifiers_offsetLimit_orderBy() {

        final BigdataValueFactory f = store.getValueFactory();

        // Add some data.
        {

            final BigdataURI g = f.createURI("http://www.bigdata.com");

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

            };

            final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                    store, 10/* capacity */);

            for (BigdataStatement stmt : stmts) {

                buf.add(stmt);

            }

            // write on the database.
            buf.flush();

        }

        final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

        final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

        final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                .toString());

        final BigdataURI mikeURI = f
                .createURI("http://www.bigdata.com/Mike");

        final BigdataURI bryanURI = f
                .createURI("http://www.bigdata.com/Bryan");

        final BigdataLiteral mikeLabel = f.createLiteral("Mike");

        final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

        final BigdataURI dcURI = f.createURI("http://www.bigdata.com/DC");

        final BigdataValue[] values = new BigdataValue[] { rdfType,
                rdfsLabel, foafPerson, mikeURI, bryanURI, mikeLabel,
                bryanLabel, dcURI };

        // resolve IVs.
        store.getLexiconRelation()
                .addTerms(values, values.length, true/* readOnly */);

        /*
         * Setup the starting point for the AST model.
         */
        final QueryRoot queryRoot = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            queryRoot.setConstruct(constructNode);

            // ?x rdf:type foaf:Person
            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the WHERE clause.
             * 
             * Note: The original WHERE clause is preserved and we add a
             * union of two statement patterns for each term (variable or
             * constant) in the DESCRIBE clause.
             */
            final JoinGroupNode whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            /*
             * The OFFSET/LIMIT.
             */
            queryRoot.setSlice(new SliceNode(1L/* offset */, 1L/* limit */));

            /*
             * The ORDER_BY clause.
             */
            {
                final OrderByNode orderBy = new OrderByNode();

                orderBy.addExpr(new OrderByExpr(new VarNode("x"), true/* ascending */));

                queryRoot.setOrderBy(orderBy);

            }

        }

        /*
         * Setup the expected AST model.
         */
        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            expected.setConstruct(constructNode);

            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the sub-SELECT.
             */
            final SubqueryRoot subqueryRoot = new SubqueryRoot(QueryType.SELECT);
            {

                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                // original WHERE clause.
                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfType.getIV()), new ConstantNode(
                                foafPerson.getIV()), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                /*
                 * The OFFSET/LIMIT.
                 */
                subqueryRoot
                        .setSlice(new SliceNode(1L/* offset */, 1L/* limit */));

                /*
                 * The ORDER_BY clause.
                 */
                {
                    final OrderByNode orderBy = new OrderByNode();

                    orderBy.addExpr(new OrderByExpr(new VarNode("x"), true/* ascending */));

                    subqueryRoot.setOrderBy(orderBy);

                }

                /*
                 * Projection for the sub-SELECT is the same as for the
                 * top-level query.
                 */
                {
                    final ProjectionNode projection = new ProjectionNode();
                    subqueryRoot.setProjection(projection);
                    
                    projection.setReduced(true);
                    
                    projection.addProjectionVar(new VarNode("x"));
                }
                
            }

            /*
             * Setup the top-level WHERE clause.
             */
            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(subqueryRoot);
            }

            /*
             * Setup the PROJECTION node on the top-level query. This should be
             * identical to the projection on the sub-SELECT.
             */
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.setReduced(true);

                projection.addProjectionVar(new VarNode("x"));
            }
            
        }

        /*
         * Rewrite the query and verify that the expected AST was produced.
         */
        {

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            final QueryRoot actual = (QueryRoot) new ASTConstructOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, null))
                    .getQueryNode();
            
            assertSameAST(expected, actual);

        }
        
    }

    /**
     * Variant on the test above verifies that a CONSTRUCT query (DESCRIBE is
     * rewritten as CONSTRUCT) with an ORDER_BY clause drops the ORDER_BY clause
     * when OFFSET/LIMIT are not present.
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *   ?x rdf:type foaf:Person
     * }
     * ORDER BY ?x
     * </pre>
     * 
     * is turned into the following:
     * 
     * <pre>
     * CONSTRUCT {
     *   ?x rdf:type foaf:Person
     * } 
     * WHERE {
     *   ?x rdf:type foaf:Person
     * }
     * </pre>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/577" >
     *      DESCRIBE with OFFSET/LIMIT must use sub-SELECT </a>
     */
    public void test_construct_withSolutionModifiers_orderBy() {

        final BigdataValueFactory f = store.getValueFactory();

        // Add some data.
        {

            final BigdataURI g = f.createURI("http://www.bigdata.com");

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"),//
                            g, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

            };

            final StatementBuffer<BigdataStatement> buf = new StatementBuffer<BigdataStatement>(
                    store, 10/* capacity */);

            for (BigdataStatement stmt : stmts) {

                buf.add(stmt);

            }

            // write on the database.
            buf.flush();

        }

        final BigdataURI rdfType = f.createURI(RDF.TYPE.toString());

        final BigdataURI rdfsLabel = f.createURI(RDFS.LABEL.toString());

        final BigdataURI foafPerson = f.createURI(FOAFVocabularyDecl.Person
                .toString());

        final BigdataURI mikeURI = f
                .createURI("http://www.bigdata.com/Mike");

        final BigdataURI bryanURI = f
                .createURI("http://www.bigdata.com/Bryan");

        final BigdataLiteral mikeLabel = f.createLiteral("Mike");

        final BigdataLiteral bryanLabel = f.createLiteral("Bryan");

        final BigdataURI dcURI = f.createURI("http://www.bigdata.com/DC");

        final BigdataValue[] values = new BigdataValue[] { rdfType,
                rdfsLabel, foafPerson, mikeURI, bryanURI, mikeLabel,
                bryanLabel, dcURI };

        // resolve IVs.
        store.getLexiconRelation()
                .addTerms(values, values.length, true/* readOnly */);

        /*
         * Setup the starting point for the AST model.
         */
        final QueryRoot queryRoot = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            queryRoot.setConstruct(constructNode);

            // ?x rdf:type foaf:Person
            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the WHERE clause.
             * 
             * Note: The original WHERE clause is preserved and we add a
             * union of two statement patterns for each term (variable or
             * constant) in the DESCRIBE clause.
             */
            final JoinGroupNode whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            /*
             * The ORDER_BY clause.
             */
            {
                final OrderByNode orderBy = new OrderByNode();

                orderBy.addExpr(new OrderByExpr(new VarNode("x"), true/* ascending */));
                
                queryRoot.setOrderBy(orderBy);
                
            }

        }

        /*
         * Setup the expected AST model.
         */
        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {
            /*
             * Setup the CONSTRUCT node.
             */
            final ConstructNode constructNode = new ConstructNode();
            expected.setConstruct(constructNode);

            constructNode.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV())));

            /*
             * Setup the WHERE clause.
             */
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // original WHERE clause.
            whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                    new ConstantNode(rdfType.getIV()), new ConstantNode(
                            foafPerson.getIV()), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            /*
             * Setup the PROJECTION.
             */
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.setReduced(true);

                projection.addProjectionVar(new VarNode("x"));
            }
            
        }

        /*
         * Rewrite the query and verify that the expected AST was produced.
         */
        {

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            final QueryRoot actual = (QueryRoot) new ASTConstructOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, null))
                    .getQueryNode();
            
            assertSameAST(expected, actual);

        }
        
    }
    
}
