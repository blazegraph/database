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
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
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
     * Unit test for the AST rewrite of a simple describe query involving an IRI
     * and a variable bound by a WHERE clause. The IRI in this case was chosen
     * such that it would not be selected by the WHERE clause.
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

            final BigdataStatement[] stmts = new BigdataStatement[] {//

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDF.TYPE.toString()),
                            f.createURI(FOAFVocabularyDecl.Person
                                    .toString()), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Mike"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Mike"), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/Bryan"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("Bryan"), null, // context
                            StatementEnum.Explicit,//
                            false// userFlag
                    ),

                    new BigdataStatementImpl(
                            f.createURI("http://www.bigdata.com/DC"),
                            f.createURI(RDFS.LABEL.toString()),
                            f.createLiteral("DC"), null, // context
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
                    .optimize(context, queryRoot, null/* bindingSet */);

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

}
