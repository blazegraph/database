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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TestStaticAnalysisMethods;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTBottomUpOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestASTBottomUpOptimizer.java 5197 2011-09-15 19:10:44Z
 *          thompsonbry $
 * 
 * @see TestStaticAnalysisMethods
 */
public class TestASTBottomUpOptimizer extends
        AbstractASTEvaluationTestCase {

//    private static final Logger log = Logger
//            .getLogger(TestASTBottomUpOptimizer.class);
    
    public TestASTBottomUpOptimizer() {
        super();
    }

    public TestASTBottomUpOptimizer(final String name) {
        super(name);
    }

    /**
     * Nested Optionals - 1 (Query is not well designed because there are no
     * shared variables in the intermediate join group and there is an embedded
     * OPTIONAL join group. Since ?v is not present in the intermediate join
     * group the (:x3 :q ?w . OPTIONAL { :x2 :p ?v }) solutions must be computed
     * first and then joined against the (:x1 :p ?v) solutions.)
     * 
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT *
     * { 
     *     :x1 :p ?v .
     *     OPTIONAL
     *     {
     *       :x3 :q ?w .
     *       OPTIONAL { :x2 :p ?v }
     *     }
     * }
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_bottomUpOptimizer_nested_optionals_1()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    :x1 :p ?v . \n" + //
                "    OPTIONAL {" +
                "      :x3 :q ?w . \n" + //
                "      OPTIONAL { :x2 :p ?v  } \n" + //
                "    } \n" + //
                "}"//
        ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI x1 = f.createURI("http://example/x1");
        final BigdataURI x2 = f.createURI("http://example/x2");
        final BigdataURI x3 = f.createURI("http://example/x3");
        final BigdataURI p = f.createURI("http://example/p");
        final BigdataURI q = f.createURI("http://example/q");
        final BigdataValue[] values = new BigdataValue[] { x1, x2, x3, p, q };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final NamedSubqueriesNode namedSubqueries = queryRoot.getNamedSubqueries();
        
        assertNotNull("did not rewrite query", namedSubqueries);

        assertEquals(1, namedSubqueries.size());

        /*
         * Create the expected AST for the lifted expression and the rewritten
         * group from which the expression was lifted.
         */
        final String namedSet = ASTBottomUpOptimizer.NAMED_SET_PREFIX + "0";
        
        final NamedSubqueryRoot expectedNSR = new NamedSubqueryRoot(
                QueryType.SELECT, namedSet);
        
        final JoinGroupNode modifiedClause = new JoinGroupNode();
        
        {

            // The NamedSubqueryRoot
            {

                // projection
                {
                    final ProjectionNode projection = new ProjectionNode();
                    expectedNSR.setProjection(projection);
                    projection.addProjectionVar(new VarNode("w"));
                    projection.addProjectionVar(new VarNode("v"));
                }

                // where clause
                {
                    final JoinGroupNode liftedClause = new JoinGroupNode(true/* optional */);
                    final JoinGroupNode innerClause = new JoinGroupNode(true/* optional */);
                    // :x3 :q ?w
                    liftedClause.addChild(new StatementPatternNode(//
                            new ConstantNode(new Constant(x3.getIV())),// s
                            new ConstantNode(new Constant(q.getIV())),// p
                            new VarNode("w"),// o
                            null,// c
                            Scope.DEFAULT_CONTEXTS//
                            ));
                    liftedClause.addChild(innerClause);
                    // :x2 :p ?v
                    innerClause.addChild(new StatementPatternNode(//
                            new ConstantNode(new Constant(x2.getIV())),// s
                            new ConstantNode(new Constant(p.getIV())),// p
                            new VarNode("v"),// o
                            null,// c
                            Scope.DEFAULT_CONTEXTS//
                            ));

                    expectedNSR.setWhereClause(liftedClause);

                }
                
            }
            
            // The group from which the named subquery was lifted.
            {

                // :x1 :p ?v 
                modifiedClause.addChild(new StatementPatternNode(//
                        new ConstantNode(new Constant(x1.getIV())),// s
                        new ConstantNode(new Constant(p.getIV())),// p
                        new VarNode("v"),// o
                        null,// c
                        Scope.DEFAULT_CONTEXTS//
                        ));

                modifiedClause.addChild(new NamedSubqueryInclude(namedSet));
                
            }
            
        }
        
        final NamedSubqueryRoot nsr = (NamedSubqueryRoot) namedSubqueries
                .get(0);
        
        assertEquals("liftedClause", expectedNSR, nsr);

        assertEquals("modifiedClause", modifiedClause,
                queryRoot.getWhereClause());        

    }

    /**
     * Static analysis of TCK query:
     * 
     * <pre>
     * PREFIX :    <http://example/>
     * 
     * SELECT *
     * { 
     *     :x :p ?v . 
     *     { :x :q ?w 
     *       OPTIONAL {  :x :p ?v2 FILTER(?v = 1) }
     *     }
     * }
     * </pre>
     * 
     * <code>?v</code> is bound in the outer join group.
     * <p>
     * <code>?w</code> is bound in the child join group regardless of whether
     * the embedded optional succeeds.
     * <p>
     * <code>?v</code> is not bound in the FILTER because it is evaluated with
     * bottom up semantics and therefore the bindings from the outer parent
     * group are not visible.
     * <p>
     * For reference, the AST for that SPARQL query is:
     * 
     * <pre>
     * PREFIX : <http://example/>
     * QueryType: SELECT
     * SELECT * 
     *   JoinGroupNode {
     *     StatementPatternNode(ConstantNode(TermId(0U)[http://example/x]), ConstantNode(TermId(0U)[http://example/p]), VarNode(v), DEFAULT_CONTEXTS)
     *     JoinGroupNode {
     *       StatementPatternNode(ConstantNode(TermId(0U)[http://example/x]), ConstantNode(TermId(0U)[http://example/q]), VarNode(w), DEFAULT_CONTEXTS)
     *       JoinGroupNode [optional] {
     *         StatementPatternNode(ConstantNode(TermId(0U)[http://example/x]), ConstantNode(TermId(0U)[http://example/p]), VarNode(v2), DEFAULT_CONTEXTS)
     *         FILTER( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(v),ConstantNode(XSDInteger(1)))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.w3.org/2005/xpath-functions#equal-to] )
     *       }
     *     }
     *   }
     * </pre>
     * 
     * This query includes both a badly designed left join pattern, which must
     * be lifted out:
     * 
     * <pre>
     * { :x :q ?w OPTIONAL {  :x :p ?v2 FILTER(?v = 1) } }
     * </pre>
     * 
     * and a FILTER on a variable which is not in scope:
     * 
     * <pre>
     * FILTER(?v = 1)
     * </pre>
     * 
     * @see ASTBottomUpOptimizer
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_bottomUpOptimizer_filter_scope_1()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    :x :p ?v . \n" + //
                "    { :x :q ?w \n" + //
                "      OPTIONAL {  :x :p ?v2 FILTER(?v = 1) } \n" + //
                "    } \n" + //
                "}"//
        ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI x = f.createURI("http://example/x");
        final BigdataURI p = f.createURI("http://example/p");
        final BigdataURI q = f.createURI("http://example/q");
        final BigdataLiteral ONE = f.createLiteral("1",XSD.INTEGER);
        final BigdataValue[] values = new BigdataValue[] { x, p, q, ONE };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final NamedSubqueriesNode namedSubqueries = queryRoot.getNamedSubqueries();
        
        assertNotNull("did not rewrite query", namedSubqueries);

        assertEquals(1, namedSubqueries.size());

        /*
         * Create the expected AST for the lifted expression and the rewritten
         * group from which the expression was lifted.
         */
        final String namedSet = ASTBottomUpOptimizer.NAMED_SET_PREFIX + "0";
        
        final NamedSubqueryRoot expectedNSR = new NamedSubqueryRoot(
                QueryType.SELECT, namedSet);
        
        final JoinGroupNode modifiedClause = new JoinGroupNode();
        
        {

            // The NamedSubqueryRoot
            {

                // projection
                {
                    final ProjectionNode projection = new ProjectionNode();
                    expectedNSR.setProjection(projection);
                    projection.addProjectionVar(new VarNode("w"));
                    projection.addProjectionVar(new VarNode("v2"));
                }

                // where clause
                {
                    final JoinGroupNode liftedClause = new JoinGroupNode();
                    final JoinGroupNode innerClause = new JoinGroupNode(true/* optional */);
                    // :x :q ?w
                    liftedClause.addChild(new StatementPatternNode(//
                            new ConstantNode(new Constant(x.getIV())),// s
                            new ConstantNode(new Constant(q.getIV())),// p
                            new VarNode("w"),// o
                            null,// c
                            Scope.DEFAULT_CONTEXTS//
                            ));
                    liftedClause.addChild(innerClause);
                    // :x :p ?v2
                    innerClause.addChild(new StatementPatternNode(//
                            new ConstantNode(new Constant(x.getIV())),// s
                            new ConstantNode(new Constant(p.getIV())),// p
                            new VarNode("v2"),// o
                            null,// c
                            Scope.DEFAULT_CONTEXTS//
                            ));
                    // FILTER(?v = 1) => anonymous variable.
                    final FilterNode filterNode = new FilterNode(
                            new FunctionNode(FunctionRegistry.EQ,
                                    null/* scalarValues */,
                                    new ValueExpressionNode[] { // args
                                            new VarNode("-unbound-var-v-1"),//
                                            new ConstantNode(new Constant(ONE
                                                    .getIV())) //
                                    }//
                            ));
                    AST2BOpUtility.toVE(context.getLexiconNamespace(),
                            filterNode.getValueExpressionNode());
                    innerClause.addChild(filterNode);

                    expectedNSR.setWhereClause(liftedClause);

                }
                
            }
            
            // The group from which the named subquery was lifted.
            {
                
                // :x :p ?v 
                modifiedClause.addChild(new StatementPatternNode(//
                        new ConstantNode(new Constant(x.getIV())),// s
                        new ConstantNode(new Constant(p.getIV())),// p
                        new VarNode("v"),// o
                        null,// c
                        Scope.DEFAULT_CONTEXTS//
                        ));

                modifiedClause.addChild(new NamedSubqueryInclude(namedSet));
                
            }
            
        }
        
        final NamedSubqueryRoot nsr = (NamedSubqueryRoot) namedSubqueries
                .get(0);
        
        diff(expectedNSR, nsr);

        diff(modifiedClause, queryRoot.getWhereClause());        

    }
    
    /**
     * Join-scope - 1 (aka var-scope-join-1).
     * 
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT *
     * { 
     *   ?X  :name "paul"
     *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
     * }
     * </pre>
     * <p>
     * Note: For this query, the bindings of <code>?X</code> in the outer group
     * are not visible when the inner groups are evaluated. Because of this,
     * <code>?X</code> in the optional clause binds for values which differ from
     * the values bound on <code>?X</code> in the outer group. This means that
     * the solutions from the middle group with bindings for <code>?Y</code> and
     * <code>?X</code> fail to join with the solutions in the outer group. Note
     * that there is no email address for "paul" for the data set used to run
     * this query. If there were, then the query would have a result.
     * <p>
     * These group expressions need to be evaluated independently because they
     * are not sharing a binding for <code>?X</code> until we join them together
     * on <code>?X</code>.
     * <p>
     * In order for us to run this query correctly we need to run
     * 
     * <pre>
     * {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
     * </pre>
     * 
     * BEFORE
     * 
     * <pre>
     * ?X  :name "paul"
     * </pre>
     * 
     * This can be most easily achieved by lifting the former out into a named
     * subquery.
     * 
     * @see ASTBottomUpOptimizer
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_bottomUpOptimizer_join_scope_1()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    ?X  :name \"paul\" . \n" + //
                "    {?Y :name \"george\" . OPTIONAL { ?X :email ?Z } } \n" + //
                "}"//
        ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI name = f.createURI("http://example/name");
        final BigdataURI email = f.createURI("http://example/email");
        final BigdataLiteral paul = f.createLiteral("paul");
        final BigdataLiteral george = f.createLiteral("george");
        final BigdataValue[] values = new BigdataValue[] { name, email, paul,
                george };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final NamedSubqueriesNode namedSubqueries = queryRoot.getNamedSubqueries();
        
        assertNotNull(namedSubqueries);

        assertEquals(1, namedSubqueries.size());

        /*
         * Create the expected AST for the lifted expression and the rewritten
         * group from which the expression was lifted.
         */
        final String namedSet = ASTBottomUpOptimizer.NAMED_SET_PREFIX + "0";
        
        final NamedSubqueryRoot expectedNSR = new NamedSubqueryRoot(
                QueryType.SELECT, namedSet);
        
        final JoinGroupNode modifiedClause = new JoinGroupNode();
        
        {

            // The NamedSubqueryRoot
            {

                // projection
                {
                    final ProjectionNode projection = new ProjectionNode();
                    expectedNSR.setProjection(projection);
                    projection.addProjectionVar(new VarNode("Y"));
                    projection.addProjectionVar(new VarNode("X"));
                    projection.addProjectionVar(new VarNode("Z"));
                }

                // where clause
                {
                    final JoinGroupNode liftedClause = new JoinGroupNode();
                    final JoinGroupNode innerClause = new JoinGroupNode(true/* optional */);
                    // ?Y :name "george" 
                    liftedClause.addChild(new StatementPatternNode(//
                            new VarNode("Y"),// s
                            new ConstantNode(new Constant(name.getIV())),// p
                            new ConstantNode(new Constant(george.getIV())),// o
                            null,// c
                            Scope.DEFAULT_CONTEXTS//
                            ));
                    liftedClause.addChild(innerClause);
                    // ?X :email ?Z
                    innerClause.addChild(new StatementPatternNode(//
                            new VarNode("X"),// s
                            new ConstantNode(new Constant(email.getIV())),// p
                            new VarNode("Z"),// o
                            null,// c
                            Scope.DEFAULT_CONTEXTS//
                            ));

                    expectedNSR.setWhereClause(liftedClause);

                }
                
            }
            
            // The group from which the named subquery was lifted.
            {
                
                // ?X  :name \"paul\"
                modifiedClause.addChild(new StatementPatternNode(//
                        new VarNode("X"),// s
                        new ConstantNode(new Constant(name.getIV())),// p
                        new ConstantNode(new Constant(paul.getIV())),// o
                        null,// c
                        Scope.DEFAULT_CONTEXTS//
                        ));

                modifiedClause.addChild(new NamedSubqueryInclude(namedSet));
                
            }
            
        }
        
        final NamedSubqueryRoot nsr = (NamedSubqueryRoot) namedSubqueries
                .get(0);
        
        assertEquals("liftedClause", expectedNSR, nsr);

        assertEquals("modifiedClause", modifiedClause,
                queryRoot.getWhereClause());        
        
    }
    
    /**
     * Variant on {@link #test_bottomUpOptimizer_join_scope_1()} where the query
     * is well designed due to the presence of a shared variable <code>?X</code>
     * in the intermediate join group. This test verifies that we DO NOT rewrite
     * the query.
     */
    public void test_wellDesigned01()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    ?X  :name \"paul\" . \n" + //
                "    {?X :name \"george\" . OPTIONAL { ?X :email ?Z } } \n" + //
                "}"//
                ;
        
        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI name = f.createURI("http://example/name");
        final BigdataURI email = f.createURI("http://example/email");
        final BigdataLiteral paul = f.createLiteral("paul");
        final BigdataLiteral george = f.createLiteral("george");
        final BigdataValue[] values = new BigdataValue[] { name, email, paul,
                george };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final QueryRoot expected = BOpUtility.deepCopy(queryRoot); 
        
        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        // Verify NO transform.
        assertEquals(expected, queryRoot);
        
    }

    /**
     * Slight variation on the structure of the query in the test above which
     * should not be recognized as a badly designed left join.
     * 
     * @throws MalformedQueryException
     */
    public void test_wellDesigned02() throws MalformedQueryException {
    
        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    ?X  :name \"paul\" . \n" + //
                "    OPTIONAL {?X :name \"george\" . ?X :email ?Z } \n" + //
                "}"//
                ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI name = f.createURI("http://example/name");
        final BigdataURI email = f.createURI("http://example/email");
        final BigdataLiteral paul = f.createLiteral("paul");
        final BigdataLiteral george = f.createLiteral("george");
        final BigdataValue[] values = new BigdataValue[] { name, email, paul,
                george };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final QueryRoot expected = BOpUtility.deepCopy(queryRoot); 
        
        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        // Verify NO transform.
        assertEquals(expected, queryRoot);
        
    }

    /**
     * Slight variation on the structure of the query in the test above which
     * should not be recognized as a badly designed left join.
     * 
     * @throws MalformedQueryException
     */
    public void test_wellDesigned03() throws MalformedQueryException {
    
        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    ?X  :name \"paul\" . \n" + //
                "    OPTIONAL {?X :name \"george\" } OPTIONAL { ?X :email ?Z } \n" + //
                "}"//
                ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI name = f.createURI("http://example/name");
        final BigdataURI email = f.createURI("http://example/email");
        final BigdataLiteral paul = f.createLiteral("paul");
        final BigdataLiteral george = f.createLiteral("george");
        final BigdataValue[] values = new BigdataValue[] { name, email, paul,
                george };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        final QueryRoot expected = BOpUtility.deepCopy(queryRoot); 
        
        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        // Verify NO transform.
        assertEquals(expected, queryRoot);
        
    }

    /**
     * This test is be based on <code>Filter-nested - 2</code> (Filter on
     * variable ?v which is not in scope).
     * 
     * <pre>
     * PREFIX : <http://example/> 
     * 
     * SELECT ?v
     * { :x :p ?v . { FILTER(?v = 1) } }
     * </pre>
     * 
     * This is one of the DAWG "bottom-up" evaluation semantics tests.
     * <code>?v</code> is not bound in the FILTER because it is evaluated with
     * bottom up semantics and therefore the bindings from the parent group are
     * not visible.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_bottomUpOptimizer_filter_nested_2()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . { FILTER(?v = 1) } }";
        
        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI x = f.createURI("http://example/x");
        final BigdataURI p = f.createURI("http://example/p");
        final BigdataLiteral ONE = f.createLiteral("1", XSD.INTEGER);
        final BigdataValue[] values = new BigdataValue[] { x, p, ONE };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);
//        x.getIV().setValue(x);
//        p.getIV().setValue(p);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();

        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        /*
         * Create the expected AST for the WHERE clause.
         */
        final JoinGroupNode expectedWhereClause = new JoinGroupNode();
        {
            // :x :p ?v
            expectedWhereClause.addChild(new StatementPatternNode(//
                    new ConstantNode(new Constant(x.getIV())),// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new VarNode("v"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    ));

            final JoinGroupNode innerGroup = new JoinGroupNode();
            expectedWhereClause.addChild(innerGroup);
            
            final String anonvar = "-unbound-var-v-0";
            final FilterNode filterNode = new FilterNode(
                    new FunctionNode(//
                            FunctionRegistry.EQ,//
                            null,// scalarValues(Map)Collections.emptyMap(),//
                            new ValueExpressionNode[]{//
                                new VarNode(anonvar),//
                                new ConstantNode(ONE.getIV())//
                            }//
                            )//
                    );
            AST2BOpUtility.toVE(context.getLexiconNamespace(),
                    filterNode.getValueExpressionNode());
            innerGroup.addChild(filterNode);

        }

        diff(expectedWhereClause, queryRoot.getWhereClause());
//        assertEquals("modifiedClause", expectedWhereClause,
//                queryRoot.getWhereClause());

    }
    
    /**
     * Test when <code>?v</code> is bound in the input {@link IBindingSet}[]. In
     * this case we can not rewrite the filter.
     * 
     * @throws MalformedQueryException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_bottomUpOptimizer_filter_nested_2_withBindings()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . { FILTER(?v = 1) } }";
        
        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI x = f.createURI("http://example/x");
        final BigdataURI p = f.createURI("http://example/p");
        final BigdataValue[] values = new BigdataValue[] { x, p };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();

        final QueryRoot expected = BOpUtility.deepCopy(queryRoot);
        
        /*
         * A single solution with [v] bound. The value of the binding does not
         * matter. The presence of the binding is what is critical.  Since [v]
         * is bound in the source solutions we can not eliminate the filter.
         */
        final IBindingSet[] bindingSets = new IBindingSet[] {
                new ListBindingSet(
                new IVariable[] { Var.var("v") },
                new IConstant[] { new Constant(x.getIV()) })
        };
        
        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, bindingSets);

        /*
         * FIXME This is failing because the optimizer is not paying attention
         * to exogenous variable bindings.
         */
        diff(expected, queryRoot);

    }

    /**
     * Unit test for filter with a variable which is never bound (this has
     * nothing to do with the variable scoping).
     * 
     * <pre>
     * SELECT ?v
     * { :x :p ?v . FILTER(?w = 1) }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_bottomUpOptimizer_filter_unboundVar()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . FILTER(?w = 1) }";
        
        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI x = f.createURI("http://example/x");
        final BigdataURI p = f.createURI("http://example/p");
        final BigdataLiteral ONE = f.createLiteral("1", XSD.INTEGER);
        final BigdataValue[] values = new BigdataValue[] { x, p, ONE };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();

        queryRoot = (QueryRoot) new ASTBottomUpOptimizer().optimize(
                context, queryRoot, null/* bindingSets */);

        /*
         * Create the expected AST for the WHERE clause.
         */
        final JoinGroupNode expectedWhereClause = new JoinGroupNode();
        {
            // :x :q ?w
            expectedWhereClause.addChild(new StatementPatternNode(//
                    new ConstantNode(new Constant(x.getIV())),// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new VarNode("v"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    ));
            
            final String anonvar = "-unbound-var-w-0";
            final FilterNode filterNode = new FilterNode(
                    new FunctionNode(//
                            FunctionRegistry.EQ,//
                            null,// scalarValues (Map)Collections.emptyMap(),//
                            new ValueExpressionNode[]{//
                                new VarNode(anonvar),//
                                new ConstantNode(ONE.getIV())//
                            }//
                            )//
            );
            AST2BOpUtility.toVE(context.getLexiconNamespace(),
                    filterNode.getValueExpressionNode());
            expectedWhereClause.addChild(filterNode);

        }

        diff(expectedWhereClause, queryRoot.getWhereClause());

    }

}
