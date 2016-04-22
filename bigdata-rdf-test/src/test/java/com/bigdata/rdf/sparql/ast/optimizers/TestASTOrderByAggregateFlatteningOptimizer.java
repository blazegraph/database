/*
 * Copyright (C) 2016 SYSTAP, LLC DBA Blazegraph
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.rdf.aggregate.COUNT;
import com.bigdata.bop.rdf.aggregate.MAX;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.ASTDeferredIVResolution;
import java.util.Iterator;
import java.util.List;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import org.openrdf.query.MalformedQueryException;

/**
 * Test suite for {@link ASTOrderByAggregateFlatteningOptimizer}. All test
 * queries in the suite contain ORDER BY with one or more aggregates, and the
 * tested optimizer simplifies them by introducing aliases for the aggregates.
 *
 * @see ASTOrderByAggregateFlatteningOptimizer
 *
 * @author <a href="mailto:ariazanov@blazegraph.com">Alexandre Riazanov</a>
 */
public class TestASTOrderByAggregateFlatteningOptimizer
        extends AbstractASTEvaluationTestCase {

    public TestASTOrderByAggregateFlatteningOptimizer() {
        super();
    }

    public TestASTOrderByAggregateFlatteningOptimizer(final String name) {
        super(name);
    }

    public void test_orderByAggregateFlatteningOptimizer_simple_case_1()
            throws MalformedQueryException {
//        
//SELECT  ?o
//WHERE { ?s :p ?o } 
//GROUP BY ?o 
//ORDER BY (count(?s))

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT  ?o \n" + //
                "WHERE { ?s :p ?o } \n" + // 
                "GROUP BY ?o \n" + // 
                "ORDER BY (count(?s))" //
                ;



        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        ASTDeferredIVResolution.resolveQuery(store, astContainer);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot given = astContainer.getOriginalAST();

        final IASTOptimizer rewriter = new ASTOrderByAggregateFlatteningOptimizer();

        final QueryRoot actual = (QueryRoot) rewriter.optimize(context,
                new QueryNodeWithBindingSet(given, new IBindingSet[]{})).
                getQueryNode();



        // Check that the rewriter has produced something like this:
//QueryType: SELECT
//SELECT VarNode(o) ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(s))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(s)] AS VarNode(d999c13a-6aea-4c0d-ae37-cf99a1a04d46) )[excludeFromProjection]
//  JoinGroupNode {
//    StatementPatternNode(VarNode(s), ConstantNode(TermId(0U)[http://example/p]), VarNode(o)) [scope=DEFAULT_CONTEXTS]
//  }
//group by VarNode(o)
//ORDER BY com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(d999c13a-6aea-4c0d-ae37-cf99a1a04d46))[ ascending=true]


        final NamedSubqueriesNode namedSubqueries = actual.getNamedSubqueries();
        assertNull(namedSubqueries);

        assertSame(QueryType.SELECT, actual.getQueryType());



        // Check
        // SELECT VarNode(o) ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(s))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(s)] AS VarNode(d999c13a-6aea-4c0d-ae37-cf99a1a04d46) )[excludeFromProjection]

        final ProjectionNode projection = actual.getProjection();
        assertNotNull(projection);


        final IValueExpression[] assignments = projection.getValueExpressions();
        assertEquals(2, assignments.length);


        //       Check VarNode(o) (
        assertNotNull(assignments[0]);
        assertTrue(assignments[0] instanceof Bind);

        final IVariable<IV> var1 = ((Bind) assignments[0]).getVar();
        final IValueExpression<IV> expr1 = ((Bind) assignments[0]).getExpr();

        assertSame(var1, expr1);
        assertEquals("o", var1.getName());
        assertFalse(projection.excludeFromProjection(var1));


        //       Check ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(s))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(s)] AS VarNode(d999c13a-6aea-4c0d-ae37-cf99a1a04d46) )[excludeFromProjection]
        assertNotNull(assignments[1]);
        assertTrue(assignments[1] instanceof Bind);


        final IVariable<IV> var2 = ((Bind) assignments[1]).getVar();
        assertFalse("s".equals(var2.getName()));
        assertFalse("o".equals(var2.getName()));
        assertTrue(projection.excludeFromProjection(var2)); // [excludeFromProjection]


        final IValueExpression<IV> expr2 = ((Bind) assignments[1]).getExpr();
        assertTrue(expr2 instanceof COUNT);
        assertTrue(((COUNT) expr2).get(0) instanceof Var);
        assertEquals("s", ((Var) ((COUNT) expr2).get(0)).getName());



        // Check
        // group by VarNode(o)
        final GroupByNode groupBy = actual.getGroupBy();
        assertNotNull(groupBy);
        final IValueExpression[] groupByArgs = groupBy.getValueExpressions();
        assertNotNull(groupByArgs);
        assertEquals(1, groupByArgs.length);
        assertNotNull(groupByArgs[0]);
        assertTrue(groupByArgs[0] instanceof Bind);
        assertEquals("o", ((Bind) groupByArgs[0]).getVar().getName());



        // Check 
        // ORDER BY com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(d999c13a-6aea-4c0d-ae37-cf99a1a04d46))[ ascending=true]

        final OrderByNode orderBy = actual.getOrderBy();
        final Iterator<OrderByExpr> orderByArgs = orderBy.iterator();
        assertTrue(orderByArgs.hasNext());
        final OrderByExpr orderByArg = orderByArgs.next();
        assertFalse(orderByArgs.hasNext());
        assertTrue(orderByArg.getValueExpression() instanceof Var);
        assertEquals(var2.getName(),
                ((Var) orderByArg.getValueExpression()).getName()); // same as in SELECT
        assertTrue(orderByArg.isAscending()); // [ ascending=true]


        // Check 
        //  JoinGroupNode {
        //    StatementPatternNode(VarNode(s), ConstantNode(TermId(0U)[http://example/p]), VarNode(o)) [scope=DEFAULT_CONTEXTS]
        //  }

        final GraphPatternGroup whereClause = actual.getWhereClause();
        assertNotNull(whereClause);
        assertTrue(whereClause instanceof JoinGroupNode);
        final List<StatementPatternNode> patterns =
                ((JoinGroupNode) whereClause).getStatementPatterns();
        assertNotNull(patterns);
        assertEquals(1, patterns.size());
        final StatementPatternNode pattern = patterns.get(0);
        assertTrue(pattern.s() instanceof VarNode);
        assertEquals("s", ((VarNode) pattern.s()).getValueExpression().getName());

        assertTrue(pattern.p() instanceof ConstantNode);
        assertTrue(((ConstantNode) pattern.p()).getValueExpression() instanceof Constant);
        assertTrue(((Constant) ((ConstantNode) pattern.p()).getValueExpression()).get() instanceof TermId);
        assertEquals("http://example/p", ((TermId) ((Constant) ((ConstantNode) pattern.p()).getValueExpression()).get()).getValue().stringValue());

        assertTrue(pattern.o() instanceof VarNode);
        assertEquals("o", ((VarNode) pattern.o()).getValueExpression().getName());


    } // test_orderByAggregateFlatteningOptimizer_simple_case_1() 

    /**
     * Covers DESC and ASC, mixing aggregates and plain variables in ORDER BY,
     * multiple aggregates in ORDER BY, aggregates before plain variables in
     * ORDER BY, multiple projection variables.
     */
    public void test_orderByAggregateFlatteningOptimizer_high_coverage_case_1()
            throws MalformedQueryException {

//PREFIX ex: <http://example.org/>
//SELECT ?x ?y
//WHERE
//{
//  ?x ex:r ?y .
//  ?y ex:q ?z 
//}
//GROUP BY ?x ?y
//ORDER BY DESC(max(?z)) ?x (count(?z)) DESC(?y) 

        final String queryStr = "" + //
                "PREFIX ex: <http://example.org/>\n" + //
                "SELECT ?x ?y\n" + //
                "WHERE {\n" + //
                "  ?x ex:r ?y .\n" + //
                "  ?y ex:q ?z }\n" + //
                "GROUP BY ?x ?y\n" + //
                "ORDER BY DESC(max(?z)) ?x (count(?z)) DESC(?y)" //
                ;



        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        ASTDeferredIVResolution.resolveQuery(store, astContainer);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot given = astContainer.getOriginalAST();

        final IASTOptimizer rewriter = new ASTOrderByAggregateFlatteningOptimizer();

        final QueryRoot actual = (QueryRoot) rewriter.optimize(context,
                new QueryNodeWithBindingSet(given, new IBindingSet[]{})).
                getQueryNode();



        // Check that the rewriter has produced something like this:
//QueryType: SELECT
//SELECT VarNode(x) VarNode(y) ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(z))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#max, valueExpr=com.bigdata.bop.rdf.aggregate.MAX(z)] AS VarNode(e6501b59-cd5b-4fbf-9587-1797b79464b2) )[excludeFromProjection] ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(z))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(z)] AS VarNode(086758ec-366a-4b33-86da-e958744d6650) )[excludeFromProjection]
//  JoinGroupNode {
//    StatementPatternNode(VarNode(x), ConstantNode(TermId(0U)[http://example.org/r]), VarNode(y)) [scope=DEFAULT_CONTEXTS]
//    StatementPatternNode(VarNode(y), ConstantNode(TermId(0U)[http://example.org/q]), VarNode(z)) [scope=DEFAULT_CONTEXTS]
//  }
//group by VarNode(x) VarNode(y)
//ORDER BY com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(e6501b59-cd5b-4fbf-9587-1797b79464b2))[ ascending=false] com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(x))[ ascending=true] com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(086758ec-366a-4b33-86da-e958744d6650))[ ascending=true] com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(y))[ ascending=false]


        final NamedSubqueriesNode namedSubqueries = actual.getNamedSubqueries();
        assertNull(namedSubqueries);

        assertSame(QueryType.SELECT, actual.getQueryType());




        // Check
        // SELECT VarNode(x) VarNode(y) ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(z))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#max, valueExpr=com.bigdata.bop.rdf.aggregate.MAX(z)] AS VarNode(e6501b59-cd5b-4fbf-9587-1797b79464b2) )[excludeFromProjection] ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(z))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(z)] AS VarNode(086758ec-366a-4b33-86da-e958744d6650) )[excludeFromProjection]



        final ProjectionNode projection = actual.getProjection();
        assertNotNull(projection);


        final IValueExpression[] assignments = projection.getValueExpressions();
        assertEquals(4, assignments.length);


        //       Check VarNode(x) (
        assertNotNull(assignments[0]);
        assertTrue(assignments[0] instanceof Bind);

        final IVariable<IV> var1 = ((Bind) assignments[0]).getVar();
        final IValueExpression<IV> expr1 = ((Bind) assignments[0]).getExpr();

        assertSame(var1, expr1);
        assertEquals("x", var1.getName());
        assertFalse(projection.excludeFromProjection(var1));



        //       Check VarNode(y) (
        assertNotNull(assignments[1]);
        assertTrue(assignments[1] instanceof Bind);

        final IVariable<IV> var2 = ((Bind) assignments[1]).getVar();
        final IValueExpression<IV> expr2 = ((Bind) assignments[1]).getExpr();

        assertSame(var2, expr2);
        assertEquals("y", var2.getName());
        assertFalse(projection.excludeFromProjection(var2));




        //       Check  ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(z))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#max, valueExpr=com.bigdata.bop.rdf.aggregate.MAX(z)] AS VarNode(e6501b59-cd5b-4fbf-9587-1797b79464b2) )[excludeFromProjection]
        assertNotNull(assignments[2]);
        assertTrue(assignments[2] instanceof Bind);


        final IVariable<IV> var3 = ((Bind) assignments[2]).getVar();
        assertFalse("x".equals(var3.getName()));
        assertFalse("y".equals(var3.getName()));
        assertFalse("z".equals(var3.getName()));
        assertTrue(projection.excludeFromProjection(var3)); // [excludeFromProjection]


        final IValueExpression<IV> expr3 = ((Bind) assignments[2]).getExpr();
        assertTrue(expr3 instanceof MAX);
        assertTrue(((MAX) expr3).get(0) instanceof Var);
        assertEquals("z", ((Var) ((MAX) expr3).get(0)).getName());




        //       Check ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(z))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(z)] AS VarNode(086758ec-366a-4b33-86da-e958744d6650) )[excludeFromProjection]
        assertNotNull(assignments[3]);
        assertTrue(assignments[3] instanceof Bind);


        final IVariable<IV> var4 = ((Bind) assignments[3]).getVar();
        assertFalse("x".equals(var4.getName()));
        assertFalse("y".equals(var4.getName()));
        assertFalse("z".equals(var4.getName()));
        assertFalse(var3.getName().equals(var4.getName()));
        assertTrue(projection.excludeFromProjection(var4)); // [excludeFromProjection]


        final IValueExpression<IV> expr4 = ((Bind) assignments[3]).getExpr();
        assertTrue(expr4 instanceof COUNT);
        assertTrue(((COUNT) expr4).get(0) instanceof Var);
        assertEquals("z", ((Var) ((COUNT) expr4).get(0)).getName());


        // Check 
        // group by VarNode(x) VarNode(y)

        final GroupByNode groupBy = actual.getGroupBy();
        assertNotNull(groupBy);
        final IValueExpression[] groupByArgs = groupBy.getValueExpressions();
        assertNotNull(groupByArgs);
        assertEquals(2, groupByArgs.length);

        assertNotNull(groupByArgs[0]);
        assertTrue(groupByArgs[0] instanceof Bind);
        assertEquals("x", ((Bind) groupByArgs[0]).getVar().getName());

        assertNotNull(groupByArgs[1]);
        assertTrue(groupByArgs[1] instanceof Bind);
        assertEquals("y", ((Bind) groupByArgs[1]).getVar().getName());



        // Check 
        // ORDER BY com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(e6501b59-cd5b-4fbf-9587-1797b79464b2))[ ascending=false] com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(x))[ ascending=true] com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(086758ec-366a-4b33-86da-e958744d6650))[ ascending=true] com.bigdata.rdf.sparql.ast.OrderByExpr(VarNode(y))[ ascending=false]


        final OrderByNode orderBy = actual.getOrderBy();
        final Iterator<OrderByExpr> orderByArgs = orderBy.iterator();


        assertTrue(orderByArgs.hasNext());
        final OrderByExpr orderByArg1 = orderByArgs.next();
        assertTrue(orderByArg1.getValueExpression() instanceof Var);
        assertEquals(var3.getName(),
                ((Var) orderByArg1.getValueExpression()).getName());
        assertFalse(orderByArg1.isAscending()); // [ ascending=false]


        assertTrue(orderByArgs.hasNext());
        final OrderByExpr orderByArg2 = orderByArgs.next();
        assertTrue(orderByArg2.getValueExpression() instanceof Var);
        assertEquals("x",
                ((Var) orderByArg2.getValueExpression()).getName());
        assertTrue(orderByArg2.isAscending()); // [ ascending=true]



        assertTrue(orderByArgs.hasNext());
        final OrderByExpr orderByArg3 = orderByArgs.next();
        assertTrue(orderByArg3.getValueExpression() instanceof Var);
        assertEquals(var4.getName(),
                ((Var) orderByArg3.getValueExpression()).getName());
        assertTrue(orderByArg3.isAscending()); // [ ascending=true]


        assertTrue(orderByArgs.hasNext());
        final OrderByExpr orderByArg4 = orderByArgs.next();
        assertTrue(orderByArg4.getValueExpression() instanceof Var);
        assertEquals("y",
                ((Var) orderByArg4.getValueExpression()).getName());
        assertFalse(orderByArg4.isAscending()); // [ ascending=false] 

        assertFalse(orderByArgs.hasNext());





        // Check 
        //  JoinGroupNode {
        //    StatementPatternNode(VarNode(x), ConstantNode(TermId(0U)[http://example.org/r]), VarNode(y)) [scope=DEFAULT_CONTEXTS]
        //    StatementPatternNode(VarNode(y), ConstantNode(TermId(0U)[http://example.org/q]), VarNode(z)) [scope=DEFAULT_CONTEXTS]
        //  }

        final GraphPatternGroup whereClause = actual.getWhereClause();
        assertNotNull(whereClause);
        assertTrue(whereClause instanceof JoinGroupNode);
        final List<StatementPatternNode> patterns =
                ((JoinGroupNode) whereClause).getStatementPatterns();
        assertNotNull(patterns);
        assertEquals(2, patterns.size());
        
        
        //      Check 
        //        StatementPatternNode(VarNode(x), ConstantNode(TermId(0U)[http://example.org/r]), VarNode(y)) [scope=DEFAULT_CONTEXTS]
        final StatementPatternNode pattern1 = patterns.get(0);
        assertTrue(pattern1.s() instanceof VarNode);
        assertEquals("x", ((VarNode) pattern1.s()).getValueExpression().getName());

        assertTrue(pattern1.p() instanceof ConstantNode);
        assertTrue(((ConstantNode) pattern1.p()).getValueExpression() instanceof Constant);
        assertTrue(((Constant) ((ConstantNode) pattern1.p()).getValueExpression()).get() instanceof TermId);
        assertEquals("http://example.org/r", ((TermId) ((Constant) ((ConstantNode) pattern1.p()).getValueExpression()).get()).getValue().stringValue());

        assertTrue(pattern1.o() instanceof VarNode);
        assertEquals("y", ((VarNode) pattern1.o()).getValueExpression().getName());


        
        
        //      Check 
        //        StatementPatternNode(VarNode(y), ConstantNode(TermId(0U)[http://example.org/q]), VarNode(z)) [scope=DEFAULT_CONTEXTS]
                
        final StatementPatternNode pattern2 = patterns.get(1);
        assertTrue(pattern2.s() instanceof VarNode);
        assertEquals("y", ((VarNode) pattern2.s()).getValueExpression().getName());

        assertTrue(pattern2.p() instanceof ConstantNode);
        assertTrue(((ConstantNode) pattern2.p()).getValueExpression() instanceof Constant);
        assertTrue(((Constant) ((ConstantNode) pattern2.p()).getValueExpression()).get() instanceof TermId);
        assertEquals("http://example.org/q", ((TermId) ((Constant) ((ConstantNode) pattern2.p()).getValueExpression()).get()).getValue().stringValue());

        assertTrue(pattern2.o() instanceof VarNode);
        assertEquals("z", ((VarNode) pattern2.o()).getValueExpression().getName());


    } // test_orderByAggregateFlatteningOptimizer_high_coverage_case_1()
} // class TestASTOrderByAggregateFlatteningOptimizer
