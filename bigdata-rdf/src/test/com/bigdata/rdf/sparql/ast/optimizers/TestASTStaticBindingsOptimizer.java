/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on June 2, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for 
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestASTStaticBindingsOptimizer extends AbstractASTEvaluationTestCase {

   public TestASTStaticBindingsOptimizer() {
   }

   public TestASTStaticBindingsOptimizer(String name) {
       super(name);
   }
   
   /**
    * Given
    * 
    * <pre>
    * SELECT ?s where {?s ?p ?o}
    * </pre>
    * 
    * and a binding for <code>?p</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?s where {?s CONST ?o}
    * </pre>
    * 
    * where CONST is the binding for <code>?p</code> in the input solution.
    * 
     * Carried over from {@link TestASTBindingAssigner#test_astBindingAssigner()}.
    */
   public void testInlineFromExogeneousBindings() {

       /*
        * Note: DO NOT share structures in this test!!!!
        */

       final IV mockIV = TermId.mockIV(VTE.URI);
       
       final IBindingSet[] bsets = new IBindingSet[] { //
       new ListBindingSet(//
               new IVariable[] { Var.var("p") },//
               new IConstant[] { new Constant<IV>(mockIV) }) //
       };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           given.setProjection(projection);
           
           projection.addProjectionVar(new VarNode("s"));
           
           final JoinGroupNode whereClause = new JoinGroupNode();
           given.setWhereClause(whereClause);

           whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                   new VarNode("p"), new VarNode("o"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           expected.setProjection(projection);

           projection.addProjectionVar(new VarNode("s"));

           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                   new ConstantNode(new Constant((IVariable) Var.var("p"),
                           mockIV)),
                   new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
           expected.setWhereClause(whereClause);

       }

       final ASTStaticBindingsOptimizer rewriter = 
          new ASTStaticBindingsOptimizer();
       
       
       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, given/* queryNode */, bsets).
             getOptimizedQueryNode();

       assertSameAST(expected, actual);

   }

   /**
    * Given
    * 
    * <pre>
    * SELECT ?p where {?s ?p ?s}
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?p where {CONST ?p CONST}
    * </pre>
    * 
    * where CONST is the binding for <code>?s</code> in the input solution.
    * <p>
    * Note: For this unit test, a variable is replaced in more than one
    * location in the AST.
    * 
    * Carried over from {@link TestASTBindingAssigner#test_astBindingAssigner2()}.
    * 
    */
   public void testMultiInlineFromExogeneousBindings() {

       /*
        * Note: DO NOT SHARE STRUCTURES IN THIS TEST.
        */
      
       final IV c12 = makeIV(store.getValueFactory().createLiteral(12));

       final IBindingSet[] bsets = new IBindingSet[] { //
       new ListBindingSet(//
               new IVariable[] { Var.var("s") },//
               new IConstant[] { new Constant<IV>(c12)}) //
       };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           projection.addProjectionVar(new VarNode("p"));
           given.setProjection(projection);

           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                   new VarNode("p"), new VarNode("s"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));
           given.setWhereClause(whereClause);

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           projection.addProjectionVar(new VarNode("p"));
           expected.setProjection(projection);

           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.addChild(new StatementPatternNode(//
                   new ConstantNode(
                           new Constant((IVariable) Var.var("s"), c12)), //
                   new VarNode("p"),//
                   new ConstantNode(
                           new Constant((IVariable) Var.var("s"), c12)), //
                   null/* c */, Scope.DEFAULT_CONTEXTS));
           expected.setWhereClause(whereClause);

       }

       final IASTOptimizer rewriter = new ASTStaticBindingsOptimizer();
       
       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, given/* queryNode */, bsets)
            .getOptimizedQueryNode();

       assertSameAST(expected, actual);
       
   }

   /**
    * TOOD: test inlining from VALUES clause
    */
   public void testInlineFromValues() {
      
   }
   
   /**
    * Given
    * 
    * <pre>
    * SELECT ?p ?o where {?s ?p ?o. FILTER( sameTerm(?o,12) ) }
    * </pre>
    * 
    * Verify that the AST is rewritten as:
    * 
    * <pre>
    * SELECT ?p CONST where {?s ?p CONST . FILTER( sameTerm(CONST,12) ) }
    * </pre>
    * 
    * where CONST is the binding for <code>?o</code> given by the FILTER.
    * <p>
    * Note: For this unit test, a variable is replaced in more than one
    * location in the AST.
    * 
    * Carried over from {@link TestASTBindingAssigner#test_astBindingAssigner_filter_eq_ConstURI()}
    */
   public void testSimpleSameTermFilter() {

      /*
       * Note: DO NOT SHARE STRUCTURES IN THIS TEST.
       */
      
      final IV c12 = makeIV(store.getValueFactory().createLiteral(12));

      final IBindingSet[] bsets = new IBindingSet[] { //
      new ListBindingSet()//
      };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          projection.addProjectionVar(new VarNode("p"));
          projection.addProjectionVar(new VarNode("o"));
          given.setProjection(projection);

          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(new FilterNode(FunctionNode.sameTerm(
                  new VarNode("o"), new ConstantNode(c12))));

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          projection.addProjectionVar(new VarNode("p"));
          projection.addProjectionVar(new VarNode("o"));

          expected.setProjection(projection);

          final JoinGroupNode whereClause = new JoinGroupNode();
          expected.setWhereClause(whereClause);

          whereClause.addChild(new StatementPatternNode(//
                  new VarNode("s"), //
                  new VarNode("p"),//
                  new ConstantNode(
                          new Constant((IVariable) Var.var("o"), c12)), //
                  null/* c */, Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(new FilterNode(FunctionNode.sameTerm(
                  new ConstantNode(
                          new Constant((IVariable) Var.var("o"), c12)),
                  new ConstantNode(c12))));

      }

      final IASTOptimizer rewriter = new ASTStaticBindingsOptimizer();
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);

      final IQueryNode actual = rewriter.optimize(
         context, given/* queryNode */, bsets).getOptimizedQueryNode();

      assertSameAST(expected, actual);
   }
   
   /**
    * Given
    * 
    * <pre>
    * SELECT ?p ?o where {?s ?p ?o. FILTER( ?o=CONST_URI) ) }
    * </pre>
    * 
    * Verify that the AST is rewritten as:
    * 
    * <pre>
    * SELECT ?p ?o where {?s ?p CONST_URI . FILTER( ?o=CONST_URI) ) }
    * </pre>
    * 
    * where CONST_URI is a URI binding for <code>?o</code> given by the FILTER.
    * <p>
    * Note: For this unit test, a variable is replaced in more than one
    * location in the AST.
    * 
    * Carried over from {@link TestASTBindingAssigner#test_astBindingAssigner_filter_eq_ConstURI()}.
    */
   public void testInlineSimpleFilterEqURI() {
      
      final IV foo = makeIV(store.getValueFactory().createURI(":foo"));

      final IBindingSet[] bsets = new IBindingSet[] { //
      new ListBindingSet()//
      };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          projection.addProjectionVar(new VarNode("p"));
          projection.addProjectionVar(new VarNode("o"));
          given.setProjection(projection);

          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(new FilterNode(FunctionNode.EQ(
                  new VarNode("o"), new ConstantNode(foo))));

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          projection.addProjectionVar(new VarNode("p"));
          projection.addProjectionVar(new VarNode("o"));
          expected.setProjection(projection);

          final JoinGroupNode whereClause = new JoinGroupNode();
          expected.setWhereClause(whereClause);

          whereClause.addChild(new StatementPatternNode(//
                  new VarNode("s"), //
                  new VarNode("p"),//
                  new ConstantNode(
                          new Constant((IVariable) Var.var("o"), foo)), //
                  null/* c */, Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(new FilterNode(FunctionNode.EQ(
                  new ConstantNode(
                          new Constant((IVariable) Var.var("o"), foo)),
                  new ConstantNode(foo))));

      }

      final IASTOptimizer rewriter = new ASTStaticBindingsOptimizer();
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = rewriter.optimize(
         context, given/* queryNode */, bsets)
           .getOptimizedQueryNode();

      assertSameAST(expected, actual);
//      System.err.println(actual.toString());
   }
   
   /**
    * Given
    * 
    * <pre>
    * SELECT ?p ?o where {?s ?p ?o. FILTER( ?o=CONST_LIT) ) }
    * </pre>
    * 
    * Verify that the AST is not rewritten.
    * 
    * TODO Variant where the roles of the variable and the constant within the
    * FILTER are reversed.
    * 
    * Carried over from {@link TestASTBindingAssigner#test_astBindingAssigner_filter_eq_ConstLit()
    */
   public void testNotInlineSimpleFilterEqLiteral() {

       /*
        * Note: DO NOT SHARE STRUCTURES IN THIS TEST.
        */
       
       final IV foo = makeIV(store.getValueFactory().createLiteral("foo"));

       final IBindingSet[] bsets = new IBindingSet[] { //
       new ListBindingSet()//
       };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           projection.addProjectionVar(new VarNode("p"));
           projection.addProjectionVar(new VarNode("o"));
           given.setProjection(projection);

           final JoinGroupNode whereClause = new JoinGroupNode();
           given.setWhereClause(whereClause);
           
           whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                   new VarNode("p"), new VarNode("o"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));

           whereClause.addChild(new FilterNode(FunctionNode.EQ(
                   new VarNode("o"), new ConstantNode(foo))));

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           projection.addProjectionVar(new VarNode("p"));
           projection.addProjectionVar(new VarNode("o"));
           expected.setProjection(projection);

           final JoinGroupNode whereClause = new JoinGroupNode();
           expected.setWhereClause(whereClause);

           whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                   new VarNode("p"), new VarNode("o"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));

           whereClause.addChild(new FilterNode(FunctionNode.EQ(
                   new VarNode("o"), new ConstantNode(foo))));

       }

       final IASTOptimizer rewriter = new ASTStaticBindingsOptimizer();
       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);

       final IQueryNode actual = rewriter.optimize(
          context, given/* queryNode */, bsets)
             .getOptimizedQueryNode();

       assertSameAST(expected, actual);
   }
}
