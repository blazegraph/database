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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

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
import com.bigdata.rdf.sparql.ast.ASTOptimizerResult;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
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
    * Test inlining from BIND clause of static bindings from BIND clause,
    * including the removal of the clause and putting the values into the
    * exogeneous mapping set.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?s where { BIND(CONST AS ?p) . ?s ?p ?s }
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?s where { ?s CONST ?s }
    * </pre>
    * 
    * where CONST is the binding for <code>?p</code> in the input solution
    * and the exogeneous mapping { ?p -> CONST } is added. 
    */
   public void testInlineFromBind() {
      
      final IV cTest = makeIV(store.getValueFactory().createURI("http://www.test.com"));

      final IBindingSet[] bsetsGiven = 
         new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          whereClause.addChild(
                new AssignmentNode(new VarNode("p"), new ConstantNode(cTest)));
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
                          cTest)),
                  new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
          expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
         new ASTStaticBindingsOptimizer();
      
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTOptimizerResult res = 
         rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      // assert that the bindings set has been modified as expected
      IBindingSet[] resBs = res.getOptimizedBindingSet();
      assertTrue(resBs.length==1);
      assertTrue(resBs[0].size()==1);
      assertTrue(resBs[0].get(Var.var("p")).equals(new Constant<IV>(cTest)));
      
      assertSameAST(expected, res.getOptimizedQueryNode());
   }
   
   /**
    * Test inlining from BIND clause of static bindings from BIND clause,
    * including the removal of the clause and putting the values into the
    * exogeneous mapping set in a nested setting.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?s where { { BIND(CONST AS ?p) . ?s ?p ?s } }
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?s where { { ?s CONST ?s } }
    * </pre>
    * 
    * where CONST is the binding for <code>?p</code> in the input solution
    * and the exogeneous mapping { ?p -> CONST } is added. 
    */
   public void testInlineFromBindNested1() {
      
      final IV cTest = makeIV(store.getValueFactory().createURI("http://www.test.com"));

      final IBindingSet[] bsetsGiven = 
         new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          final JoinGroupNode jgn = new JoinGroupNode();
          whereClause.addChild(jgn);
          jgn.addChild(
                new AssignmentNode(new VarNode("p"), new ConstantNode(cTest)));
          jgn.addChild(new StatementPatternNode(new VarNode("s"),
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
          final JoinGroupNode jgn = new JoinGroupNode();
          whereClause.addChild(jgn);

          jgn.addChild(new StatementPatternNode(new VarNode("s"),
                  new ConstantNode(new Constant((IVariable) Var.var("p"),
                          cTest)),
                  new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
          expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
         new ASTStaticBindingsOptimizer();
      
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTOptimizerResult res = 
         rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      // assert that the bindings set has been modified as expected
      IBindingSet[] resBs = res.getOptimizedBindingSet();
      assertTrue(resBs.length==1);
      assertTrue(resBs[0].size()==1);
      assertTrue(resBs[0].get(Var.var("p")).equals(new Constant<IV>(cTest)));
      
      assertSameAST(expected, res.getOptimizedQueryNode());
   }
   
   /**
    * Test inlining from BIND clause of static bindings from BIND clause,
    * including the removal of the clause and putting the values into the
    * exogeneous mapping set in a nested setting.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?s where { { BIND(CONST AS ?p) } ?s ?p ?s }
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?s where { { } ?s CONST ?s }
    * </pre>
    * 
    * where CONST is the binding for <code>?p</code> in the input solution
    * and the exogeneous mapping { ?p -> CONST } is added. 
    */
   public void testInlineFromBindNested2() {
      
      final IV cTest = makeIV(store.getValueFactory().createURI("http://www.test.com"));

      final IBindingSet[] bsetsGiven = 
         new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          final JoinGroupNode jgn = new JoinGroupNode();
          whereClause.addChild(jgn);
          jgn.addChild(
                new AssignmentNode(new VarNode("p"), new ConstantNode(cTest)));
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
          final JoinGroupNode jgn = new JoinGroupNode();
          whereClause.addChild(jgn);

          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new ConstantNode(new Constant((IVariable) Var.var("p"),
                          cTest)),
                  new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
          expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
         new ASTStaticBindingsOptimizer();
      
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTOptimizerResult res = 
         rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      // assert that the bindings set has been modified as expected
      IBindingSet[] resBs = res.getOptimizedBindingSet();
      assertTrue(resBs.length==1);
      assertTrue(resBs[0].size()==1);
      assertTrue(resBs[0].get(Var.var("p")).equals(new Constant<IV>(cTest)));
      
      assertSameAST(expected, res.getOptimizedQueryNode());
   }
   
   /**
    * Test inlining from BIND clause of static bindings from top-level VALUES
    * clause, including the removal of the clause and putting the values into 
    * the exogeneous mapping set.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?p where { ?s ?p ?s } VALUES ?p { CONST }
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?p where { ?s CONST ?s }
    * </pre>
    * 
    * where CONST is the binding for <code>?p</code> in the input solution
    * and the exogeneous mapping { ?p -> CONST } is added. 
    */
   public void testInlineFromTopLevelValues() {
      
      final IV cTest = makeIV(store.getValueFactory().createURI("http://www.test.com"));

      final IBindingSet[] bsetsGiven = 
         new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          final IVariable<?> var = Var.var("p");
          final LinkedHashSet<IVariable<?>> declaredVars =
             new LinkedHashSet<IVariable<?>>();
          declaredVars.add(var);
          
          final List<IBindingSet> bindingSets = new ArrayList<IBindingSet>();
          IBindingSet bs = new ListBindingSet();
          bs.set(var, new Constant<IV>(cTest));
          bindingSets.add(bs);
          
          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));

          final BindingsClause bc = new BindingsClause(declaredVars, bindingSets);
          given.setBindingsClause(bc);

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
                          cTest)),
                  new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
          expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
         new ASTStaticBindingsOptimizer();
      
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTOptimizerResult res = 
         rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      // assert that the bindings set has been modified as expected
      IBindingSet[] resBs = res.getOptimizedBindingSet();
      assertTrue(resBs.length==1);
      assertTrue(resBs[0].size()==1);
      assertTrue(resBs[0].get(Var.var("p")).equals(new Constant<IV>(cTest)));
      
      assertSameAST(expected, res.getOptimizedQueryNode());    
   }
   
   /**
    * Test inlining from BIND clause of static bindings from top-level VALUES
    * clause, including the removal of the clause and putting the values into
    * the exogeneous mapping set.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?p where { VALUES ?p { CONST } . ?s ?p ?s }
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?p where { ?s CONST ?s }
    * </pre>
    * 
    * where CONST is the binding for <code>?p</code> in the input solution
    * and the exogeneous mapping { ?p -> CONST } is added. 
    */
   public void testInlineFromValues() {
      final IV cTest = makeIV(store.getValueFactory().createURI("http://www.test.com"));

      final IBindingSet[] bsetsGiven = 
         new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          final IVariable<?> var = Var.var("p");
          final LinkedHashSet<IVariable<?>> declaredVars =
             new LinkedHashSet<IVariable<?>>();
          declaredVars.add(var);
          
          final List<IBindingSet> bindingSets = new ArrayList<IBindingSet>();
          IBindingSet bs = new ListBindingSet();
          bs.set(var, new Constant<IV>(cTest));
          bindingSets.add(bs);
          
          final BindingsClause bc = new BindingsClause(declaredVars, bindingSets);
          whereClause.addChild(bc);
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
                          cTest)),
                  new VarNode("o"), null/* c */, Scope.DEFAULT_CONTEXTS));
          expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
         new ASTStaticBindingsOptimizer();
      
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTOptimizerResult res = 
         rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      // assert that the bindings set has been modified as expected
      IBindingSet[] resBs = res.getOptimizedBindingSet();
      assertTrue(resBs.length==1);
      assertTrue(resBs[0].size()==1);
      assertTrue(resBs[0].get(Var.var("p")).equals(new Constant<IV>(cTest)));
      
      assertSameAST(expected, res.getOptimizedQueryNode());
   }
   

   /**
    * Test inlining from BIND clause of static bindings from top-level VALUES
    * clause, including the removal of the clause and putting the values into
    * the exogeneous mapping set.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?p where { BIND(1 AS ?p) }
    * </pre>
    * 
    * and a binding for <code>?s</code> in an input solution, verify that the
    * AST is rewritten as:
    * 
    * <pre>
    * SELECT ?p where { }
    * </pre>
    * 
    * and the exogeneous mapping is /extended/ by { ?p -> CONST }. 
    */
   public void testMergeWithSimpleExogeneousMapping() {
      
      final IV cFalse = makeIV(store.getValueFactory().createLiteral(false));
      final IV cTrue = makeIV(store.getValueFactory().createLiteral(true));

      final IBindingSet[] bsetsGiven = new IBindingSet[] { //
            new ListBindingSet(//
                    new IVariable[] { Var.var("s") },//
                    new IConstant[] { new Constant<IV>(cFalse)}) //
      };      

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("p"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.addChild(
                new AssignmentNode(new VarNode("p"), new ConstantNode(cTrue)));
          
          given.setWhereClause(whereClause);

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         
         projection.addProjectionVar(new VarNode("p"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
         new ASTStaticBindingsOptimizer();
      
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTOptimizerResult res = 
         rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      // assert that the bindings set has been modified as expected
      IBindingSet[] resBs = res.getOptimizedBindingSet();
      assertTrue(resBs.length==1);
      assertTrue(resBs[0].size()==2);
      assertTrue(resBs[0].get(Var.var("s")).equals(new Constant<IV>(cFalse)));
      assertTrue(resBs[0].get(Var.var("p")).equals(new Constant<IV>(cTrue)));
      
      assertSameAST(expected, res.getOptimizedQueryNode());      
   }

   public void testMergeWithComplexExogeneousMapping() {
      
   }
   
   /**
    * Assert that even for complex exogeneous mappings, variables that map
    * to the same value in all cases are inlined properly.
    * 
    * Given
    * 
    * <pre>
    * SELECT ?p where { 
    *   BIND("b" AS ?b) . 
    *   VALUES (?c ?d) { ("c1" "d1") ("c2" "d2") } 
    * } VALUES ?e { "e" }
    * </pre>
    * 
    * and an exogeneous binding set 
    * { { ?a -> "a1", ?c -> "c1" }, 
    *   { ?a -> "a2", ?c -> "c2" }, 
    *   { ?a -> "a3", ?c -> "c1" } } 
    *   
    * in an input solution, verify that the rewritten query is
    * 
    * <pre>
    * SELECT ?p where { }
    * </pre>
    * 
    * and that the resulting generated exogeneous binding set is:
    * 
    * { { ?a -> "a1", ?b -> "b", ?c -> "c1", ?d -> "d1", ?e -> "e" }, 
    *   { ?a -> "a2", ?b -> "b", ?c -> "c2", ?d -> "d2", ?e -> "e" } }
    * 
    */
   public void testInliningForComplexExogeneousMapping() {
      
      final IV a1Lit = makeIV(store.getValueFactory().createLiteral("a1"));
      final IV a2Lit = makeIV(store.getValueFactory().createLiteral("a2"));
      final IV a3Lit = makeIV(store.getValueFactory().createLiteral("a3"));
      final IV bLit = makeIV(store.getValueFactory().createLiteral("b"));
      final IV c1Lit = makeIV(store.getValueFactory().createLiteral("c1"));
      final IV c2Lit = makeIV(store.getValueFactory().createLiteral("c2"));
      final IV c3Lit = makeIV(store.getValueFactory().createLiteral("c3"));
      final IV d1Lit = makeIV(store.getValueFactory().createLiteral("d1"));
      final IV d2Lit = makeIV(store.getValueFactory().createLiteral("d2"));
      final IV eLit = makeIV(store.getValueFactory().createLiteral("e"));

      /**
       * Construct in mapping set:
       * { { ?a -> "a1", ?c -> "c1" }, 
       *   { ?a -> "a2", ?c -> "c2" }, 
       *   { ?a -> "a3", ?c -> "c3" } } 
       */
      final IBindingSet exogeneousIn1 = new ListBindingSet();
      exogeneousIn1.set(Var.var("a"), new Constant<IV>(a1Lit));
      exogeneousIn1.set(Var.var("c"), new Constant<IV>(c1Lit));
      final IBindingSet exogeneousIn2 = new ListBindingSet();
      exogeneousIn2.set(Var.var("a"), new Constant<IV>(a2Lit));
      exogeneousIn2.set(Var.var("c"), new Constant<IV>(c2Lit));
      final IBindingSet exogeneousIn3 = new ListBindingSet();
      exogeneousIn3.set(Var.var("a"), new Constant<IV>(a3Lit));
      exogeneousIn3.set(Var.var("c"), new Constant<IV>(c3Lit));
      final IBindingSet[] bsetsGiven =
         new IBindingSet[] { exogeneousIn1, exogeneousIn2, exogeneousIn3 };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          
          projection.addProjectionVar(new VarNode("p"));
          
          // BIND("b" AS ?b)
          final AssignmentNode bAss = 
             new AssignmentNode(new VarNode("b"), new ConstantNode(bLit));
          
          // VALUES ?e { "e" }
          final LinkedHashSet<IVariable<?>> declaredVarsE = 
             new LinkedHashSet<IVariable<?>>();
          declaredVarsE.add(Var.var("e"));
          
          final List<IBindingSet> bindingSetsE = new ArrayList<IBindingSet>();
          IBindingSet bsE = new ListBindingSet();
          bsE.set(Var.var("e"), new Constant<IV>(eLit));
          bindingSetsE.add(bsE);          
          final BindingsClause eBindings = new BindingsClause(declaredVarsE, bindingSetsE);

          // VALUES (?c ?d) { ("c1" "d1") ("c2" "d2") } 
          final LinkedHashSet<IVariable<?>> declaredVarsBcd = 
             new LinkedHashSet<IVariable<?>>();
          declaredVarsE.add(Var.var("b"));
          declaredVarsE.add(Var.var("c"));
          declaredVarsE.add(Var.var("d"));
          
          final List<IBindingSet> bindingSetsCd = new ArrayList<IBindingSet>();
          IBindingSet bsCd1 = new ListBindingSet();
          bsCd1.set(Var.var("c"), new Constant<IV>(c1Lit));
          bsCd1.set(Var.var("d"), new Constant<IV>(d1Lit));
          bindingSetsCd.add(bsCd1);
          
          IBindingSet bsCd2 = new ListBindingSet();
          bsCd2.set(Var.var("c"), new Constant<IV>(c2Lit));
          bsCd2.set(Var.var("d"), new Constant<IV>(d2Lit));
          bindingSetsCd.add(bsCd2);
          
          final BindingsClause bcdBindings =
             new BindingsClause(declaredVarsBcd, bindingSetsCd);
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.addChild(bAss);
          whereClause.addChild(bcdBindings);
          given.setBindingsClause(eBindings);
          
          given.setWhereClause(whereClause);

      }      
      
      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         
         projection.addProjectionVar(new VarNode("p"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         expected.setWhereClause(whereClause);

      }
      
      final ASTStaticBindingsOptimizer rewriter = 
            new ASTStaticBindingsOptimizer();
         
         
         final AST2BOpContext context = 
               new AST2BOpContext(new ASTContainer(given), store);
         
         final ASTOptimizerResult res = 
            rewriter.optimize(context, given/* queryNode */, bsetsGiven);

      /*
       * Verify that the resulting binding set is:
       * { { ?a -> "a1", ?b -> "b", ?c -> "c1", ?d -> "d1", ?e -> "e" }, 
       *   { ?a -> "a2", ?b -> "b", ?c -> "c2", ?d -> "d2", ?e -> "e" } }
       * 
       */
         
         // assert that the bindings set has been modified as expected
         IBindingSet[] resBs = res.getOptimizedBindingSet();
         
         final IBindingSet bs1 = resBs[0];
         assertTrue(bs1.size()==5);
         assertTrue(bs1.get(Var.var("a")).equals(new Constant<IV>(a1Lit)));
         assertTrue(bs1.get(Var.var("b")).equals(new Constant<IV>(bLit)));
         assertTrue(bs1.get(Var.var("c")).equals(new Constant<IV>(c1Lit)));
         assertTrue(bs1.get(Var.var("d")).equals(new Constant<IV>(d1Lit)));
         assertTrue(bs1.get(Var.var("e")).equals(new Constant<IV>(eLit)));
         
         final IBindingSet bs2 = resBs[1];
         assertTrue(bs2.size()==5);
         assertTrue(bs2.get(Var.var("a")).equals(new Constant<IV>(a2Lit)));
         assertTrue(bs2.get(Var.var("b")).equals(new Constant<IV>(bLit)));
         assertTrue(bs2.get(Var.var("c")).equals(new Constant<IV>(c2Lit)));
         assertTrue(bs2.get(Var.var("d")).equals(new Constant<IV>(d2Lit)));
         assertTrue(bs2.get(Var.var("e")).equals(new Constant<IV>(eLit)));
        
         assertSameAST(expected, res.getOptimizedQueryNode());    
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
   
   // TOOD: test for FILTERs (constraints only)
   

}
