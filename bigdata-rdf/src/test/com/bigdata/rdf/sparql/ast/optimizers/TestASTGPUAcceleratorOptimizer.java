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
 * Created on July 15, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.join.GPUJoinGroupOp;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for the {@link ASTGPUAccelerationOptimizer} class.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestASTGPUAcceleratorOptimizer extends AbstractASTEvaluationTestCase {

   public TestASTGPUAcceleratorOptimizer() {
   }

   public TestASTGPUAcceleratorOptimizer(String name) {
       super(name);
   }
   
   
   /**
    * Simple test case without attached filters.
    */
   public void testOptimizerSimpleWithoutAttachedFilters() {

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {
          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s1"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          whereClause.addChild(
             new StatementPatternNode(new VarNode("s1"),
                new VarNode("p1"), new VarNode("o1"), null/* c */,
                Scope.DEFAULT_CONTEXTS));
          whereClause.addChild(
                new StatementPatternNode(new VarNode("s2"),
                   new VarNode("p2"), new VarNode("o2"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s1"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         expected.setWhereClause(whereClause);

         final JoinGroupNode liftedGroup1 = new JoinGroupNode();
         liftedGroup1.setProperty(GPUJoinGroupOp.Annotations.EVALUATE_ON_GPU, true);
         liftedGroup1.addChild(
            new StatementPatternNode(new VarNode("s1"),
               new VarNode("p1"), new VarNode("o1"), null/* c */,
               Scope.DEFAULT_CONTEXTS));
         liftedGroup1.addChild(
               new StatementPatternNode(new VarNode("s2"),
                  new VarNode("p2"), new VarNode("o2"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));
         
         whereClause.addChild(liftedGroup1);

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTGPUAccelerationOptimizer rewriter =
            new ASTGPUAccelerationOptimizer();

      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);          
      
   }

   /**
    * Complex test case without attached filters.
    */
   public void testOptimizerComplexWithoutAttachedFilters() {

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {
          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s1"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          whereClause.addChild(
             new StatementPatternNode(new VarNode("s1"),
                new VarNode("p1"), new VarNode("o1"), null/* c */,
                Scope.DEFAULT_CONTEXTS));
          whereClause.addChild(
                new StatementPatternNode(new VarNode("s2"),
                   new VarNode("p2"), new VarNode("o2"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(
             new FilterNode(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] {
                      new VarNode("o1")})));
          
          whereClause.addChild(
             new StatementPatternNode(new VarNode("s3"),
                new VarNode("p3"), new VarNode("o3"), null/* c */,
                Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(
             new FilterNode(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] {
                      new VarNode("o2")})));
          
          whereClause.addChild(
             new StatementPatternNode(new VarNode("s4"),
                new VarNode("p4"), new VarNode("o4"), null/* c */,
                Scope.DEFAULT_CONTEXTS));

          whereClause.addChild(
             new StatementPatternNode(new VarNode("s5"),
                new VarNode("p5"), new VarNode("o5"), null/* c */,
                Scope.DEFAULT_CONTEXTS));
      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s1"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         expected.setWhereClause(whereClause);

         final JoinGroupNode liftedGroup1 = new JoinGroupNode();
         liftedGroup1.setProperty(GPUJoinGroupOp.Annotations.EVALUATE_ON_GPU, true);

         liftedGroup1.addChild(
            new StatementPatternNode(new VarNode("s1"),
               new VarNode("p1"), new VarNode("o1"), null/* c */,
               Scope.DEFAULT_CONTEXTS));
         liftedGroup1.addChild(
            new StatementPatternNode(new VarNode("s2"),
               new VarNode("p2"), new VarNode("o2"), null/* c */,
               Scope.DEFAULT_CONTEXTS));
         
         whereClause.addChild(liftedGroup1);

         whereClause.addChild(
            new FilterNode(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] {
                     new VarNode("o1")})));
         
         final JoinGroupNode liftedGroup2 = new JoinGroupNode();
         liftedGroup2.setProperty(GPUJoinGroupOp.Annotations.EVALUATE_ON_GPU, true);

         liftedGroup2.addChild(
            new StatementPatternNode(new VarNode("s3"),
               new VarNode("p3"), new VarNode("o3"), null/* c */,
               Scope.DEFAULT_CONTEXTS));
         
         whereClause.addChild(liftedGroup2);

         whereClause.addChild(
            new FilterNode(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] {
                     new VarNode("o2")})));

         final JoinGroupNode liftedGroup3 = new JoinGroupNode();
         liftedGroup3.setProperty(GPUJoinGroupOp.Annotations.EVALUATE_ON_GPU, true);

         liftedGroup3.addChild(
               new StatementPatternNode(new VarNode("s4"),
                  new VarNode("p4"), new VarNode("o4"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));

         liftedGroup3.addChild(
               new StatementPatternNode(new VarNode("s5"),
                  new VarNode("p5"), new VarNode("o5"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));
         
         whereClause.addChild(liftedGroup3);


      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTGPUAccelerationOptimizer rewriter =
            new ASTGPUAccelerationOptimizer();

      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);          
      
   }
   
   /**
    * Simple test case with attached filters.
    */
   @SuppressWarnings("serial")
   public void testOptimizerSimpleWithAttachedFilters() {

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {
          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s1"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          given.setWhereClause(whereClause);
          
          final StatementPatternNode spn1 = 
             new StatementPatternNode(new VarNode("s1"),
                new VarNode("p1"), new VarNode("o1"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
          
          final FilterNode fn1a = 
             new FilterNode(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] { new VarNode("s1") }));          

          final FilterNode fn1b = 
                new FilterNode(
                   new FunctionNode(
                      FunctionRegistry.BOUND, null/* scalarValues */,
                      new ValueExpressionNode[] { new VarNode("o1") }));     
          
          spn1.setAttachedJoinFilters(
             new ArrayList<FilterNode>() {{ add(fn1a); add(fn1b); }});

          
          final StatementPatternNode spn2 = 
             new StatementPatternNode(new VarNode("s2"),
                new VarNode("p2"), new VarNode("o2"), null/* c */,
                Scope.DEFAULT_CONTEXTS);

          final FilterNode fn2a = 
                new FilterNode(
                   new FunctionNode(
                      FunctionRegistry.BOUND, null/* scalarValues */,
                      new ValueExpressionNode[] { new VarNode("o2") }));
          
          spn2.setAttachedJoinFilters(
             new ArrayList<FilterNode>() {{ add(fn2a); }});

          whereClause.addChild(spn1);
          whereClause.addChild(spn2);
          
      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s1"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         expected.setWhereClause(whereClause);
         
         final StatementPatternNode spn1 = 
            new StatementPatternNode(new VarNode("s1"),
               new VarNode("p1"), new VarNode("o1"), null/* c */,
               Scope.DEFAULT_CONTEXTS);
         
         final StatementPatternNode spn2 = 
            new StatementPatternNode(new VarNode("s2"),
               new VarNode("p2"), new VarNode("o2"), null/* c */,
               Scope.DEFAULT_CONTEXTS);

         final FilterNode fn1a = 
            new FilterNode(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] { new VarNode("s1") }));

         final FilterNode fn1b = 
            new FilterNode(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] { new VarNode("o1") }));     
            
         final FilterNode fn2a = 
               new FilterNode(
                  new FunctionNode(
                     FunctionRegistry.BOUND, null/* scalarValues */,
                     new ValueExpressionNode[] { new VarNode("o2") }));     
         
         final JoinGroupNode liftedGroup = new JoinGroupNode();
         
         liftedGroup.setProperty(GPUJoinGroupOp.Annotations.EVALUATE_ON_GPU, true);
         liftedGroup.addChild(spn1);
         liftedGroup.addChild(spn2);
         liftedGroup.addChild(fn1a);
         liftedGroup.addChild(fn1b);
         liftedGroup.addChild(fn2a);
         
         whereClause.addChild(liftedGroup);

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTGPUAccelerationOptimizer rewriter =
            new ASTGPUAccelerationOptimizer();

      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);          
      
   }

}
