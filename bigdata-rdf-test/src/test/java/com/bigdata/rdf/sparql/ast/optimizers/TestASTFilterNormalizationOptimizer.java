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
 * Created on June 10, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for the {@link ASTFilterNormalizationOptimizer} class and associated
 * utility methods in {@link StaticAnalysis}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
@SuppressWarnings({ "rawtypes" })
public class TestASTFilterNormalizationOptimizer extends AbstractASTEvaluationTestCase {

   public TestASTFilterNormalizationOptimizer() {
   }

   public TestASTFilterNormalizationOptimizer(String name) {
       super(name);
   }
   
   
   /**
    * Test the {@link ASTFilterNormalizationOptimizer#extractToplevelConjuncts(
    * com.bigdata.rdf.sparql.ast.IValueExpressionNode, List)} method.
    */
   public void testExtractTopLevelConjunctsMethod() {

      // conjunct 1
      final FunctionNode bound1 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s1") });

      // conjunct 2
      final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s2") });
      final FunctionNode not1 = FunctionNode.NOT(bound2);

      // conjunct 3
      final FunctionNode bound3 =
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s3") });
      final FunctionNode bound4 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s4") });
      final FunctionNode bound5 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s5") });
      final FunctionNode or1 = 
         FunctionNode.OR(FunctionNode.AND(bound3,bound4), bound5);

      // conjunct 4
      final FunctionNode bound6 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s6") });
      
      final FunctionNode toCheck = 
         FunctionNode.AND(bound1,
            FunctionNode.AND(
               not1, FunctionNode.AND(or1, bound6)));

      final List<IValueExpressionNode> actual =
            StaticAnalysis.extractToplevelConjuncts(
            toCheck, new ArrayList<IValueExpressionNode>());

      assertFalse(StaticAnalysis.isCNF(toCheck));
      assertEquals(actual.size(), 4);
      assertEquals(actual.get(0), bound1);
      assertEquals(actual.get(1), not1);
      assertEquals(actual.get(2), or1);
      assertEquals(actual.get(3), bound6);
      
   }
   
   /**
    * Test the {@link ASTFilterNormalizationOptimizer#constructFiltersForValueExpressionNode(
    * IValueExpressionNode, List)} method.
    */
   public void testConstructFiltersForValueExpressionNodeMethod() {
      
      // conjunct 1
      final FunctionNode bound3 =
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s3") });
      final FunctionNode bound4 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s4") });
      final FunctionNode bound5 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s5") });
      final FunctionNode or1 = 
         FunctionNode.OR(FunctionNode.AND(bound3,bound4), bound5);

      // conjunct 2
      final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s2") });
      final FunctionNode not1 = FunctionNode.NOT(bound2);

      // conjunct 3
      final FunctionNode bound6 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s6") });


      // conjunct 4
      final FunctionNode bound1 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s1") });
      

      final FunctionNode base = 
         FunctionNode.AND(
            FunctionNode.AND(
               or1, FunctionNode.AND(not1, bound6)),bound1);


      final ASTFilterNormalizationOptimizer filterOptimizer = new ASTFilterNormalizationOptimizer();
      
      final List<FilterNode> filters =
         filterOptimizer.constructFiltersForValueExpressionNode(
            base, new ArrayList<FilterNode>());

      assertFalse(StaticAnalysis.isCNF(base));
      assertEquals(filters.size(), 4);
      assertEquals(filters.get(0), new FilterNode(or1));
      assertEquals(filters.get(1), new FilterNode(not1));
      assertEquals(filters.get(2), new FilterNode(bound6));
      assertEquals(filters.get(3), new FilterNode(bound1));      
      
   }
   
   /**
    * Test the {@link ASTFilterNormalizationOptimizer#toConjunctiveValueExpression(List)}
    * method.
    */
   public void testToConjunctiveValueExpressionMethod() {
      
      // conjunct 1
      final FunctionNode bound1 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s1") });

      // conjunct 2
      final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s2") });
      final FunctionNode not1 = FunctionNode.NOT(bound2);

      // conjunct 3
      final FunctionNode bound3 =
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s3") });
      final FunctionNode bound4 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s4") });
      final FunctionNode bound5 = 
         new FunctionNode(FunctionRegistry.BOUND,
            null, new ValueExpressionNode[] { new VarNode("s5") });
      final FunctionNode or1 = 
         FunctionNode.OR(FunctionNode.AND(bound3,bound4), bound5);

      // conjunct 4
      final FunctionNode bound6 = 
            new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s6") });

      final List<IValueExpressionNode> baseConjuncts = 
         new ArrayList<IValueExpressionNode>();
      baseConjuncts.add(bound1);
      baseConjuncts.add(not1);
      baseConjuncts.add(or1);
      baseConjuncts.add(bound6);
      
      final IValueExpressionNode expected =
         FunctionNode.AND(
            FunctionNode.AND(
               FunctionNode.AND(bound1, not1),
               or1),
            bound6);

      final IValueExpressionNode actual =
         StaticAnalysis.toConjunctiveValueExpression(baseConjuncts);
      
      assertFalse(StaticAnalysis.isCNF(actual));
      assertEquals(expected, actual);
   }

   
   
   /**
    * The FILTER
    * 
    * <pre>
    * SELECT ?s where { ?s ?p ?o . FILTER(?s=?o) }
    * </pre>
    * 
    * is not being modified.
    */
   public void testFilterDecompositionNoOp() {

       final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

       /*
        * Note: DO NOT share structures in this test!!!!
        */

       final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

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
           whereClause.addChild(
              new FilterNode(
                 FunctionNode.EQ(new VarNode("s"), new VarNode("o"))));

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

          final ProjectionNode projection = new ProjectionNode();
          expected.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          expected.setWhereClause(whereClause);

          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));
          
          final FilterNode filterNode = 
             new FilterNode(
                FunctionNode.EQ(new VarNode("s"), new VarNode("o")));
          assertTrue(StaticAnalysis.isCNF(filterNode));
          whereClause.addChild(filterNode);


       }

       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
             getQueryNode();

       assertSameAST(expected, actual);

   }
   
   /**
    * The FILTER
    * 
    * <pre>
    * SELECT ?s where { ?s ?p ?o . FILTER(?s=?o && ?s!=<http://www.test.com>) }
    * </pre>
    * 
    * is rewritten as
    * 
    * <pre>
    * SELECT ?s where { ?s ?p ?o . FILTER(?s=?o) . FILTER(?s!=<http://www.test.com>) }
    * </pre>
    * 
    */
   public void testSimpleConjunctiveFilter() {

      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

       /*
        * Note: DO NOT share structures in this test!!!!
        */
      final BigdataValueFactory f = store.getValueFactory();
      final BigdataURI testUri = f.createURI("http://www.test.com");

      final IV test = makeIV(testUri);
      
      final BigdataValue[] values = new BigdataValue[] { testUri };
      store.getLexiconRelation()
              .addTerms(values, values.length, false/* readOnly */);


       final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           given.setProjection(projection);
           projection.addProjectionVar(new VarNode("s"));
           
           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
           given.setWhereClause(whereClause);

           whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                   new VarNode("p"), new VarNode("o"), null/* c */,
                   Scope.DEFAULT_CONTEXTS));

           final FilterNode filterNode =
              new FilterNode(
                 FunctionNode.AND(
                     FunctionNode.EQ(new VarNode("s"), new VarNode("o")),
                     FunctionNode.NE(new VarNode("s"), new ConstantNode(test)))); 
           assertTrue(StaticAnalysis.isCNF(filterNode));

           whereClause.addChild(filterNode);

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

          final ProjectionNode projection = new ProjectionNode();
          expected.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          expected.setWhereClause(whereClause);

          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));
          whereClause.addChild(
             new FilterNode(
                FunctionNode.EQ(new VarNode("s"), new VarNode("o"))));
          whereClause.addChild(
                new FilterNode(
                   FunctionNode.NE(new VarNode("s"), new ConstantNode(test))));

       }

       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
             getQueryNode();

       assertSameAST(expected, actual);

   }

   /**
    * The FILTER
    * 
    * <pre>
    * SELECT ?s where { FILTER(NOT(?s<?o || BOUND(?o))) . OPTIONAL { ?s ?p ?o } }
    * </pre>
    * 
    * is rewritten as
    * 
    * <pre>
    * SELECT ?s where { OPTIONAL { ?s ?p ?o } .  FILTER(?s>=?o) . FILTER(!BOUND(?s) }
    * </pre>
    * 
    */
   public void testSimpleDisjunctiveFilter() {

       final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

       /*
        * Note: DO NOT share structures in this test!!!!
        */
       final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           given.setProjection(projection);
           projection.addProjectionVar(new VarNode("s"));
           
           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
           given.setWhereClause(whereClause);

           final FilterNode filterNode = 
              new FilterNode(
                 FunctionNode.NOT(
                    FunctionNode.OR(
                       FunctionNode.LT(new VarNode("s"), new VarNode("o")),
                          new FunctionNode(FunctionRegistry.BOUND, null,
                             new ValueExpressionNode[] { new VarNode("o") }))));
           assertFalse(StaticAnalysis.isCNF(filterNode));
           whereClause.addChild(filterNode);
           
           final StatementPatternNode spn = 
                 new StatementPatternNode(
                    new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                    null, Scope.DEFAULT_CONTEXTS);
           spn.setOptional(true);
              
           whereClause.addChild(spn);

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {
          
          final ProjectionNode projection = new ProjectionNode();
          expected.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          expected.setWhereClause(whereClause);

          final StatementPatternNode spn = 
             new StatementPatternNode(
                new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                null, Scope.DEFAULT_CONTEXTS);
          spn.setOptional(true);
          
          whereClause.addChild(spn);
          whereClause.addChild(
             new FilterNode(
                FunctionNode.GE(new VarNode("s"), new VarNode("o"))));
          whereClause.addChild(
                new FilterNode(
                   FunctionNode.NOT(
                      new FunctionNode(FunctionRegistry.BOUND, null,
                         new ValueExpressionNode[] { new VarNode("o") }))));

       }

       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
             getQueryNode();

       assertSameAST(expected, actual);

   }
   
   /**
    * Test rewriting of negated leaves, such as !(?x=?y) -> ?x!=?y, 
    * !(?a<?b) -> ?a>=?b, etc. in 
    */
   public void testNegationLeafRewriting01() {

       final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

       /*
        * Note: DO NOT share structures in this test!!!!
        */
       final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           given.setProjection(projection);
           projection.addProjectionVar(new VarNode("s"));
           
           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
           given.setWhereClause(whereClause);

           
           final FunctionNode filterEq = FunctionNode.EQ(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterNeq = FunctionNode.NE(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterLe = FunctionNode.LE(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterLt = FunctionNode.LT(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterGe = FunctionNode.GE(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterGt = FunctionNode.GT(new VarNode("s"), new VarNode("o"));

           final FunctionNode comb1 = FunctionNode.AND(filterEq, filterNeq);
           final FunctionNode comb2 = FunctionNode.AND(filterLe, filterLt);
           final FunctionNode comb3 = FunctionNode.AND(filterGt, filterGe);
           
           final FilterNode filterNode = 
              new FilterNode(
                FunctionNode.NOT(
                   FunctionNode.AND(FunctionNode.AND(comb1, comb2),comb3)));
           
           assertFalse(StaticAnalysis.isCNF(filterNode));
           whereClause.addChild(filterNode);

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

          final ProjectionNode projection = new ProjectionNode();
          expected.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          expected.setWhereClause(whereClause);

          
          final FunctionNode filterEqInv = FunctionNode.NE(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterNeqInv = FunctionNode.EQ(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterLeInv = FunctionNode.GT(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterLtInv = FunctionNode.GE(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterGeInv = FunctionNode.LT(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterGtInv = FunctionNode.LE(new VarNode("s"), new VarNode("o"));

          final FunctionNode comb1 = FunctionNode.OR(filterEqInv, filterNeqInv);
          final FunctionNode comb2 = FunctionNode.OR(filterLeInv, filterLtInv);
          final FunctionNode comb3 = FunctionNode.OR(filterGtInv, filterGeInv);
          
          final FilterNode filterNode = 
             new FilterNode(
                FunctionNode.OR(FunctionNode.OR(comb1, comb2),comb3));
          assertTrue(StaticAnalysis.isCNF(filterNode));
          
          whereClause.addChild(filterNode);

       }

       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
             getQueryNode();

       assertSameAST(expected, actual);

   }
   
   /**
    * Test rewriting of negated leaves, such as !(?x=?y) -> ?x!=?y, 
    * !(?a<?b) -> ?a>=?b, etc. (differs from v01 in tree shape).
    */
   public void testNegationLeafRewriting02() {

       final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

       /*
        * Note: DO NOT share structures in this test!!!!
        */
       final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

       // The source AST.
       final QueryRoot given = new QueryRoot(QueryType.SELECT);
       {

           final ProjectionNode projection = new ProjectionNode();
           given.setProjection(projection);
           projection.addProjectionVar(new VarNode("s"));
           
           final JoinGroupNode whereClause = new JoinGroupNode();
           whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
           given.setWhereClause(whereClause);

           
           final FunctionNode filterEq = FunctionNode.EQ(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterNeq = FunctionNode.NE(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterLe = FunctionNode.LE(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterLt = FunctionNode.LT(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterGe = FunctionNode.GE(new VarNode("s"), new VarNode("o"));
           final FunctionNode filterGt = FunctionNode.GT(new VarNode("s"), new VarNode("o"));

           final FunctionNode comb1 = FunctionNode.AND(filterEq, filterNeq);
           final FunctionNode comb2 = FunctionNode.AND(filterLe, filterLt);
           final FunctionNode comb3 = FunctionNode.AND(filterGt, filterGe);

           final FilterNode filterNode = 
              new FilterNode(
                 FunctionNode.NOT(
                    FunctionNode.AND(comb1, FunctionNode.AND(comb2,comb3))));
           assertFalse(StaticAnalysis.isCNF(filterNode));

           whereClause.addChild(filterNode);

       }

       // The expected AST after the rewrite.
       final QueryRoot expected = new QueryRoot(QueryType.SELECT);
       {

          final ProjectionNode projection = new ProjectionNode();
          expected.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          expected.setWhereClause(whereClause);

          
          final FunctionNode filterEqInv = FunctionNode.NE(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterNeqInv = FunctionNode.EQ(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterLeInv = FunctionNode.GT(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterLtInv = FunctionNode.GE(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterGeInv = FunctionNode.LT(new VarNode("s"), new VarNode("o"));
          final FunctionNode filterGtInv = FunctionNode.LE(new VarNode("s"), new VarNode("o"));

          final FunctionNode comb1 = FunctionNode.OR(filterEqInv, filterNeqInv);
          final FunctionNode comb2 = FunctionNode.OR(filterLeInv, filterLtInv);
          final FunctionNode comb3 = FunctionNode.OR(filterGtInv, filterGeInv);
          
          final FilterNode filterNode = 
             new FilterNode(
                FunctionNode.OR(comb1, FunctionNode.OR(comb2,comb3)));
          assertTrue(StaticAnalysis.isCNF(filterNode));

          whereClause.addChild(filterNode);

       }

       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(given), store);
       
       final IQueryNode actual = 
          rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
             getQueryNode();

       assertSameAST(expected, actual);

   }

   /**
    * Test level three pushing of negation.
    */
   public void testNestedNegationRewriting() {
      
      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          
          final FunctionNode filterANot1 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                new ValueExpressionNode[] { new VarNode("o") }));
          final FunctionNode filterANot2 = FunctionNode.NOT(filterANot1);
          final FunctionNode filterANot3 = FunctionNode.NOT(filterANot2);

          final FunctionNode filterBNot1 = 
                FunctionNode.NOT(
                   new FunctionNode(FunctionRegistry.EQ, null,
                   new ValueExpressionNode[] { new VarNode("s"), new VarNode("o") }));
          final FunctionNode filterBNot2 = FunctionNode.NOT(filterBNot1);
          final FunctionNode filterBNot3 = FunctionNode.NOT(filterBNot2);
          final FunctionNode filterBNot4 = FunctionNode.NOT(filterBNot3);

          final FilterNode filterNode = 
             new FilterNode(
                FunctionNode.NOT(
                   FunctionNode.AND(filterANot3, filterBNot4)));
          assertFalse(StaticAnalysis.isCNF(filterNode));

          whereClause.addChild(filterNode);

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         final FunctionNode bound = 
            new FunctionNode(FunctionRegistry.BOUND, null,
            new ValueExpressionNode[] { new VarNode("o") });

         final FunctionNode neq = 
            new FunctionNode(FunctionRegistry.NE, null,
               new ValueExpressionNode[] { new VarNode("s"), new VarNode("o") });
            
         FilterNode filterNode = new FilterNode(FunctionNode.OR(bound, neq));
         assertTrue(StaticAnalysis.isCNF(filterNode));

         // all NOT nodes should be resolved
         whereClause.addChild(filterNode);

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);      
   }

   /**
    * Test level three pushing of negation.
    */
   public void testNestedNegationRewritingAndSplit() {
      
      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          
          final FunctionNode filterANot1 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                new ValueExpressionNode[] { new VarNode("o") }));
          final FunctionNode filterANot2 = FunctionNode.NOT(filterANot1);
          final FunctionNode filterANot3 = FunctionNode.NOT(filterANot2);

          final FunctionNode filterBNot1 = 
                FunctionNode.NOT(
                   new FunctionNode(FunctionRegistry.EQ, null,
                   new ValueExpressionNode[] { new VarNode("s"), new VarNode("o") }));
          final FunctionNode filterBNot2 = FunctionNode.NOT(filterBNot1);
          final FunctionNode filterBNot3 = FunctionNode.NOT(filterBNot2);
          final FunctionNode filterBNot4 = FunctionNode.NOT(filterBNot3);

          final FilterNode filterNode =
             new FilterNode(
                FunctionNode.NOT(
                  FunctionNode.OR(filterANot3, filterBNot4)));
          assertFalse(StaticAnalysis.isCNF(filterNode));

          whereClause.addChild(filterNode);

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         final FunctionNode bound = 
            new FunctionNode(FunctionRegistry.BOUND, null,
            new ValueExpressionNode[] { new VarNode("o") });

         final FunctionNode neq = 
            new FunctionNode(FunctionRegistry.NE, null,
               new ValueExpressionNode[] { new VarNode("s"), new VarNode("o") });
            
         whereClause.addChild(new FilterNode(bound));
         whereClause.addChild(new FilterNode(neq));

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);      
   }
   
   /**
    * Test switch of OR over AND expression expression.
    */
   public void testSimpleOrAndSwitch() {
      
      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          final StatementPatternNode spn = 
                new StatementPatternNode(
                   new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                   null, Scope.DEFAULT_CONTEXTS);
          whereClause.addChild(spn);

          final FunctionNode bound1 = 
             new FunctionNode(FunctionRegistry.BOUND, null,
                new ValueExpressionNode[] { new VarNode("s1") });
          final FunctionNode bound2 = 
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s2") });
          final FunctionNode bound3 = 
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s3") });
          final FunctionNode bound4 = 
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s4") });
          final FunctionNode bound5 = 
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s5") });
          
          final FilterNode filterNode =
             new FilterNode(
                FunctionNode.OR(
                   FunctionNode.AND(bound1, bound2),
                   FunctionNode.AND(bound3, 
                      FunctionNode.AND(bound4, bound5))));
          assertFalse(StaticAnalysis.isCNF(filterNode));
          
          whereClause.addChild(filterNode);

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         final StatementPatternNode spn = 
               new StatementPatternNode(
                  new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                  null, Scope.DEFAULT_CONTEXTS);
         whereClause.addChild(spn);

         final FunctionNode bound1 = 
            new FunctionNode(FunctionRegistry.BOUND, null,
               new ValueExpressionNode[] { new VarNode("s1") });
         final FunctionNode bound2 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s2") });
         final FunctionNode bound3 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s3") });
         final FunctionNode bound4 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s4") });
         final FunctionNode bound5 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s5") });

         final FunctionNode and1 = FunctionNode.OR(bound1, bound3);
         final FunctionNode and2 = FunctionNode.OR(bound1, bound4);
         final FunctionNode and3 = FunctionNode.OR(bound1, bound5);
         final FunctionNode and4 = FunctionNode.OR(bound2, bound3);
         final FunctionNode and5 = FunctionNode.OR(bound2, bound4);
         final FunctionNode and6 = FunctionNode.OR(bound2, bound5);
         
         // after splitting, we should get the following conjuncts
         whereClause.addChild(new FilterNode(and1));
         whereClause.addChild(new FilterNode(and2));
         whereClause.addChild(new FilterNode(and3));
         whereClause.addChild(new FilterNode(and4));
         whereClause.addChild(new FilterNode(and5));
         whereClause.addChild(new FilterNode(and6));

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);
   }

   
   /**
    * Test switch of OR over AND expression with top-level negation expression.
    */
   public void testOrAndSwitchWithNegation() {
      
      final ASTFilterNormalizationOptimizer rewriter = 
         new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setQueryHint(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          final StatementPatternNode spn = 
                new StatementPatternNode(
                   new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                   null, Scope.DEFAULT_CONTEXTS);
          whereClause.addChild(spn);

          final FunctionNode notBound1 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s1") }));
          final FunctionNode notBound2 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s2") }));
          final FunctionNode notBound3 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s3") }));
          final FunctionNode notBound4 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s4") }));
          final FunctionNode notBound5 = 
             FunctionNode.NOT(
                new FunctionNode(FunctionRegistry.BOUND, null,
                   new ValueExpressionNode[] { new VarNode("s5") }));
          
          final FilterNode filterNode = 
             new FilterNode(
                FunctionNode.NOT(
                  FunctionNode.AND(
                     FunctionNode.OR(notBound1, notBound2),
                     FunctionNode.OR(notBound3, 
                        FunctionNode.OR(notBound4, notBound5)))));
          
          assertFalse(StaticAnalysis.isCNF(filterNode));
          whereClause.addChild(filterNode);
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setQueryHint(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);
         

         final StatementPatternNode spn = 
               new StatementPatternNode(
                  new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                  null, Scope.DEFAULT_CONTEXTS);
         whereClause.addChild(spn);

         final FunctionNode bound1 = 
            new FunctionNode(FunctionRegistry.BOUND, null,
               new ValueExpressionNode[] { new VarNode("s1") });
         final FunctionNode bound2 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s2") });
         final FunctionNode bound3 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s3") });
         final FunctionNode bound4 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s4") });
         final FunctionNode bound5 = 
               new FunctionNode(FunctionRegistry.BOUND, null,
                  new ValueExpressionNode[] { new VarNode("s5") });

         final FunctionNode or1 = FunctionNode.OR(bound1, bound3);
         final FunctionNode or2 = FunctionNode.OR(bound1, bound4);
         final FunctionNode or3 = FunctionNode.OR(bound1, bound5);
         final FunctionNode or4 = FunctionNode.OR(bound2, bound3);
         final FunctionNode or5 = FunctionNode.OR(bound2, bound4);
         final FunctionNode or6 = FunctionNode.OR(bound2, bound5);
         
         // after splitting, we should get the following conjuncts
         whereClause.addChild(new FilterNode(or1));
         whereClause.addChild(new FilterNode(or2));
         whereClause.addChild(new FilterNode(or3));
         whereClause.addChild(new FilterNode(or4));
         whereClause.addChild(new FilterNode(or5));
         whereClause.addChild(new FilterNode(or6));
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");

      }
      
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);
   }
   
   /**
    * Test recursive optimization of OR - AND - OR - AND pattern.
    */
   public void testOrAndSwitchRecursive() {
      
      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          final StatementPatternNode spn = 
                new StatementPatternNode(
                   new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                   null, Scope.DEFAULT_CONTEXTS);
          whereClause.addChild(spn);

          final FunctionNode bound1 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s1") });
          final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s2") });
          final FunctionNode bound3 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s3") });
          final FunctionNode bound4 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s4") });
          final FunctionNode bound5 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s5") });
          final FunctionNode bound6 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s6") });
          final FunctionNode bound7 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s7") });
          final FunctionNode bound8 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s8") });
          final FunctionNode bound9 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s9") });
          final FunctionNode bound10 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s10") });
          final FunctionNode bound11 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s11") });
          final FunctionNode bound12 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s12") });
          final FunctionNode bound13 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s13") });
          final FunctionNode bound14 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s14") });
          final FunctionNode bound15 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s15") });
          final FunctionNode bound16 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s16") });

          final FilterNode filterNode =
             new FilterNode(
                FunctionNode.OR(
                  FunctionNode.AND(
                     FunctionNode.OR(
                        FunctionNode.AND(bound1, bound2),
                           FunctionNode.AND(bound3, bound4)
                        ),
                        FunctionNode.OR(
                           FunctionNode.AND(bound5, bound6),
                           FunctionNode.AND(bound7, bound8)      
                        )
                     ),
                     FunctionNode.AND(
                        FunctionNode.OR(
                           FunctionNode.AND(bound9, bound10),
                           FunctionNode.AND(bound11, bound12)
                        ),
                        FunctionNode.OR(
                           FunctionNode.AND(bound13, bound14),
                           FunctionNode.AND(bound15, bound16)      
                        ))));
          
          assertFalse(StaticAnalysis.isCNF(filterNode));
             
          whereClause.addChild(filterNode);

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         final StatementPatternNode spn = 
               new StatementPatternNode(
                  new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                  null, Scope.DEFAULT_CONTEXTS);
         whereClause.addChild(spn);

         final FunctionNode bound1 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s1") });
         final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s2") });
         final FunctionNode bound3 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s3") });
         final FunctionNode bound4 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s4") });
         final FunctionNode bound5 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s5") });
         final FunctionNode bound6 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s6") });
         final FunctionNode bound7 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s7") });
         final FunctionNode bound8 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s8") });
         final FunctionNode bound9 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s9") });
         final FunctionNode bound10 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s10") });
         final FunctionNode bound11 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s11") });
         final FunctionNode bound12 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s12") });
         final FunctionNode bound13 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s13") });
         final FunctionNode bound14 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s14") });
         final FunctionNode bound15 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s15") });
         final FunctionNode bound16 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s16") });

         /**
          * Sketch of intended rewriting process (bottom-up)
          * 
          * ### STEP 1: for the four leaf nodes we get the following.
          * 
          * FunctionNode.OR(
          *   FunctionNode.AND(
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound1,bound3)
          *       FunctionNode.OR(bound1,bound4)
          *       FunctionNode.OR(bound2,bound3)
          *       FunctionNode.OR(bound2,bound4)
          *     ),
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound5,bound7)
          *       FunctionNode.OR(bound5,bound8)
          *       FunctionNode.OR(bound6,bound7)
          *       FunctionNode.OR(bound6,bound8)
          *     )
          *   ),
          *   FunctionNode.AND(
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound9,bound11)
          *       FunctionNode.OR(bound9,bound12)
          *       FunctionNode.OR(bound10,bound11)
          *       FunctionNode.OR(bound10,bound12)
          *     ), 
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound13,bound15)
          *       FunctionNode.OR(bound13,bound16)
          *       FunctionNode.OR(bound14,bound15)
          *       FunctionNode.OR(bound14,bound16)
          *     )
          *   )
          * )
          * 
          * ### STEP 2: pushing down the top-level OR, we compute the cross
          *             product of the left and right disjuncts (we flatten
          *             out the and in the representation below):
          *             
          * FunctionNode.AND(
          *   FunctionNode.AND(
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound3),
          *       FunctionNode.OR(bound9,bound11)
          *     ),
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound3),
          *       FunctionNode.OR(bound9,bound12)
          *     ),
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound3),
          *       FunctionNode.OR(bound10,bound11)
          *     ),
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound3),
          *       FunctionNode.OR(bound10,bound12)
          *     ),
          *     
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound4),
          *       FunctionNode.OR(bound9,bound11)
          *     ),
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound4),
          *       FunctionNode.OR(bound9,bound12)
          *     ),
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound4),
          *       FunctionNode.OR(bound10,bound11)
          *     ),
          *     FunctionNode.OR(
          *       FunctionNode.OR(bound1,bound4),
          *       FunctionNode.OR(bound10,bound12)
          *     ),
          *     
          *     ...
          *     
          * Each of those topmost OR expression gives us one FILTER expression
          * in the end, resulting in 8x8 = 64 FILTERs. We construct them
          * schematically below.
          */

         final List<FunctionNode> lefts = new ArrayList<FunctionNode>();
         lefts.add(FunctionNode.OR(bound1,bound3));
         lefts.add(FunctionNode.OR(bound1,bound4));
         lefts.add(FunctionNode.OR(bound2,bound3));
         lefts.add(FunctionNode.OR(bound2,bound4));
         lefts.add(FunctionNode.OR(bound5,bound7));
         lefts.add(FunctionNode.OR(bound5,bound8));
         lefts.add(FunctionNode.OR(bound6,bound7));
         lefts.add(FunctionNode.OR(bound6,bound8));
            
         final List<FunctionNode> rights = new ArrayList<FunctionNode>();
         rights.add(FunctionNode.OR(bound9,bound11));
         rights.add(FunctionNode.OR(bound9,bound12));
         rights.add(FunctionNode.OR(bound10,bound11));
         rights.add(FunctionNode.OR(bound10,bound12));
         rights.add(FunctionNode.OR(bound13,bound15));
         rights.add(FunctionNode.OR(bound13,bound16));
         rights.add(FunctionNode.OR(bound14,bound15));
         rights.add(FunctionNode.OR(bound14,bound16));
         
         for (final FunctionNode left : lefts) {
            for (final FunctionNode right : rights) {
               whereClause.addChild(
                  new FilterNode(FunctionNode.OR(left, right)));
            }
         }
         
      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);      
   }
   

   /**
    * Test recursive optimization of OR - OR - AND pattern.
    */
   public void testOrOrAndSwitch() {

      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          final StatementPatternNode spn = 
                new StatementPatternNode(
                   new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                   null, Scope.DEFAULT_CONTEXTS);
          whereClause.addChild(spn);

          final FunctionNode bound1 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s1") });
          final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s2") });
          final FunctionNode bound3 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s3") });
          final FunctionNode bound4 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s4") });
          final FunctionNode bound5 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s5") });
          final FunctionNode bound6 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s6") });
          final FunctionNode bound7 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s7") });
          final FunctionNode bound8 = new FunctionNode(FunctionRegistry.BOUND,
                null, new ValueExpressionNode[] { new VarNode("s8") });
             
          final FilterNode filterNode = new FilterNode(
             FunctionNode.OR(
                FunctionNode.OR(
                   FunctionNode.AND(bound1, bound2),
                      FunctionNode.AND(bound3, bound4)
                   ),
                   FunctionNode.OR(
                      FunctionNode.AND(bound5, bound6),
                      FunctionNode.AND(bound7, bound8)
                   )));
          
          assertFalse(StaticAnalysis.isCNF(filterNode));
          whereClause.addChild(filterNode);

      }      

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         
         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         final StatementPatternNode spn = 
               new StatementPatternNode(
                  new VarNode("s"), new VarNode("p"), new VarNode("o"), 
                  null, Scope.DEFAULT_CONTEXTS);
         whereClause.addChild(spn);

         final FunctionNode bound1 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s1") });
         final FunctionNode bound2 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s2") });
         final FunctionNode bound3 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s3") });
         final FunctionNode bound4 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s4") });
         final FunctionNode bound5 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s5") });
         final FunctionNode bound6 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s6") });
         final FunctionNode bound7 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s7") });
         final FunctionNode bound8 = new FunctionNode(FunctionRegistry.BOUND,
               null, new ValueExpressionNode[] { new VarNode("s8") });
         
         /**
          * 
          * ### STEP 1: generates to OR connected leafs in CNF
          * 
          * FunctionNode.OR(
          *   FunctionNode.AND(
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound1, bound3),
          *       FunctionNode.OR(bound1, bound4)
          *     ),
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound2, bound3),
          *       FunctionNode.OR(bound2, bound4)
          *     )
          *   ),
          *   FunctionNode.AND(
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound5, bound7),
          *       FunctionNode.OR(bound5, bound8)
          *     ),
          *     FunctionNode.AND(
          *       FunctionNode.OR(bound6, bound7),
          *       FunctionNode.OR(bound6, bound8)
          *     )
          *   )
          * )
          * 
          * ### STEP 2: pushes down the uppermost OR expression
          * 
          *  Considers all OR-leafs in the left top-level AND expression
          *  and joins them with OR-leafs in the right top-level AND expression.
          *  After decomposing, this actually gives us 4x4 = 16 FILTERs.
          *   
          */

         final List<FunctionNode> lefts = new ArrayList<FunctionNode>();
         lefts.add(FunctionNode.OR(bound1,bound3));
         lefts.add(FunctionNode.OR(bound1,bound4));
         lefts.add(FunctionNode.OR(bound2,bound3));
         lefts.add(FunctionNode.OR(bound2,bound4));
            
         final List<FunctionNode> rights = new ArrayList<FunctionNode>();
         rights.add(FunctionNode.OR(bound5,bound7));
         rights.add(FunctionNode.OR(bound5,bound8));
         rights.add(FunctionNode.OR(bound6,bound7));
         rights.add(FunctionNode.OR(bound6,bound8));
         
         for (final FunctionNode left : lefts) {
            for (final FunctionNode right : rights) {
               whereClause.addChild(
                  new FilterNode(FunctionNode.OR(left, right)));
            }
         }
         
      }
            
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);   
   }
   
   /**
    * Test removal of duplicate filter.
    */
   public void testRemoveDuplicateFilter() {
      
      final ASTFilterNormalizationOptimizer rewriter = new ASTFilterNormalizationOptimizer();

      /*
       * Note: DO NOT share structures in this test!!!!
       */

      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          whereClause.addChild(new StatementPatternNode(new VarNode("s1"),
                  new VarNode("p1"), new VarNode("o1"), null/* c */,
                  Scope.DEFAULT_CONTEXTS)); // just noise

          
          // two times exactly the same pattern
          final FunctionNode simpleFunctionNode1 =
             FunctionNode.NE(new VarNode("s1"), new VarNode("s2"));
          final FunctionNode simpleFunctionNode2 =
             FunctionNode.NE(new VarNode("s1"), new VarNode("s2"));
          
          // three times the same pattern
          final FunctionNode complexFunctionNode1 =
             FunctionNode.OR(
                FunctionNode.NE(new VarNode("s1"), new VarNode("s2")),
                FunctionNode.NOT(
                   new FunctionNode(
                      FunctionRegistry.BOUND,
                      null, new ValueExpressionNode[] { new VarNode("s1") })));
          final FunctionNode complexFunctionNode2 =
             FunctionNode.OR(
                FunctionNode.NE(new VarNode("s1"), new VarNode("s2")),
                FunctionNode.NOT(
                   new FunctionNode(
                      FunctionRegistry.BOUND,
                      null, new ValueExpressionNode[] { new VarNode("s1") })));
          final FunctionNode complexFunctionNode3 =
             FunctionNode.OR(
                FunctionNode.NE(new VarNode("s1"), new VarNode("s2")),
                FunctionNode.NOT(
                   new FunctionNode(
                      FunctionRegistry.BOUND,
                      null, new ValueExpressionNode[] { new VarNode("s1") })));
          
          whereClause.addChild(new FilterNode(simpleFunctionNode1));
          whereClause.addChild(new FilterNode(simpleFunctionNode2));
          whereClause.addChild(new FilterNode(complexFunctionNode1));
          whereClause.addChild(new FilterNode(complexFunctionNode2));

          whereClause.addChild(new StatementPatternNode(new VarNode("s2"),
                new VarNode("p2"), new VarNode("o2"), null/* c */,
                Scope.DEFAULT_CONTEXTS)); // just noise

          whereClause.addChild(new FilterNode(complexFunctionNode3));

          assertTrue(StaticAnalysis.isCNF(simpleFunctionNode1));
          assertTrue(StaticAnalysis.isCNF(simpleFunctionNode2));
          assertTrue(StaticAnalysis.isCNF(complexFunctionNode1));
          assertTrue(StaticAnalysis.isCNF(complexFunctionNode2));
          assertTrue(StaticAnalysis.isCNF(complexFunctionNode3));

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         whereClause.addChild(new StatementPatternNode(new VarNode("s1"),
                 new VarNode("p1"), new VarNode("o1"), null/* c */,
                 Scope.DEFAULT_CONTEXTS)); // just noise

         
         // the simple function node
         final FunctionNode simpleFunctionNode =
            FunctionNode.NE(new VarNode("s1"), new VarNode("s2"));
         
         // the complex function node
         final FunctionNode complexFunctionNode =
            FunctionNode.OR(
               FunctionNode.NE(new VarNode("s1"), new VarNode("s2")),
               FunctionNode.NOT(
                  new FunctionNode(
                     FunctionRegistry.BOUND,
                     null, new ValueExpressionNode[] { new VarNode("s1") })));

         
         whereClause.addChild(new FilterNode(simpleFunctionNode));

         whereClause.addChild(new StatementPatternNode(new VarNode("s2"),
               new VarNode("p2"), new VarNode("o2"), null/* c */,
               Scope.DEFAULT_CONTEXTS)); // just noise

         whereClause.addChild(new FilterNode(complexFunctionNode));

         assertTrue(StaticAnalysis.isCNF(simpleFunctionNode));
         assertTrue(StaticAnalysis.isCNF(complexFunctionNode));

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);      
   }
   
   /**
    * Test removal of duplicate filter, where the duplicate is introduced
    * through the CNF based decomposition process. This is a variant of test
    * {@link TestASTFilterNormalizationOptimizer#testSimpleConjunctiveFilter()},
    * where we just add a duplicate.
    */
   public void testRemoveDuplicateGeneratedFilter() {

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final BigdataValueFactory f = store.getValueFactory();
      final BigdataURI testUri = f.createURI("http://www.test.com");

      final IV test = makeIV(testUri);
      
      final BigdataValue[] values = new BigdataValue[] { testUri };
      store.getLexiconRelation()
              .addTerms(values, values.length, false/* readOnly */);
      
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);

          whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));

          final FilterNode filterNode =
             new FilterNode(
                FunctionNode.AND(
                    FunctionNode.EQ(new VarNode("s"), new VarNode("o")),
                    FunctionNode.NE(new VarNode("s"), new ConstantNode(test)))); 
          
          // difference towards base test: this is the duplicate to be dropped
          whereClause.addChild(
                new FilterNode(
                   FunctionNode.EQ(new VarNode("s"), new VarNode("o"))));
          
          assertTrue(StaticAnalysis.isCNF(filterNode));

          whereClause.addChild(filterNode);

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);

         whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                 new VarNode("p"), new VarNode("o"), null/* c */,
                 Scope.DEFAULT_CONTEXTS));
         whereClause.addChild(
            new FilterNode(
               FunctionNode.EQ(new VarNode("s"), new VarNode("o"))));
         whereClause.addChild(
               new FilterNode(
                  FunctionNode.NE(new VarNode("s"), new ConstantNode(test))));

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTFilterNormalizationOptimizer rewriter =
         new ASTFilterNormalizationOptimizer();
      
      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);      
   }
   
   /**
    * Test removal of unsatisfiable filters. More precisely, the query
    * 
    * SELECT ?s WHERE {
    *   ?s ?p ?o1 .
    *   { ?s ?p ?o2 }
    *   OPTIONAL { ?s ?p ?o3 }
    *   
    *   FILTER(bound(?o1))
    *   FILTER(bound(?o2))
    *   FILTER(bound(?o3))
    *   
    *   FILTER(!bound(?o1))
    *   FILTER(!bound(?o2))
    *   FILTER(!bound(?o3))
    *   FILTER(!bound(?o4))
    *   
    *   // some duplicates (which should be dropped)
    *   FILTER(!bound(?o2))
    *   FILTER(!bound(?o3))
    * }
    * 
    * will be rewritten to
    * 
    * SELECT ?s WHERE {
    *   ?s ?p ?o1 .
    *   { ?s ?p ?o2 }
    *   OPTIONAL { ?s ?p ?o3 }
    *   
    *   // ?o1 and ?o2 are definitely bound, so we can't optimize away
    *   FILTER(bound(?o3))
    *   
    *   // ?o4 is the only variable that is definitely not bound
    *   FILTER(!bound(?o1))
    *   FILTER(!bound(?o2))
    *   FILTER(!bound(?o3))
    * }
    * 
    */
   public void testRemoveUnsatisfiableFilters() {

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      
      final IBindingSet[] bsets = new IBindingSet[] { new ListBindingSet() };

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

          final ProjectionNode projection = new ProjectionNode();
          given.setProjection(projection);
          projection.addProjectionVar(new VarNode("s"));
          
          final JoinGroupNode whereClause = new JoinGroupNode();
          whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
          given.setWhereClause(whereClause);
          
          final StatementPatternNode spo1 =
             new StatementPatternNode(new VarNode("s"),
                new VarNode("p"), new VarNode("o1"), null/* c */,
                Scope.DEFAULT_CONTEXTS);

          final StatementPatternNode spo2 =
                new StatementPatternNode(new VarNode("s"),
                   new VarNode("p"), new VarNode("o2"), null/* c */,
                   Scope.DEFAULT_CONTEXTS);       
          final JoinGroupNode jgn = new JoinGroupNode(spo2);

          final StatementPatternNode spo3 =
             new StatementPatternNode(new VarNode("s"),
                new VarNode("p"), new VarNode("o3"), null/* c */,
                Scope.DEFAULT_CONTEXTS);
          spo3.setOptional(true);

          whereClause.addChild(spo1);
          whereClause.addChild(jgn);
          whereClause.addChild(spo3);
     

          final FunctionNode filterBound1 = 
             new FunctionNode(
                FunctionRegistry.BOUND, null/* scalarValues */,
                new ValueExpressionNode[] {
                   new VarNode("o1")});
          
          final FunctionNode filterBound2 = 
             new FunctionNode(
                FunctionRegistry.BOUND, null/* scalarValues */,
                new ValueExpressionNode[] {
                   new VarNode("o2")});
          
          final FunctionNode filterBound3 = 
             new FunctionNode(
                FunctionRegistry.BOUND, null/* scalarValues */,
                new ValueExpressionNode[] {
                   new VarNode("o3")});
          
          final FunctionNode filterNotBound1 = 
             FunctionNode.NOT(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] {
                      new VarNode("o1")}));
             
          final FunctionNode filterNotBound2 = 
             FunctionNode.NOT(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] {
                      new VarNode("o2")}));
             
          final FunctionNode filterNotBound3 = 
             FunctionNode.NOT(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] {
                      new VarNode("o3")}));  
 
          final FunctionNode filterNotBound4 = 
             FunctionNode.NOT(
                new FunctionNode(
                   FunctionRegistry.BOUND, null/* scalarValues */,
                   new ValueExpressionNode[] {
                      new VarNode("o4")}));  
          
          whereClause.addChild(new FilterNode(filterBound1));
          whereClause.addChild(new FilterNode(filterBound2));
          whereClause.addChild(new FilterNode(filterBound3));
          whereClause.addChild(new FilterNode(filterNotBound1));
          whereClause.addChild(new FilterNode(filterNotBound2));
          whereClause.addChild(new FilterNode(filterNotBound3));
          whereClause.addChild(new FilterNode(filterNotBound4));

          // add some duplicates (they should be removed)
          whereClause.addChild(new FilterNode(filterNotBound2));
          whereClause.addChild(new FilterNode(filterNotBound3));

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection = new ProjectionNode();
         expected.setProjection(projection);
         projection.addProjectionVar(new VarNode("s"));
         
         final JoinGroupNode whereClause = new JoinGroupNode();
         whereClause.setProperty(QueryHints.NORMALIZE_FILTER_EXPRESSIONS, "true");
         expected.setWhereClause(whereClause);
         
         final StatementPatternNode spo1 =
            new StatementPatternNode(new VarNode("s"),
               new VarNode("p"), new VarNode("o1"), null/* c */,
               Scope.DEFAULT_CONTEXTS);

         final StatementPatternNode spo2 =
               new StatementPatternNode(new VarNode("s"),
                  new VarNode("p"), new VarNode("o2"), null/* c */,
                  Scope.DEFAULT_CONTEXTS);       
         final JoinGroupNode jgn = new JoinGroupNode(spo2);

         final StatementPatternNode spo3 =
            new StatementPatternNode(new VarNode("s"),
               new VarNode("p"), new VarNode("o3"), null/* c */,
               Scope.DEFAULT_CONTEXTS);
         spo3.setOptional(true);

         whereClause.addChild(spo1);
         whereClause.addChild(jgn);
         whereClause.addChild(spo3);
    
         final FunctionNode filterBound3 = 
            new FunctionNode(
               FunctionRegistry.BOUND, null/* scalarValues */,
               new ValueExpressionNode[] {
                  new VarNode("o3")});
         
         final FunctionNode filterNotBound1 = 
            FunctionNode.NOT(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] {
                     new VarNode("o1")}));
            
         final FunctionNode filterNotBound2 = 
            FunctionNode.NOT(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] {
                     new VarNode("o2")}));
            
         final FunctionNode filterNotBound3 = 
            FunctionNode.NOT(
               new FunctionNode(
                  FunctionRegistry.BOUND, null/* scalarValues */,
                  new ValueExpressionNode[] {
                     new VarNode("o3")}));  

         
         whereClause.addChild(new FilterNode(filterBound3));
         whereClause.addChild(new FilterNode(filterNotBound1));
         whereClause.addChild(new FilterNode(filterNotBound2));
         whereClause.addChild(new FilterNode(filterNotBound3));

      }

      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);
      
      final ASTFilterNormalizationOptimizer rewriter =
            new ASTFilterNormalizationOptimizer();

      final IQueryNode actual = 
         rewriter.optimize(context, new QueryNodeWithBindingSet(given, bsets)).
            getQueryNode();

      assertSameAST(expected, actual);          
      
   }


}
