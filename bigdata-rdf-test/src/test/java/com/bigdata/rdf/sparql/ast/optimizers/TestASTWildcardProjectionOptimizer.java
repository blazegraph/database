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
 * Created on Oct 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collections;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
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

/**
 * Unit tests for {@link ASTWildcardProjectionOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestASTWildcardProjectionOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTWildcardProjectionOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTWildcardProjectionOptimizer(String name) {
        super(name);
    }
    

    /**
     * <pre>
     * SELECT * 
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     */
    public void test_wildcardProjectionOptimizer00() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
        
        final AST2BOpContext context = 
              new AST2BOpContext(new ASTContainer(given), store);
        
        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * <pre>
     * SELECT DISTINCT * 
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     */
    public void test_wildcardProjectionOptimizer01() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
        final AST2BOpContext context = 
              new AST2BOpContext(new ASTContainer(given), store);
        
        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * <pre>
     * SELECT REDUCED * 
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     */
    public void test_wildcardProjectionOptimizer02() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setReduced(true);
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setReduced(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
        final AST2BOpContext context = 
              new AST2BOpContext(new ASTContainer(given), store);
        
        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

   /**
    * <pre>
    * SELECT * 
    *   JoinGroupNode {
    *     SELECT (*) {
    *       JoinGroupNode {
    *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
    *       }
    *     }
    *    StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(x), DEFAULT_CONTEXTS)
    *   }
    * </pre>
    * 
    * @see <a href="http://trac.bigdata.com/ticket/757" > Wildcard projection
    *      was not rewritten. </a>
    */
   public void test_wildcard_nestedSubquery() {

      /*
       * Note: DO NOT share structures in this test!!!!
       */
      final IBindingSet[] bsets = new IBindingSet[] {};

      @SuppressWarnings("rawtypes")
      final IV p = makeIV(new URIImpl("http://example/p"));

      @SuppressWarnings("rawtypes")
      final IV q = makeIV(new URIImpl("http://example/q"));

      // The source AST.
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection1 = new ProjectionNode();
         given.setProjection(projection1);
         projection1.addProjectionVar(new VarNode("*"));

         final JoinGroupNode whereClause1 = new JoinGroupNode();
         given.setWhereClause(whereClause1);

         final SubqueryRoot subSelect = new SubqueryRoot(QueryType.SELECT);
         {
            final ProjectionNode projection2 = new ProjectionNode();
            subSelect.setProjection(projection2);
            projection2.addProjectionVar(new VarNode("*"));
            final JoinGroupNode whereClause2 = new JoinGroupNode();
            subSelect.setWhereClause(whereClause2);
            whereClause2.addChild(new StatementPatternNode(new VarNode("s"),
                  new ConstantNode(p), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));
         }

         whereClause1.addChild(subSelect);

         whereClause1.addChild(new StatementPatternNode(new VarNode("s"),
               new ConstantNode(q), new VarNode("x"), null/* c */,
               Scope.DEFAULT_CONTEXTS));

      }

      // The expected AST after the rewrite.
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {

         final ProjectionNode projection1 = new ProjectionNode();
         expected.setProjection(projection1);
         projection1.addProjectionVar(new VarNode("s"));
         projection1.addProjectionVar(new VarNode("o"));
         projection1.addProjectionVar(new VarNode("x"));

         final JoinGroupNode whereClause1 = new JoinGroupNode();
         expected.setWhereClause(whereClause1);

         final SubqueryRoot subSelect = new SubqueryRoot(QueryType.SELECT);
         {
            final ProjectionNode projection2 = new ProjectionNode();
            subSelect.setProjection(projection2);
            projection2.addProjectionVar(new VarNode("s"));
            projection2.addProjectionVar(new VarNode("o"));
            final JoinGroupNode whereClause2 = new JoinGroupNode();
            subSelect.setWhereClause(whereClause2);
            whereClause2.addChild(new StatementPatternNode(new VarNode("s"),
                  new ConstantNode(p), new VarNode("o"), null/* c */,
                  Scope.DEFAULT_CONTEXTS));
         }

         whereClause1.addChild(subSelect);

         whereClause1.addChild(new StatementPatternNode(new VarNode("s"),
               new ConstantNode(q), new VarNode("x"), null/* c */,
               Scope.DEFAULT_CONTEXTS));

      }

      final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);

      final IQueryNode actual = rewriter.optimize(context,
            new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

      assertSameAST(expected, actual);

   }

   /**
    * Given the query:
    * 
    * <pre>
    * SELECT (COUNT(*) as ?c) {
    *   SELECT * {
    *     SELECT * { ?s ?p ?o }
    *   } LIMIT 21 OFFSET 0
    * }
    * </pre>
    * 
    * We obtain the following AST:
    * 
    * <pre>
    * QueryType: SELECT
    * includeInferred=true
    * SELECT ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(*))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(*)] AS VarNode(c) )
    *   JoinGroupNode {
    *     JoinGroupNode {
    *       QueryType: SELECT
    *       SELECT * 
    *         JoinGroupNode {
    *           JoinGroupNode {
    *             QueryType: SELECT
    *             SELECT * 
    *               JoinGroupNode {
    *                 StatementPatternNode(VarNode(s), VarNode(p), VarNode(o)) [scope=DEFAULT_CONTEXTS]
    *               }
    *             
    *           }
    *         }
    *       slice(limit=21)
    *       
    *     }
    *   }
    * </pre>
    * 
    * This is the current AST as rewritten:
    * 
    * <pre>
    * QueryType: SELECT
    * includeInferred=true
    * SELECT ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(*))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(*)] AS VarNode(c) )
    *   JoinGroupNode {
    *     JoinGroupNode {
    *       QueryType: SELECT
    *       SELECT * #  ==> SELECT ?s ?p ?o
    *         JoinGroupNode {
    *           JoinGroupNode {
    *             QueryType: SELECT
    *             SELECT * #  ==> SELECT ?s ?p ?o
    *               JoinGroupNode {
    *                 StatementPatternNode(VarNode(s), VarNode(p), VarNode(o)) [scope=DEFAULT_CONTEXTS]
    *               }
    *             
    *           }
    *         }
    *       slice(limit=21)
    *       
    *     }
    *   }
    * </pre>
    * 
    * @see <a href="http://trac.bigdata.com/ticket/757" > Wildcard projection
    *      was not rewritten. </a>
    * @see <a href="http://trac.bigdata.com/ticket/1210" >
    *      BOpUtility.postOrderIteratorWithAnnotations() is has wrong visitation
    *      order. </a>
    */
   public void test_wildcardProjectionOptimizer03() {

      /*
     * Note: DO NOT share structures in this test!!!!
     */
      final IBindingSet[] bsets = new IBindingSet[] {};

      /**
       * The source AST.
       * 
       * <pre>
       * QueryType: SELECT
       * includeInferred=true
       * SELECT ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(*))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(*)] AS VarNode(c) )
       *   JoinGroupNode {
       *     JoinGroupNode {
       *       QueryType: SELECT // [sliceQuery]
       *       SELECT * 
       *         JoinGroupNode {
       *           JoinGroupNode {
       *             QueryType: SELECT // [selectQuery]
       *             SELECT *
       *               JoinGroupNode {
       *                 StatementPatternNode(VarNode(s), VarNode(p), VarNode(o)) [scope=DEFAULT_CONTEXTS]
       *               }
       *             
       *           }
       *         }
       *       slice(limit=21)
       *     }
       *   }
       * </pre>
       */
      final QueryRoot given = new QueryRoot(QueryType.SELECT);
      {

         /**
          * Top level query.
          * 
          * <pre>
          * QueryType: SELECT
          * includeInferred=true
          * SELECT ( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(*))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#count, valueExpr=com.bigdata.bop.rdf.aggregate.COUNT(*)] AS VarNode(c) )
          *   JoinGroupNode {
          *     JoinGroupNode {
          *       // [sliceQuery]
          *          // [selectQuery]
          *     }
          *   }
          * </pre>
          */
         final SubqueryRoot sliceQuery = new SubqueryRoot(QueryType.SELECT); // subquery
         final SubqueryRoot selectQuery = new SubqueryRoot(QueryType.SELECT); // inner most subquery
          {
             given.setIncludeInferred(true);          

             final FunctionNode countNode = new FunctionNode(
                     FunctionRegistry.COUNT,
                     Collections.<String, Object> emptyMap(),
                     new VarNode("*"));
   
             final ProjectionNode countProjection = new ProjectionNode();
             given.setProjection(countProjection);
             countProjection.addProjectionExpression(new AssignmentNode(new VarNode("c"), countNode));
   
             final JoinGroupNode whereClause = new JoinGroupNode();
             given.setWhereClause(whereClause);

             final JoinGroupNode childJoinGroup = new JoinGroupNode();
             whereClause.addChild(childJoinGroup);
             childJoinGroup.addChild(sliceQuery);
          }

          /**
          * <pre>
          *       QueryType: SELECT // [sliceQuery]
          *       SELECT * 
          *         JoinGroupNode {
          *           JoinGroupNode {
          *             // [selectQuery]
          *           }
          *         }
          *       slice(limit=21)
          * </pre>
          */
          {
              final ProjectionNode p = new ProjectionNode();
              sliceQuery.setProjection(p);
              p.addProjectionVar(new VarNode("*"));
              
              final JoinGroupNode whereClause = new JoinGroupNode();
              sliceQuery.setWhereClause(whereClause);

              final JoinGroupNode childJoinGroup = new JoinGroupNode();
              whereClause.addChild(childJoinGroup);
              childJoinGroup.addChild(selectQuery); // Doubly nested.

              sliceQuery.setSlice(new SliceNode(0L/* offset */, 21L/* limit */));
          }

         /**
          * <pre>
          *             QueryType: SELECT // selectQuery
          *             SELECT * 
          *               JoinGroupNode {
          *                 StatementPatternNode(VarNode(s), VarNode(p), VarNode(o)) [scope=DEFAULT_CONTEXTS]
          *               }
          * </pre>
          */
          {
              final ProjectionNode p = new ProjectionNode();
              selectQuery.setProjection(p);
              p.addProjectionVar(new VarNode("*"));

              final JoinGroupNode whereClause = new JoinGroupNode();
              selectQuery.setWhereClause(whereClause);
              whereClause.addChild(new StatementPatternNode(new VarNode("s"), new VarNode("p"), new VarNode("o"), null, Scope.DEFAULT_CONTEXTS));
          }

      }
//System.err.println("given AST::\n"+given.toString());
      final QueryRoot expected = new QueryRoot(QueryType.SELECT);
      {
         /*
          * Top level query.
          */
         final SubqueryRoot sliceQuery = new SubqueryRoot(QueryType.SELECT); // subquery
         final SubqueryRoot selectQuery = new SubqueryRoot(QueryType.SELECT); // inner most subquery
          {
             expected.setIncludeInferred(true);          

             final FunctionNode countNode = new FunctionNode(
                     FunctionRegistry.COUNT,
                     Collections.<String, Object> emptyMap(),
                     new VarNode("*"));
   
             final ProjectionNode countProjection = new ProjectionNode();
             expected.setProjection(countProjection);
             countProjection.addProjectionExpression(new AssignmentNode(new VarNode("c"), countNode));
   
             final JoinGroupNode whereClause = new JoinGroupNode();
             expected.setWhereClause(whereClause);

             final JoinGroupNode childJoinGroup = new JoinGroupNode();
             whereClause.addChild(childJoinGroup);
             childJoinGroup.addChild(sliceQuery);
          }

         /*
          * [sliceQuery]
          */
          {
              final ProjectionNode p = new ProjectionNode();
              sliceQuery.setProjection(p);
//              p.addProjectionVar(new VarNode("*"));
              p.addProjectionVar(new VarNode("s"));
              p.addProjectionVar(new VarNode("p"));
              p.addProjectionVar(new VarNode("o"));
              
              final JoinGroupNode whereClause = new JoinGroupNode();
              sliceQuery.setWhereClause(whereClause);

              final JoinGroupNode childJoinGroup = new JoinGroupNode();
              whereClause.addChild(childJoinGroup);
              childJoinGroup.addChild(selectQuery); // Doubly nested.

              sliceQuery.setSlice(new SliceNode(0L/* offset */, 21L/* limit */));
          }

         /*
          * [selectQuery]
          */
          {
              final ProjectionNode p = new ProjectionNode();
              selectQuery.setProjection(p);
//              p.addProjectionVar(new VarNode("*"));
              p.addProjectionVar(new VarNode("s"));
              p.addProjectionVar(new VarNode("p"));
              p.addProjectionVar(new VarNode("o"));

              final JoinGroupNode whereClause = new JoinGroupNode();
              selectQuery.setWhereClause(whereClause);
              whereClause.addChild(new StatementPatternNode(new VarNode("s"), new VarNode("p"), new VarNode("o"), null, Scope.DEFAULT_CONTEXTS));
          }
      }

      final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
      final AST2BOpContext context = 
            new AST2BOpContext(new ASTContainer(given), store);

      final IQueryNode actual = rewriter.optimize(context,
            new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

      assertSameAST(expected, actual);

  }
   
}
