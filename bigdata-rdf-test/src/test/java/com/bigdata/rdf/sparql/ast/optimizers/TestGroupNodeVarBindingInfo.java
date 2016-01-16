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
 * Created on June 19, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfo;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;



/**
 * Test suite for the GroupNodeVarBindingInfo class.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGroupNodeVarBindingInfo extends AbstractOptimizerTestCaseWithUtilityMethods  {
   
   public TestGroupNodeVarBindingInfo() {
   }

   public TestGroupNodeVarBindingInfo(String name) {
      super(name);
   }

   @Override
   IASTOptimizer newOptimizer() {
      
      /**
       * This test case is not optimizer specific, but we want to reuse the
       * Helper factory methods here and that's the (only) reason why we
       * extend the AbstractOptimizerTestCase class.
       */
      throw new RuntimeException(
         "Not optimizer specific, don't call this method.");
   }
   
   
   /**
    * Test interface implementation for statement patterns nodes.
    */
   public void testConstructionAndUtilities() {

      final StatementPatternNode spn = 
         (StatementPatternNode) new Helper(){{
            tmp = statementPatternNode(varNode("x"),constantNode(b),constantNode(c));
         }}.getTmp();

      final StatementPatternNode spnOpt = 
         (StatementPatternNode) new Helper(){{
            tmp = statementPatternNode(varNode("y"),constantNode(b),constantNode(c));
         }}.getTmp();
      spnOpt.setOptional(true);

      final UnionNode unionNode = 
         (UnionNode) new Helper(){{
            tmp = unionNode(
               joinGroupNode(statementPatternNode(varNode("w"),constantNode(c),varNode("z"))),
               joinGroupNode(bind(varNode("req"), varNode("z"))));                  
         }}.getTmp();
  
       final FilterNode fn = (FilterNode) new Helper(){{
          tmp = 
             filter(FunctionNode.EQ(constantNode(w), varNode("filterVar")));
         }}.getTmp();         
         
      final JoinGroupNode jgn = 
         (JoinGroupNode) new Helper(){{
            tmp = 
               joinGroupNode(spn, spnOpt, unionNode, fn);
            }}.getTmp();
      
       final QueryRoot query = (QueryRoot) new Helper(){{
          tmp = select(varNodes(x,y,z), jgn);
       }}.getTmp();
   
       final AST2BOpContext context = 
             new AST2BOpContext(new ASTContainer(query), store);
       final StaticAnalysis sa = new StaticAnalysis(query, context); 
       final GlobalAnnotations globals = 
             new GlobalAnnotations(
                context.getLexiconNamespace(), context.getTimestamp());
          AST2BOpUtility.toVE(getBOpContext(), globals, fn.getValueExpressionNode());
          
       final GroupNodeVarBindingInfo biSpn = 
          new GroupNodeVarBindingInfo(spn, sa,null);     
       assertEquals(varSet("x"), biSpn.getDefinitelyProduced());
       assertEquals(varSet("x"), biSpn.getDesiredBound());
       assertEquals(varSet(), biSpn.getRequiredBound());
       assertEquals(varSet("x"), biSpn.getMaybeProduced());
       assertEquals(varSet(), biSpn.leftToBeBound(varSet()));
       assertEquals(varSet(), biSpn.leftToBeBound(varSet("x")));
       assertEquals(spn, biSpn.getNode());
       

       final GroupNodeVarBindingInfo biSpnOpt = 
          new GroupNodeVarBindingInfo(spnOpt, sa, null);
       assertEquals(varSet(), biSpnOpt.getDefinitelyProduced());
       assertEquals(varSet("y"), biSpnOpt.getDesiredBound());
       assertEquals(varSet(), biSpnOpt.getRequiredBound());
       assertEquals(varSet("y"), biSpnOpt.getMaybeProduced());
       assertEquals(varSet(), biSpnOpt.leftToBeBound(varSet()));
       assertEquals(varSet(), biSpnOpt.leftToBeBound(varSet("x")));
       assertEquals(spnOpt, biSpnOpt.getNode());       

       final GroupNodeVarBindingInfo biUnionNode = 
          new GroupNodeVarBindingInfo(unionNode, sa, null);
       assertEquals(varSet(), biUnionNode.getDefinitelyProduced());
       assertEquals(varSet("w","z"), biUnionNode.getDesiredBound());
       assertEquals(varSet("req"), biUnionNode.getRequiredBound());
       assertEquals(varSet("w","z"), biUnionNode.getMaybeProduced());
       assertEquals(varSet("req"), biUnionNode.leftToBeBound(varSet()));
       assertEquals(varSet(), biUnionNode.leftToBeBound(varSet("req")));
       assertEquals(unionNode, biUnionNode.getNode());
       
       final GroupNodeVarBindingInfo biFn = 
          new GroupNodeVarBindingInfo(fn, sa, null);
       assertEquals(varSet(), biFn.getDefinitelyProduced());
       assertEquals(varSet(), biFn.getDesiredBound());
       assertEquals(varSet("filterVar"), biFn.getRequiredBound());
       assertEquals(varSet(), biFn.getMaybeProduced());
       assertEquals(varSet("filterVar"), biFn.leftToBeBound(varSet()));
       assertEquals(varSet("filterVar"), biFn.leftToBeBound(varSet("x")));
       assertEquals(varSet(), biFn.leftToBeBound(varSet("y","filterVar")));
       assertEquals(fn, biFn.getNode());
       
       final GroupNodeVarBindingInfo biJgn = 
          new GroupNodeVarBindingInfo(jgn, sa, null);
       assertEquals(varSet("x"), biJgn.getDefinitelyProduced());
       assertEquals(varSet("x", "y", "z", "w"), biJgn.getDesiredBound());
       assertEquals(varSet("filterVar", "req"), biJgn.getRequiredBound());
       assertEquals(varSet("x", "y", "z", "w"), biJgn.getMaybeProduced());
       assertEquals(varSet("filterVar", "req"), biJgn.leftToBeBound(varSet()));
       assertEquals(varSet("filterVar"), biJgn.leftToBeBound(varSet("x","req")));
       assertEquals(jgn, biJgn.getNode());
   }

   
}
