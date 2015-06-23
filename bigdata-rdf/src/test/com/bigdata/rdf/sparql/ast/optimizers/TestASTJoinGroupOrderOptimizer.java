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
 * Created on June 16, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;



/**
 * Test suite for the {@link ASTJoinGroupOrderOptimizer}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestASTJoinGroupOrderOptimizer extends AbstractOptimizerTestCaseWithUtilityMethods {

   public TestASTJoinGroupOrderOptimizer() {
   }

   public TestASTJoinGroupOrderOptimizer(String name) {
       super(name);
   }
   
   @Override
   IASTOptimizer newOptimizer() {
      return new ASTOptimizerList(new ASTJoinGroupOrderOptimizer());
   }

   public void testFilterPlacement01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               filterWithVars("x1","x2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               filterWithVars("x1","x2"),
               stmtPatternWithVar("x3")
               ));
         
      }}.test();
   }
   
   public void testFilterPlacement02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               joinGroupWithVars("x1"),
               joinGroupWithVars("x2"),
               joinGroupWithVars("x1","x3"),
               filterWithVar("x4"),
               filterWithVar("x1"),
               filterExistsWithVars("x1","x2"),
               filterNotExistsWithVars("x3")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               joinGroupWithVars("x1"),
               filterExistsWithVars("x1","x2"),
               filterWithVar("x1"),
               joinGroupWithVars("x2"),
               joinGroupWithVars("x1","x3"),
               filterNotExistsWithVars("x3"),
               filterWithVar("x4")
            ));
         
      }}.test();
   }
   
   public void testFilterPlacement03() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               joinGroupNode(
                  stmtPatternWithVar("x2"),
                  filterWithVar("x1")
               )
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
                  joinGroupNode(
                  filterWithVar("x1"),
                  stmtPatternWithVar("x2")
               )
            ));

         
      }}.test();
   }
   
   /**
    * Test filter placement where one filter variables is bound in the first,
    * one in the join group
    */
   public void testFilterPlacement04() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVar("x2"),               
               stmtPatternWithVars("x2","x3"),
               stmtPatternWithVarOptional("y2"),
               filterWithVars("x1","x2"),
               filterWithVar("y2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVar("x2"),               
               filterWithVars("x1","x2"),
               stmtPatternWithVars("x2","x3"),
               stmtPatternWithVarOptional("y2"),
               filterWithVar("y2")
            ));
         
      }}.test();
   }

   public void testBindPlacement01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               assignmentWithConst("x1"),
               assignmentWithConst("x4"),
               assignmentWithConst("x2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               assignmentWithConst("x1"),
               stmtPatternWithVar("x1"),
               assignmentWithConst("x2"),
               stmtPatternWithVar("x2"),
               assignmentWithConst("x4")
           ));
         
      }}.test();
   } 
   
   public void testBindPlacement02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               assignmentWithVar("x2","x1"),
               assignmentWithVar("x2","x3")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               assignmentWithVar("x2","x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               assignmentWithVar("x2","x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   /**
    * Test complex pattern, including inter- and intra-partition reordering,
    * with focus on BIND and ASSIGNMENT nodes.
    */
   public void testBindPlacement03() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("y1"),
               stmtPatternWithVar("y1"),
               stmtPatternWithVars("y1","z1"),
               assignmentWithVar("z1","y1")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("y1"),
               stmtPatternWithVar("y1"),
               assignmentWithVar("z1","y1"),
               stmtPatternWithVars("y1","z1")
           ));
         
      }}.test();
      
   }   
   
   public void testValuesPlacement01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               bindingsClauseWithVars("x1"),
               bindingsClauseWithVars("x1","x3"),
               stmtPatternWithVar("x3"),
               bindingsClauseWithVars("x2"),
               stmtPatternWithVar("x4")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               bindingsClauseWithVars("x1"),
               bindingsClauseWithVars("x1","x3"),
               stmtPatternWithVar("x1"),
               bindingsClauseWithVars("x2"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   }    
   
   public void testServicePlacementSparql11a() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               serviceSparql11WithVariable("x1", "x2"),
               serviceSparql11WithVariable("x3", "x4", "x5")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               serviceSparql11WithVariable("x1", "x2"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               serviceSparql11WithVariable("x3", "x4", "x5"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparql11b() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               serviceSparql11WithVariable("x1", "x2"),
               serviceSparql11WithVariable("x2", "x1", "x3")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               serviceSparql11WithVariable("x1", "x2"),
               serviceSparql11WithVariable("x2", "x1", "x3"), /* x2 bound by previous service! */
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparqlBDS() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               serviceBDSWithVariable("x2")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               serviceBDSWithVariable("x2"), /* first possible position */
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparqlFTS01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("inParams"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("inParams"),
               serviceFTSWithVariable(
                     "outRes", "outScore", "outSnippet", 
                     "inSearch", "inEndpoint", "inParams"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementSparqlFTS02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams2"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams"), 
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("inParams"),
               stmtPatternWithVar("x"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("inParams"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams"),
               stmtPatternWithVar("x"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams2")
           ));
         
      }}.test();
   } 
   
   public void testPlacementInContextOfUnions() {
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               unionWithVars("x1","x2"),
               unionWithVars("x1", "x1", "x2"),
               unionWithVars("x3","x3"),
               unionWithVars("bound","x3"),
               assignmentWithVar("bound", "x1")
            ));
         
         /**
          * Only the second UNION expression bounds x1 for sure. The BIND node
          * is placed at the first "useful" position after that one.
          */
         expected = 
            select(varNode(x), 
            where (
               unionWithVars("x1","x2"),
               unionWithVars("x1", "x1", "x2"), /* { ... x1 ... } UNION { ... x1 ... x2 } */
               unionWithVars("x3","x3"),
               assignmentWithVar("bound", "x1"),
               unionWithVars("bound","x3")
           ));
         
      }}.test();   
      
   }

   public void testPlacementInContextOfSubquery() {
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               assignmentWithVar("boundVar", "x3"),
               subqueryWithVars("x1", "x2"),
               subqueryWithVars("x2", "x3"),
               subqueryWithVars("x3", "x4"),
               subqueryWithVars("x4", "boundVar")
            ));
         
         expected = 
           select(varNode(x), 
           where (
               subqueryWithVars("x1", "x2"),
               subqueryWithVars("x2", "x3"),
               subqueryWithVars("x3", "x4"),
               assignmentWithVar("boundVar", "x3"),
               subqueryWithVars("x4", "boundVar")
            ));

         
      }}.test();   
   }

   public void testPlacementInContextOfNamedSubquery() {
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            namedSubQuery(
               "_set1",
               varNode("x1"),
               where(statementPatternNode(varNode("x1"), varNode("inner"), constantNode(b)))
            ),
            where (
               assignmentWithVar("boundVar", "x1"),
               namedSubQueryInclude("_set1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("boundVar")
            ));
         
         expected = 
            select(varNode(x), 
               namedSubQuery(
                  "_set1",
                  varNode("x1"),
                  where(statementPatternNode(varNode("x1"), varNode("inner"), constantNode(b)))
               ),
               where (
                  namedSubQueryInclude("_set1"),
                  stmtPatternWithVar("x2"),
                  assignmentWithVar("boundVar", "x1"),
                  stmtPatternWithVar("boundVar")
               ));

         
      }}.test();   
   }

   public void testPlacementInContextOfOptional() {
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVarOptional("x1"),
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("bound"),
               assignmentWithVar("bound", "x1")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVarOptional("x1"),
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("x2"),
               stmtPatternWithVarOptional("bound"),
               assignmentWithVar("bound", "x1")
           ));
         
      }}.test();   
   }

   /**
    * Test complex pattern, including inter- and intra-partition reordering,
    * excluding BIND and ASSIGNMENT nodes.
    */
   public void testComplexOptimization01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               joinGroupWithVars("x1","x2"),
               alpNodeWithVars("x1","x2"),
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("y1"),
               unionWithVars("y2","y1","y2"), 
               stmtPatternWithVar("y1"),
               subqueryWithVars("y1","y4","y5"),
               stmtPatternWithVarOptional("z1"),
               joinGroupWithVars("a1","a2"),
               joinGroupWithVars("z1","z2"),
               filterWithVar("x1"),
               filterWithVars("x1","y1"),
               filterWithVars("x1","z1")               
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               filterWithVar("x1"),
               joinGroupWithVars("x1","x2"),
               joinGroupWithVars("a1","a2"), /* can be moved to the front */
               alpNodeWithVars("x1","x2"),
               stmtPatternWithVarOptional("y1"),
               stmtPatternWithVar("y1"),
               filterWithVars("x1","y1"),
               unionWithVars("y2","y1","y2"), 
               subqueryWithVars("y1","y4","y5"),
               stmtPatternWithVarOptional("z1"),
               joinGroupWithVars("z1","z2"),
               filterWithVars("x1","z1")      
           ));
         
      }}.test();
      
   }
   
   /**
    * Test complex pattern, including inter- and intra-partition reordering,
    * with focus on BIND and ASSIGNMENT nodes.
    */
   public void testComplexOptimization02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               alpNodeWithVars("x1","x2"),
               joinGroupWithVars("x1","x2"),
               stmtPatternWithVar("x1"),
               stmtPatternWithVarOptional("y1"),
               unionWithVars("y2","y1","y2"), 
               subqueryWithVars("y1","y4","y5"),
               stmtPatternWithVar("y1"),
               bindingsClauseWithVars("x1","z1"),
               assignmentWithVar("bound","y1")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               bindingsClauseWithVars("x1","z1"),
               stmtPatternWithVar("x1"),
               joinGroupWithVars("x1","x2"),
               alpNodeWithVars("x1","x2"),
               stmtPatternWithVarOptional("y1"),
               stmtPatternWithVar("y1"),
               unionWithVars("y2","y1","y2"), 
               subqueryWithVars("y1","y4","y5"),
               assignmentWithVar("bound","y1")
           ));
         
      }}.test();
      
   }   

   /**
    * Test placement of named subquery at the beginning of the associated
    * partition.
    */
   public void testNamedSubqueryPlacement01() {
      
      new Helper(){{
         
         given = 
            select(
               varNode(x), 
               namedSubQuery("_set1",varNode("y1"),where(statementPatternNode(varNode("y1"), constantNode(a), constantNode(b),1))),
               where (
                  stmtPatternWithVar("x1"),
                  stmtPatternWithVarOptional("y1"),
                  stmtPatternWithVar("y1"),
                  namedSubQueryInclude("_set1")
            ));
         
         expected = 
            select(
               varNode(x), 
               namedSubQuery("_set1",varNode("y1"),where(statementPatternNode(varNode("y1"), constantNode(a), constantNode(b),1))),
               where (
                  stmtPatternWithVar("x1"),
                  stmtPatternWithVarOptional("y1"),
                  namedSubQueryInclude("_set1"),
                  stmtPatternWithVar("y1")
           ));
         
      }}.test();

   }
   
   /**
    * Test placement of named subquery at the beginning of the previous
    * partition (where intra-partition optimization is possible).
    */
   public void testNamedSubqueryPlacement02() {
      
      new Helper(){{
         
         given = 
            select(
               varNode(x), 
               namedSubQuery("_set1",varNode("x1"),where(statementPatternNode(varNode("x1"), constantNode(a), constantNode(b),1))),
               where (
                  stmtPatternWithVar("x1"),
                  stmtPatternWithVarOptional("y1"),
                  stmtPatternWithVar("y1"),
                  namedSubQueryInclude("_set1")
            ));
         
         expected = 
            select(
               varNode(x), 
               namedSubQuery("_set1",varNode("x1"),where(statementPatternNode(varNode("x1"), constantNode(a), constantNode(b),1))),
               where (
                  namedSubQueryInclude("_set1"),
                  stmtPatternWithVar("x1"),
                  stmtPatternWithVarOptional("y1"),
                  stmtPatternWithVar("y1")
           ));
         
      }}.test();

   }   
   
   /**
//    * Test case for ASK subqueries, as they emerge from FILTER (NOT) EXISTS
//    * clauses.
//    */
//   public void testAskSubquery01() {
//      throw new NotImplementedException();
//   }
//   
//   /**
//    * Test case for ASK subqueries, as they emerge from FILTER (NOT) EXISTS
//    * clauses. This test case focuses on the interaction with simple FILTERs.
//    */
//   public void testAskSubquery02() {
//      throw new NotImplementedException();
//   }

//given = 
//select(varNode(x), 
//where (
// filterExistsWithVars("x6","x7"),
// filterExistsWithVars("x8","x9","x10"),
// joinGroupWithVars("x11","x12"),
// serviceSparql11WithConstant("x13","x14"),
// serviceSparql11WithVariable("x15","x16","x17"),
// serviceBDSWithVariable("x18"),
// serviceFTSWithVariable("x19","x20","x21","x22"),
// alpNodeWithVars("x23","x24","x25"),
// unionWithVars("x26","x27"),
// assignmentWithVar("x28","x29"),
// bindingsClauseWithVars("x32","x33","x34"),
// subqueryWithVars("x35","x36")
//));
}