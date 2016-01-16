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
 * Created on June 16, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.UnionNode;



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
               filterWithVar("x4"), /* can't be bound anyway */
               joinGroupWithVars("x1"),
               filterExistsWithVars("x1","x2"),
               filterWithVar("x1"),
               joinGroupWithVars("x2"),
               joinGroupWithVars("x1","x3"),
               filterNotExistsWithVars("x3")
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
         
      }}.testWhileIgnoringExplainHints();
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
         
      }}.testWhileIgnoringExplainHints();
      
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
   
   /**
    * VALUES claused placed early whenever it binds values that are reused
    * by other nodes such as BINDs. Motivated by the scenario discussed in
    * https://jira.blazegraph.com/browse/BLZG-1463.
    */
   public void testValuesPlacement02() {

      new Helper(){{
         
         given = 
            select(varNode(s), 
            where (
               stmtPatternWithVars("s","o"),
               assignmentWithVar("o","reused"),
               bindingsClauseWithVars("reused")
            ));
         
         expected = 
            select(varNode(s), 
            where (
               bindingsClauseWithVars("reused"),
               assignmentWithVar("o","reused"),
               stmtPatternWithVars("s","o")
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
   
   public void testServicePlacementSparql11c() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               serviceSparql11WithConstant("x1")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4"),
               serviceSparql11WithConstant("x1") /* SPARQL 1.1 service is placed as late as possible */
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementServiceBDS() {

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
               serviceBDSWithVariable("x2"), /* BDS service is placed as early as possible */
               stmtPatternWithVar("x1"),
               stmtPatternWithVar("x2"),
               stmtPatternWithVar("x3"),
               stmtPatternWithVar("x4")
           ));
         
      }}.test();
   } 
   
   public void testServicePlacementServiceFTS01() {

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
   
   public void testServicePlacementServiceFTS02() {


      final JoinGroupNode jgnOpt = joinGroupWithVars("inParams2","z");
      jgnOpt.setOptional(true);
      
      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams"), 
               stmtPatternWithVar("inSearch"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("inParams"),
               stmtPatternWithVar("x"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               jgnOpt,
               stmtPatternWithVar("z"),
               serviceFTSWithVariable(
                  "outRes", "outScore", "outSnippet", 
                  "inSearch", "inEndpoint", "inParams2")
                
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
               jgnOpt,
               serviceFTSWithVariable( /* inParams2 can't be bound at a later time */
                     "outRes", "outScore", "outSnippet", 
                     "inSearch", "inEndpoint", "inParams2"),
               stmtPatternWithVar("z")
           ));
         
      }}.testWhileIgnoringExplainHints();
   } 
   
   /**
    * Interaction of BIND/SPARQL SERVICE keyword.
    */
   public void testServiceBindDependencyOrdering() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("X"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("inParams"),
               serviceFTSWithVariable(
                     "outRes", "outScore", "outSnippet", 
                     "inSearch", "inEndpoint", "inParams"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               assignmentWithVar("inSearch", "X"),
               assignmentWithVar("dummy", "Y")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("X"),
               stmtPatternWithVar("inEndpoint"),
               stmtPatternWithVar("inParams"),
               stmtPatternWithVar("outRes"),
               stmtPatternWithVar("outScore"),
               stmtPatternWithVar("outSnippet"),
               assignmentWithVar("inSearch", "X"),
               serviceFTSWithVariable(
                     "outRes", "outScore", "outSnippet", 
                     "inSearch", "inEndpoint", "inParams"),
               assignmentWithVar("dummy", "Y")
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
         
      }}.testWhileIgnoringExplainHints();   
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
         
      }}.testWhileIgnoringExplainHints();
      
   }
   
   /**
    * Test OPTIONAL inter-partition reordering.
    */
   public void testOptional01() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVarsOptional("x1", "y1"),
               stmtPatternWithVars("x1", "z1")          
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVar("x1"),
               stmtPatternWithVars("x1", "z1"),  
               stmtPatternWithVarsOptional("x1", "y1")      
           ));
         
      }}.test();
      
   }
   
   /**
    * Test OPTIONAL inter-partition reordering.
    */
   public void testOptional02() {

      new Helper(){{
         
         given = 
            select(varNode(x), 
            where (
               stmtPatternWithVarsOptional("x1", "y1"),
               stmtPatternWithVar("x1")
            ));
         
         expected = 
            select(varNode(x), 
            where (
               stmtPatternWithVarsOptional("x1", "y1"),
               stmtPatternWithVar("x1")
           ));
         
      }}.testWhileIgnoringExplainHints();
      
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
         
      }}.testWhileIgnoringExplainHints();
      
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
         
      }}.testWhileIgnoringExplainHints();

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
         
      }}.testWhileIgnoringExplainHints();

   }   
   
   /**
    * Test case for ASK subqueries, as they emerge from FILTER (NOT) EXISTS
    * clauses.
    */
   public void testAskSubquery01() {
      
      final String anonFilterVar1 = "--exists-1";
      final String[] filterVars1 = new String[] { "x1" };
      final String anonFilterVar2 = "--not-exists-1";
      final String[] filterVars2 = new String[] { "y1", "y2", "y3" };
      
      new Helper(){{
         
         given = 
            select(
               varNode(x), 
               where (
                  filterExistsWithVars(anonFilterVar1, filterVars1),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar1, filterVars1),
                  filterExistsWithVars(anonFilterVar2, filterVars2),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar2, filterVars2),
                  stmtPatternWithVar("x1"),
                  stmtPatternWithVar("y1"),
                  stmtPatternWithVar("y2"),
                  stmtPatternWithVar("y3")
            ));
         
         expected = 
            select(
               varNode(x), 
               where (
                  stmtPatternWithVar("x1"),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar1, filterVars1),
                  filterExistsWithVars(anonFilterVar1, filterVars1),
                  stmtPatternWithVar("y1"),
                  stmtPatternWithVar("y2"),
                  stmtPatternWithVar("y3"),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar2, filterVars2),
                  filterExistsWithVars(anonFilterVar2, filterVars2)
           ));
         
      }}.test();
      
   }
   
   /**
    * Test case for ASK subqueries, as they emerge from FILTER (NOT) EXISTS
    * clauses. This test case focuses on the placement in the context of
    * OPTIONAL patterns.
    */
   public void testAskSubquery02() {
      
      
      final String anonFilterVar1 = "--exists-1";
      final String[] filterVars1 = new String[] { "x1" };
      final String anonFilterVar2 = "--not-exists-1";
      final String[] filterVars2 = new String[] { "y1", "y2", "y3" };

      final JoinGroupNode optJG = joinGroupWithVars("y1","y3");
      optJG.setOptional(true);
      
      new Helper(){{
         
         given = 
            select(
               varNode(x), 
               where (
                  filterExistsWithVars(anonFilterVar1, filterVars1),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar1, filterVars1),
                  filterExistsWithVars(anonFilterVar2, filterVars2),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar2, filterVars2),
                  stmtPatternWithVar("x1"),
                  stmtPatternWithVarOptional("y1"),
                  joinGroupWithVars("y1","y2"),
                  optJG,
                  joinGroupWithVars("y1","y3"),
                  joinGroupWithVars("y2","y3")
            ));
         
         expected = 
            select(
               varNode(x), 
               where (
                  stmtPatternWithVar("x1"),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar1, filterVars1),                  
                  filterExistsWithVars(anonFilterVar1, filterVars1),
                  stmtPatternWithVarOptional("y1"),
                  joinGroupWithVars("y1","y2"),
                  optJG,
                  joinGroupWithVars("y1","y3"),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar2, filterVars2),
                  filterExistsWithVars(anonFilterVar2, filterVars2),
                  joinGroupWithVars("y2","y3")
           ));
         
      }}.testWhileIgnoringExplainHints();      
   }

   /**
    * Test case for ASK subqueries, as they emerge from FILTER (NOT) EXISTS
    * clauses. This test case focuses on the interaction with simple FILTERs.
    */
   public void testAskSubquery03() {
      
      final String anonFilterVar1 = "--exists-1";
      final String[] filterVars1 = new String[] { "x1" };
      
      new Helper(){{
         
         given = 
            select(
               varNode(x), 
               where (
                  filterExistsWithVars(anonFilterVar1, filterVars1),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar1, filterVars1),
                  stmtPatternWithVar("x1"),
                  filterWithVar("x1"),
                  stmtPatternWithVar("x2"),
                  stmtPatternWithVar("x3")
            ));
         
         expected = 
            select(
               varNode(x), 
               where (
                  stmtPatternWithVar("x1"),
                  filterWithVar("x1"),
                  filterExistsOrNotExistsSubqueryWithVars(anonFilterVar1, filterVars1),
                  filterExistsWithVars(anonFilterVar1, filterVars1),
                  stmtPatternWithVar("x2"),
                  stmtPatternWithVar("x3")

           ));
         
      }}.test();      
   }
   

   
   /**
    * A UNION node usually has precedence over subqueries.
    */
   public void testTicket1363a() {
      
      final JoinGroupNode jgn1a = new JoinGroupNode();
      final JoinGroupNode jgn1b = new JoinGroupNode();
      jgn1a.addChild(stmtPatternWithVar("y1"));
      jgn1b.addChild(stmtPatternWithVar("y1"));
      
      final JoinGroupNode jgn2a = new JoinGroupNode();
      final JoinGroupNode jgn2b = new JoinGroupNode();
      jgn2a.addChild(stmtPatternWithVar("y2"));
      jgn2b.addChild(stmtPatternWithVar("y2"));
      
      final UnionNode unA = new UnionNode();
      unA.addChild(jgn1a);
      unA.addChild(jgn2a);
      
      final UnionNode unB = new UnionNode();
      unB.addChild(jgn1b);
      unB.addChild(jgn2b);

      
      new Helper(){{
          
         given = 
            select(varNode(x), 
            where (
               unA,
               subqueryWithVars("x1", "x2")
            ));
            
         expected = 
           select(varNode(x), 
           where (
               unB,
               subqueryWithVars("x1", "x2")
            ));

            
      }}.test();   
      
   }
   
   /**
    * In case the UNION node has binding requirements that cannot be satisified
    * internally, it must be evaluated after the subquery.
    */
   public void testTicket1363b() {
      
      final JoinGroupNode jgn1a = new JoinGroupNode();
      final JoinGroupNode jgn1b = new JoinGroupNode();
      jgn1a.addChild(assignmentWithVar("z", "x1"));
      jgn1b.addChild(assignmentWithVar("z", "x1"));
      
      final JoinGroupNode jgn2a = new JoinGroupNode();
      final JoinGroupNode jgn2b = new JoinGroupNode();
      jgn2a.addChild(stmtPatternWithVar("y1"));
      jgn2b.addChild(stmtPatternWithVar("y1"));
      
      final UnionNode unA = new UnionNode();
      unA.addChild(jgn1a);
      unA.addChild(jgn2a);
      
      final UnionNode unB = new UnionNode();
      unB.addChild(jgn1b);
      unB.addChild(jgn2b);

      
      new Helper(){{
          
         given = 
            select(varNode(x), 
            where (
               unA,
               subqueryWithVars("x1", "x2")
            ));
            
         expected = 
           select(varNode(x), 
           where (
               subqueryWithVars("x1", "x2"),
               unB
            ));

            
      }}.test();   
      
   }
   
   /**
    * In the following variant, the union node has binding requirements but
    * can (and does) internally satisfy them.
    */
   public void testTicket1363c() {
      
      final JoinGroupNode jgn1a = new JoinGroupNode();
      jgn1a.addChild(assignmentWithVar("z", "x1"));
      jgn1a.addChild(stmtPatternWithVar("x1"));
      
      // same as a1, but reordered to satisfy binding requirements
      final JoinGroupNode jgn1b = new JoinGroupNode();
      jgn1b.addChild(stmtPatternWithVar("x1"));
      jgn1b.addChild(assignmentWithVar("z", "x1"));
      
      final JoinGroupNode jgn2a = new JoinGroupNode();
      final JoinGroupNode jgn2b = new JoinGroupNode();
      jgn2a.addChild(stmtPatternWithVar("x1"));
      jgn2b.addChild(stmtPatternWithVar("x1"));
      
      final UnionNode unA = new UnionNode();
      unA.addChild(jgn1a);
      unA.addChild(jgn2a);
      
      final UnionNode unB = new UnionNode();
      unB.addChild(jgn1b);
      unB.addChild(jgn2b);

      
      new Helper(){{
          
         given = 
            select(varNode(x), 
            where (
               unA,
               subqueryWithVars("x1", "x2")
            ));
            
         expected = 
           select(varNode(x), 
           where (
               unB,
               subqueryWithVars("x1", "x2")
            ));

            
      }}.test();   
      
   }
}
