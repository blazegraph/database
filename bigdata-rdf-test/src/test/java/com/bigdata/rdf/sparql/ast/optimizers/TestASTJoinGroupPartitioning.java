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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfoMap;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;


/**
 * Test suite for the {@link ASTJoinGroupPartition} and the 
 * {@link ASTJoinGroupPartitions} utility classes.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestASTJoinGroupPartitioning extends AbstractOptimizerTestCaseWithUtilityMethods {

   public TestASTJoinGroupPartitioning() {
   }

   public TestASTJoinGroupPartitioning(String name) {
       super(name);
   }
   
   @Override
   IASTOptimizer newOptimizer() {
      return null;
   }

   /**
    * Test empty partition.
    */
   public void testEmptyPartitions() {
      
      final List<IGroupMemberNode> nodes = new ArrayList<IGroupMemberNode>();
      final GroupNodeVarBindingInfoMap bindingInfo =
         new GroupNodeVarBindingInfoMap(
            nodes, statisAnalysisForNodes(nodes), null);
      final Set<IVariable<?>> external = new HashSet<IVariable<?>>();
      
      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(nodes, bindingInfo, external);
      
      assertTrue(partitions.getPartitionList().size()==1);
      assertTrue(partitions.getPartitionList().get(0).nonOptionalNonMinusNodes.isEmpty());
      assertTrue(partitions.extractNodeList(true).isEmpty());
   }

   /**
    * Test partition with a single triple pattern.
    */
   public void testSinglePartitionWithOneStatementPattern() {
      
      final StatementPatternNode spn = stmtPatternWithVar("x");
      
      final List<IGroupMemberNode> nodes = new ArrayList<IGroupMemberNode>();
      nodes.add(spn);
      
      final GroupNodeVarBindingInfoMap bindingInfo =
         new GroupNodeVarBindingInfoMap(
            nodes, statisAnalysisForNodes(nodes), null);
      
      final Set<IVariable<?>> external = new HashSet<IVariable<?>>();
      external.add(Var.var("y"));
      external.add(Var.var("z"));
      

      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(nodes, bindingInfo, external);

      // checks on partitions object
      assertEquals(1, partitions.getPartitionList().size());
      assertEquals(1, partitions.extractNodeList(true).size());
      assertTrue(partitions.extractNodeList(true).contains(spn));
      
      // checks on partition object
      ASTJoinGroupPartition partition = partitions.getPartitionList().get(0);
      assertEquals(1,partition.extractNodeList(true).size());
      assertTrue(partition.extractNodeList(true).contains(spn));
      assertTrue(partition.externallyBound.equals(external));
      assertEquals(1,partition.nonOptionalNonMinusNodes.size());
      assertTrue(partition.nonOptionalNonMinusNodes.contains(spn));
      assertTrue(partition.optionalOrMinus==null);
      assertEquals(3,partition.definitelyProduced.size());
      assertTrue(partition.definitelyProduced.contains(Var.var("x")));
      assertTrue(partition.definitelyProduced.contains(Var.var("y")));
      assertTrue(partition.definitelyProduced.contains(Var.var("z")));
      
   }
   
   /**
    * Test partition with two statement patterns (both not optional).
    */
   public void testSinglePartitionWithTwoStatementPattern() {
      
      final StatementPatternNode spn1 = stmtPatternWithVar("x1");
      final StatementPatternNode spn2 = stmtPatternWithVar("x2");
      
      final List<IGroupMemberNode> nodes = new ArrayList<IGroupMemberNode>();
      nodes.add(spn1);
      nodes.add(spn2);
      
      final GroupNodeVarBindingInfoMap bindingInfo =
         new GroupNodeVarBindingInfoMap(
            nodes, statisAnalysisForNodes(nodes), null);
      
      final Set<IVariable<?>> external = new HashSet<IVariable<?>>();
      external.add(Var.var("y"));

      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(nodes, bindingInfo, external);

      // checks on partitions object
      assertEquals(1, partitions.getPartitionList().size());
      assertEquals(2, partitions.extractNodeList(true).size());
      assertTrue(partitions.extractNodeList(true).contains(spn1));
      assertTrue(partitions.extractNodeList(true).contains(spn2));
      
      // checks on partition object
      ASTJoinGroupPartition partition = partitions.getPartitionList().get(0);
      assertEquals(2,partition.extractNodeList(true).size());
      assertTrue(partition.extractNodeList(true).contains(spn1));
      assertTrue(partition.extractNodeList(true).contains(spn2));
      assertTrue(partition.externallyBound.equals(external));
      assertEquals(2,partition.nonOptionalNonMinusNodes.size());
      assertTrue(partition.nonOptionalNonMinusNodes.contains(spn1));
      assertTrue(partition.nonOptionalNonMinusNodes.contains(spn2));
      assertNull(partition.optionalOrMinus);
      assertEquals(3,partition.definitelyProduced.size());
      assertTrue(partition.definitelyProduced.contains(Var.var("x1")));
      assertTrue(partition.definitelyProduced.contains(Var.var("x2")));
      assertTrue(partition.definitelyProduced.contains(Var.var("y")));
      
   }
   
   /**
    * Test partition with three statement patterns, where the last one
    * is optional.
    */
   public void testSinglePartitionWithThreeStatementPattern() {
      
      final StatementPatternNode spn1 = stmtPatternWithVar("x1");
      final StatementPatternNode spn2 = stmtPatternWithVar("x2");
      final StatementPatternNode spn3Opt = stmtPatternWithVarOptional("x3");
      
      final List<IGroupMemberNode> nodes = new ArrayList<IGroupMemberNode>();
      nodes.add(spn1);
      nodes.add(spn2);
      nodes.add(spn3Opt);
      
      final GroupNodeVarBindingInfoMap bindingInfo =
         new GroupNodeVarBindingInfoMap(
            nodes, statisAnalysisForNodes(nodes), null);

      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(
            nodes, bindingInfo, new HashSet<IVariable<?>>());

      // checks on partitions object
      assertEquals(1, partitions.getPartitionList().size());
      assertEquals(3, partitions.extractNodeList(true).size());
      assertTrue(partitions.extractNodeList(true).contains(spn1));
      assertTrue(partitions.extractNodeList(true).contains(spn2));
      assertTrue(partitions.extractNodeList(true).contains(spn3Opt));
      
      // checks on partition object
      ASTJoinGroupPartition partition = partitions.getPartitionList().get(0);
      assertEquals(3,partition.extractNodeList(true).size());
      assertTrue(partition.extractNodeList(true).contains(spn1));
      assertTrue(partition.extractNodeList(true).contains(spn2));
      assertEquals(2,partition.nonOptionalNonMinusNodes.size());
      assertTrue(partition.nonOptionalNonMinusNodes.contains(spn1));
      assertTrue(partition.nonOptionalNonMinusNodes.contains(spn2));
      assertTrue(partition.optionalOrMinus.equals(spn3Opt));
      assertEquals(2,partition.definitelyProduced.size());
      assertTrue(partition.definitelyProduced.contains(Var.var("x1")));
      assertTrue(partition.definitelyProduced.contains(Var.var("x2")));
      
   }
 
   
   /**
    * Test multiple partitions (all of which are made out of statement
    * patterns).
    */
   public void testMultiplePartitionsWithStatementPattern() {
      
      final StatementPatternNode spn1 = stmtPatternWithVar("x1");
      final StatementPatternNode spn2 = stmtPatternWithVar("x2");
      final StatementPatternNode spn3Opt = stmtPatternWithVarOptional("x3");
      final StatementPatternNode spn4 = stmtPatternWithVar("x4");
      final StatementPatternNode spn5Opt = stmtPatternWithVarOptional("x5");
      final StatementPatternNode spn6 = stmtPatternWithVar("x6");
      final StatementPatternNode spn7Opt = stmtPatternWithVarOptional("x7");
      final StatementPatternNode spn8 = stmtPatternWithVar("x8");
      
      final List<IGroupMemberNode> nodes = new ArrayList<IGroupMemberNode>();
      nodes.add(spn1);
      nodes.add(spn2);
      nodes.add(spn3Opt);
      nodes.add(spn4);
      nodes.add(spn5Opt);
      nodes.add(spn6);
      nodes.add(spn7Opt);
      nodes.add(spn8);
      
      final GroupNodeVarBindingInfoMap bindingInfo =
         new GroupNodeVarBindingInfoMap(
            nodes, statisAnalysisForNodes(nodes), null);

      final Set<IVariable<?>> external = new HashSet<IVariable<?>>();
      external.add(Var.var("y"));

      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(nodes, bindingInfo, external);

      // checks on partitions object
      assertEquals(4, partitions.getPartitionList().size());
      assertEquals(nodes, partitions.extractNodeList(true));
      
      // checks on partition object
      final ASTJoinGroupPartition p1 = partitions.getPartitionList().get(0);
      final List<IGroupMemberNode> p1Exp = new ArrayList<IGroupMemberNode>();
      p1Exp.add(spn1);
      p1Exp.add(spn2);
      p1Exp.add(spn3Opt);
      assertEquals(p1Exp, p1.extractNodeList(true));
      assertEquals(spn3Opt, p1.optionalOrMinus);
      assertEquals(varSet("y","x1","x2"), p1.definitelyProduced);
      
      final ASTJoinGroupPartition p2 = partitions.getPartitionList().get(1);
      final List<IGroupMemberNode> p2Exp = new ArrayList<IGroupMemberNode>();
      p2Exp.add(spn4);
      p2Exp.add(spn5Opt);
      assertEquals(p2Exp, p2.extractNodeList(true));
      assertEquals(spn5Opt, p2.optionalOrMinus);
      assertEquals(varSet("y","x1","x2","x4"), p2.definitelyProduced);

      
      final ASTJoinGroupPartition p3 = partitions.getPartitionList().get(2);
      final List<IGroupMemberNode> p3Exp = new ArrayList<IGroupMemberNode>();
      p3Exp.add(spn6);
      p3Exp.add(spn7Opt);
      assertEquals(p3Exp, p3.extractNodeList(true));
      assertEquals(spn7Opt, p3.optionalOrMinus);
      assertEquals(varSet("y","x1","x2","x4","x6"), p3.definitelyProduced);


      
      final ASTJoinGroupPartition p4 = partitions.getPartitionList().get(3);
      final List<IGroupMemberNode> p4Exp = new ArrayList<IGroupMemberNode>();
      p4Exp.add(spn8);
      assertEquals(p4Exp, p4.extractNodeList(true));
      assertNull(p4.optionalOrMinus);
      assertEquals(varSet("y","x1","x2","x4","x6","x8"), p4.definitelyProduced);
      
   }
   
   /**
    * Test multiple partitions including constructs other than simple 
    * statement patterns.
    */
   public void testComplexMultiplePartitions() {
      
      final IGroupMemberNode n1 = stmtPatternWithVar("x1");
      final IGroupMemberNode n2 = stmtPatternWithVar("x2");
      final IGroupMemberNode n3 = joinGroupWithVars("x2","x4");
      final JoinGroupNode n4 = joinGroupWithVars("x5");
      n4.setOptional(true);
      final IGroupMemberNode n5 = joinGroupWithVars("x2","x6");
      final SubqueryRoot n6 = subqueryWithVars("x5","x1");
      final JoinGroupNode n7 = joinGroupWithVars("x3","x4");
      n7.setMinus(true);
      final IGroupMemberNode n8 = serviceSparql11WithConstant("x7","x1");
      final IGroupMemberNode n9 = unionWithVars("x2","x3");
      final IGroupMemberNode n10 = unionWithVars("x8","x8","x7");

      
      final List<IGroupMemberNode> nodes = new ArrayList<IGroupMemberNode>();
      nodes.add(n1);
      nodes.add(n2);
      nodes.add(n3);
      nodes.add(n4);
      nodes.add(n5);
      nodes.add(n6);
      nodes.add(n7);
      nodes.add(n8);
      nodes.add(n9);
      nodes.add(n10);
      
      final GroupNodeVarBindingInfoMap bindingInfo =
         new GroupNodeVarBindingInfoMap(
            nodes, statisAnalysisForNodes(nodes), null);

      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(
            nodes, bindingInfo, new HashSet<IVariable<?>>());

      // checks on partitions object
      assertEquals(3, partitions.getPartitionList().size());
      assertEquals(nodes, partitions.extractNodeList(true));
      
      // checks on partition object
      final ASTJoinGroupPartition p1 = partitions.getPartitionList().get(0);
      final List<IGroupMemberNode> p1Exp = new ArrayList<IGroupMemberNode>();
      p1Exp.add(n1);
      p1Exp.add(n2);
      p1Exp.add(n3);
      p1Exp.add(n4);
      assertEquals(p1Exp, p1.extractNodeList(true));
      assertEquals(n4, p1.optionalOrMinus);
      assertEquals(varSet("x1","x2","x4"), p1.definitelyProduced);
      
      final ASTJoinGroupPartition p2 = partitions.getPartitionList().get(1);
      final List<IGroupMemberNode> p2Exp = new ArrayList<IGroupMemberNode>();
      p2Exp.add(n5);
      p2Exp.add(n6);
      p2Exp.add(n7);
      assertEquals(p2Exp, p2.extractNodeList(true));
      assertEquals(n7, p2.optionalOrMinus);
      assertEquals(varSet("x1","x2","x4","x5","x6"), p2.definitelyProduced);

      
      final ASTJoinGroupPartition p3 = partitions.getPartitionList().get(2);
      final List<IGroupMemberNode> p3Exp = new ArrayList<IGroupMemberNode>();
      p3Exp.add(n8);
      p3Exp.add(n9);
      p3Exp.add(n10);
      assertEquals(p3Exp, p3.extractNodeList(true));
      assertNull(p3.optionalOrMinus);
      assertEquals(varSet("x1","x2","x4","x5","x6","x7","x8"), p3.definitelyProduced);
      
   }
}
