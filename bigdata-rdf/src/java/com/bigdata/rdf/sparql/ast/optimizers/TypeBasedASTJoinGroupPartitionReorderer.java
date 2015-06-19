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
 * Created on June 18, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;


/**
 * Reorders nodes based on their types.
 * 
 * The heuristics is as follows:
 * 
 * 1. Named subquery includes (given that subqueries are typically used to enforce order)
 * 2. Statement patterns
 * 3. Subgroups
 * 4. ALP nodes
 * 5. Subqueries
 * 6. SERVICE calls
 * 7. OTHER
 */
@SuppressWarnings("deprecation")
public class TypeBasedASTJoinGroupPartitionReorderer 
implements IASTJoinGroupPartitionReorderer {
   
   /**
    * Node types for non-special handled nodes in join groups. I.e., we do
    * not include FILTERs, BIND, VALUES, etc.
    */
   public enum NodeType {
      ALP_NODE,                   /* Property path nodes */
      NAMED_SUBQUERY_INCLUDE,     /* INCLUDE %NAMED_QUERY% */
      SUBGROUP,                   /* Subgroup, such as { ... } or UNIONs */
      SUBQUERY,                   /* { SELECT ... WHERE { ... } } */
      STMT_PATTERN,               /* (s p o) */
      SERVICE,                    /* SERVICE ... { } */
      OTHER                       /* Any construct we forgot to list */
   }
   
   /**
    * Reorders the given partition in a semantics-preserving way.
    */
   @Override
   public void reorderNodes(ASTJoinGroupPartition partition) {
      
      final Map<NodeType,List<IGroupMemberNode>> nodeTypeInfo = 
         classifyNodes(partition.nonOptionalNonMinusNodes);

      final List<IGroupMemberNode> ordered = new LinkedList<IGroupMemberNode>();
      
      addNodesOfType(ordered, nodeTypeInfo, NodeType.NAMED_SUBQUERY_INCLUDE);
      addNodesOfType(ordered, nodeTypeInfo, NodeType.STMT_PATTERN);
      addNodesOfType(ordered, nodeTypeInfo, NodeType.SUBGROUP);
      addNodesOfType(ordered, nodeTypeInfo, NodeType.ALP_NODE);
      addNodesOfType(ordered, nodeTypeInfo, NodeType.SUBQUERY);
      addNodesOfType(ordered, nodeTypeInfo, NodeType.SERVICE);
      addNodesOfType(ordered, nodeTypeInfo, NodeType.OTHER);

      // just replace the order in the partition
      partition.replaceNonOptionalNonMinusNodesWith(ordered);
      
   }
   
   /**
    * Adds the node of the specified type recorded in the nodeTypeInfo variable
    * to the set of nodes.
    * 
    * @param ordered the set of nodes where to add
    * @param nodeTypeInfo the node type info map
    * @param nodeType the type of nodes to add
    */
   private void addNodesOfType(List<IGroupMemberNode> ordered,
         Map<NodeType, List<IGroupMemberNode>> nodeTypeInfo,
         NodeType nodeType) {
      
      if (ordered==null || nodeTypeInfo==null) {
         return; // nothing we can do (should not happen though)
      }
      
      final List<IGroupMemberNode> nodesOfType = nodeTypeInfo.get(nodeType);
      if (nodesOfType!=null) {
         ordered.addAll(nodesOfType);
      }
      
   }

   Map<NodeType,List<IGroupMemberNode>> classifyNodes(
      final List<IGroupMemberNode> nodes) {
      
      final Map<NodeType,List<IGroupMemberNode>> nodeTypeInfo =
         new HashMap<NodeType,List<IGroupMemberNode>>();
      
      for (IGroupMemberNode node : nodes) {
         
         if (node instanceof StatementPatternNode) {
            
            classifyNode(node, NodeType.STMT_PATTERN, nodeTypeInfo);
            
         } else if (node instanceof NamedSubqueryInclude) {
            
            classifyNode(node, NodeType.NAMED_SUBQUERY_INCLUDE, nodeTypeInfo);
            
         } else if (node instanceof ArbitraryLengthPathNode ||
            node instanceof ZeroLengthPathNode || 
            node instanceof PropertyPathUnionNode) {
            
            classifyNode(node, NodeType.ALP_NODE, nodeTypeInfo);
            
         // TODO: verify case of ASK subqueries
         } else if (node instanceof SubqueryRoot) {
            
            classifyNode(node, NodeType.SUBQUERY, nodeTypeInfo);
               
         } else if (node instanceof ServiceNode) {

            classifyNode(node, NodeType.SERVICE, nodeTypeInfo);
            
         // NOTE: don't move around, this is in the end in purpose to allow
         // the treatment of more specialized cases
         } else if (node instanceof GraphPatternGroup<?>) {
            
            classifyNode(node, NodeType.SUBGROUP, nodeTypeInfo);

         } else {
            
            classifyNode(node, NodeType.OTHER, nodeTypeInfo);
            
         }
      }
         
      return nodeTypeInfo;
   }
   
   /**
    * Classifies the node with the given type, recording it in the given map.
    */
   void classifyNode(
      final IGroupMemberNode node, final NodeType classification,
      final Map<NodeType,List<IGroupMemberNode>> nodeTypePartition) {
      
      // record in map for lookup
      if (!nodeTypePartition.containsKey(classification)) {
         nodeTypePartition.put(
            classification,new ArrayList<IGroupMemberNode>());
      }            
      nodeTypePartition.get(classification).add(node);
      
   }
}
