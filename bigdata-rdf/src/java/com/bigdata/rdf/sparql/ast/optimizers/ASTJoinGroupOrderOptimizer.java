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
 * Created on June 12, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IVariableBindingRequirements;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;


/**
 * This optimizer brings a join group node into a valid order according to the
 * SPARQL 1.1 semantics and optimizes the order of the nodes in the join group
 * using various heuristics.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
@SuppressWarnings("deprecation")
public class ASTJoinGroupOrderOptimizer extends AbstractJoinGroupOptimizer 
implements IASTOptimizer {
   
   // TODO: optimize join group is called too often when having nested join groups, why?
   @Override
   protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode joinGroup) {

      if (!ASTStaticJoinOptimizer.isStaticOptimizer(ctx, joinGroup))
         return;

      /**
       * Initialize summary containers with a single pass over the children.
       */
      final Map<IGroupMemberNode, VariableBindingInfo> bindingInfo = 
         new HashMap<IGroupMemberNode, VariableBindingInfo>();

      final NodeClassification nodeClassification = new NodeClassification();
            
      final Set<IVariable<?>> externallyIncoming = 
         sa.getDefinitelyIncomingBindings(joinGroup, new HashSet<IVariable<?>>());
      for (IGroupMemberNode child : joinGroup) {
         
         final VariableBindingInfo vbc = 
            new VariableBindingInfo(child, sa, externallyIncoming);
         bindingInfo.put(child,vbc); 
         nodeClassification.registerNode(child,vbc);
         
      }
      

      /**
       * First, add all nodes that do not require special handling
       */
      LinkedList<IGroupMemberNode> nodeList = new LinkedList<IGroupMemberNode>();
      for (IGroupMemberNode node : nodeClassification.noneSpecialNodes) {
         nodeList.add(node);
      }
      
      // basic reordering
      nodeList = reorderNodes(nodeList, bindingInfo, externallyIncoming);
      
      // place the service nodes
      for (IGroupMemberNode node : nodeClassification.serviceNodesWithSpecialHandling) {
         placeAtFirstContributingPosition(
            nodeList, node, bindingInfo, externallyIncoming);
      }

      // place the VALUES nodes
      for (IGroupMemberNode node : nodeClassification.valuesNodes) {
         placeAtFirstContributingPosition(
            nodeList, node, bindingInfo, externallyIncoming);
      }

      /**
       * Place the BIND nodes: it is important that we bring them into the
       * right order, e.g. if bind node 1 uses variables bound by bind node 2,
       * then we must insert node 2 first in order to be able to place node 1
       * after the first bind node.
       */
      
      // calculate variables that are known to be bound after the join group
      final Set<IVariable<?>> knownBoundSomewhere =
         new HashSet<IVariable<?>>(externallyIncoming);
      for (final IGroupMemberNode node : nodeList) {
         knownBoundSomewhere.addAll(bindingInfo.get(node).definitelyProduced);
      }
      
      final List<AssignmentNode> bindNodesOrdered = 
         orderBindNodesByDependencies(
            nodeClassification.bindNodes, bindingInfo, knownBoundSomewhere);
      for (AssignmentNode node : bindNodesOrdered) {
         placeAtFirstContributingPosition(
            nodeList, node, bindingInfo, externallyIncoming);
      }
      
      
      
      // place the join filters at the best position
      for (IGroupMemberNode node : nodeClassification.filters) {
         placeAtFirstPossiblePosition(
            nodeList, node, bindingInfo, externallyIncoming);
      }
      
      // Replace the children with those in the [ordered] list.
      for (int i = 0; i < joinGroup.arity(); i++) {
          joinGroup.setArg(i, (BOp) nodeList.get(i));
      }

      
      // TODO: creation of subgroups at "borders"
      // TODO: make sure that the placement is performed correctly at the end!
   }


   /**
    * Brings the BIND nodes in a correct order according to the dependencies
    * that they have. Note that this is a best effort approach, which may
    * fail in cases where we allow for liberate patterns that do not strictly
    * follow the restriction of SPARQL semantics (e.g., for cyclic patterns
    * such as BIND(?x AS ?y) . BIND(?y AS ?x)). We may want to check the query
    * for such patterns statically and throw a pre-runtime exception, as the
    * SPARQL 1.1 standard suggests.
    * 
    * Failing means we return an order that is not guaranteed to be "safe", 
    * which might give us unexpected results in some exceptional cases.
    * However, whenever the pattern is valid according to the SPARQL 1.1
    * semantics, this method should return nodes in a valid order.
    * 
    * @param bindNodes the list of BIND nodes to reorder
    * @param bindingInfo hash map for binding info lookup
    * @param knownBoundSomewhere variables that are known to be bound somewhere
    *        in the node list where we want to place the BIND nodes
    *        
    * @return the ordered node set if exists, otherwise a "best effort" order
    */
   List<AssignmentNode> orderBindNodesByDependencies(
      final List<AssignmentNode> bindNodes,
      final Map<IGroupMemberNode,VariableBindingInfo> bindingInfo,
      final Set<IVariable<?>> knownBoundSomewhere) {
      
      final List<AssignmentNode> ordered = 
         new ArrayList<AssignmentNode>(bindNodes.size());

      final LinkedList<AssignmentNode> toBePlaced = 
         new LinkedList<AssignmentNode>(bindNodes);

      final Set<IVariable<?>> knownBound =
         new HashSet<IVariable<?>>(knownBoundSomewhere);
      while (!toBePlaced.isEmpty()) {
         
         for (int i=0 ; i<toBePlaced.size(); i++) {
            
            final AssignmentNode node = toBePlaced.get(i);
            final VariableBindingInfo nodeBindingInfo = bindingInfo.get(node);
            
            /**
             * The first condition is that the node can be safely placed. The
             * second condition is a fallback, where we randomly pick the last
             * node (even if it does not satisfy the condition).
             */
            if (nodeBindingInfo.leftToBeBound(knownBound).isEmpty() ||
                i+1==toBePlaced.size()) {

               // add the node to the ordered set
               ordered.add(node);
               
               // remove it from the toBePlaced array
               toBePlaced.remove(i);
               
               // add its bound variables to the known bound set
               knownBound.addAll(nodeBindingInfo.definitelyProduced);
               
               break;
            }
            
         }
         
      }
      
      return ordered;
      
   }


   /**
    * Perform basic reordering, trying to find a good order obeying the
    * SPARQL semantics based on the types of nodes contained in the list.
    * 
    * @param nodeList
    * @param nodesByType 
    */
  LinkedList<IGroupMemberNode> reorderNodes(
      final LinkedList<IGroupMemberNode> nodeList, 
      final Map<IGroupMemberNode,VariableBindingInfo> bindingInfo,
      final Set<IVariable<?>> externallyKnownProduced) {
      
     /**
      * Compute the partitions.
      */
     // TODO: make the bindingInfo a class and create a utility method to
     //       populate it
     final List<ASTJoinGroupPartition> partitions = 
        ASTJoinGroupPartition.partition(
           nodeList, bindingInfo, externallyKnownProduced);
      
     /**
      * First, optimize across partitions.
      */
      // first, move the statement patterns to the beginning wherever possible
      optimizeAcrossPartitions(
         partitions, bindingInfo, externallyKnownProduced);

      /**
       * Second, optimize within partitions.
       */
      optimizeWithinPartitions(partitions);
      
      /**
       * Generate the output node list through iteration over partitions.
       */
      LinkedList<IGroupMemberNode> res = new LinkedList<IGroupMemberNode>();
      for (int i=0; i<partitions.size(); i++) {
         res.addAll(partitions.get(i).extractNodeList());
      }
      return res;
      
   }


  /**
   * Moves the nodes contained in the set as far to the beginning of the join
   * group as possible without violating the semantics. In particular, this
   * function takes care to not "skip" OPTIONAL and MINUS constructs when
   * it would change the outcome of the query.
   * 
   * @param set a set of nodes
   */
  void optimizeAcrossPartitions(
     final List<ASTJoinGroupPartition> partitions,
     final Map<IGroupMemberNode,VariableBindingInfo> bindingInfo,      
     final Set<IVariable<?>> externallyIncoming) {      

     /**
      * We follow a bubble sort approach, where we iteratively move nodes
      * across partitions until a fixed point is reached.
      * 
      * In case the last partition becomes empty, remove it from the list of
      * partitions. // TODO: test case
      * 
      * The condition for moving nodes into prior partitions is made solely
      * based on variables shared across nodes and subgroups. More precisely,
      * we are allowed to move a node n from a partition to its predecessor 
      * partition p if and only if the following holds:
      * 
      * TBD
      * 
      */
     // TODO: implement
     
  }

   /**
    * Optimize the order of nodes within a single partition. The nice thing
    * about partitions is that we can freely reorder the non-optional
    * non-minus nodes within them. 
    */
   private void optimizeWithinPartitions(
      final List<ASTJoinGroupPartition> partitions) {
 
      IASTJoinGroupPartitionReorderer reorderer =
         new TypeBasedASTJoinGroupPartitionReorderer();
      for (ASTJoinGroupPartition partition : partitions) {
         reorderer.reorderNodes(partition);
      }
   }


   /**
    * Places the given node at the first position where, for the subsequent
    * child, at least one of the variables bound through the node is used.
    * Also considers the fact that this node must not be placed *before*
    * its first possible position according to the binding requirements.
    * 
    * @param nodeList the list of nodes in which to place the given node
    * @param node the node to place
    * @param bindingRequirements the binding requirements for the node to place
    * @param externallyIncoming variables that are known to be bound from the
    *        join groups parent
    * 
    * @return a new list containing the node, placed at the first position
    *         where, for the subsequent child, at least one of the variables 
    *         bound through the node is used
    */
   void placeAtFirstContributingPosition(
         final LinkedList<IGroupMemberNode> nodeList, 
         final IGroupMemberNode node,
         final Map<IGroupMemberNode,VariableBindingInfo> bindingInfo, 
         final Set<IVariable<?>> externallyIncoming) {

         final Integer firstPossiblePosition = 
            getFirstPossiblePosition(nodeList,node,bindingInfo,externallyIncoming);

         /**
          * Special case (which simplifies subsequent code, as it asserts that
          * firstPossiblePosition indeed exists; if not, we skip analysis).
          */
         if (firstPossiblePosition==null) {
            placeAtPosition(nodeList, node, firstPossiblePosition); // place at end
            return;
         }
         
         /**
          * The binding requirements for the given node
          */
         final VariableBindingInfo bindingInfoForNode = bindingInfo.get(node);
         final Set<IVariable<?>> maybeProducedByNode = 
            bindingInfoForNode.maybeProduced;
         
         /**
          * If there is some overlap between the known bound variables and the
          * maybe produced variables by this node, than it might be good to
          * place this node right at the beginning, as this implies a join
          * that could restrict the intermediate result set.
          */
         final Set<IVariable<?>> intersectionWithExternallyIncomings = 
            new HashSet<IVariable<?>>();
         intersectionWithExternallyIncomings.addAll(externallyIncoming);
         intersectionWithExternallyIncomings.retainAll(maybeProducedByNode);
         
         if (!intersectionWithExternallyIncomings.isEmpty()) {
            placeAtPosition(nodeList, node, firstPossiblePosition);
         }
         
         /**
          * If this is not the case, we watch out for the first construct using
          * one of the variables that may be produced by this node and place
          * the node right in front of it. This is a heuristics, of course,
          * which may be refined based on experiences that we make over time.
          */
         for (int i=0; i<nodeList.size(); i++) {

            final Set<IVariable<?>> desiredBound = 
               bindingInfo.get(nodeList.get(i)).desiredBound;
               
            final Set<IVariable<?>> intersection = new HashSet<IVariable<?>>();
            intersection.addAll(desiredBound);
            intersection.retainAll(maybeProducedByNode);
            
            // if no more variables need to be bound, place the node
            if (!intersection.isEmpty()) {
               
               /**
                * If the first possible position differs from null and is
                * larger than i, then place it there; if it is
                * smaller than i, then i is where we place the node. So we're
                * looking for the maximum of both.
                */
               placeAtPosition(nodeList, node, Math.max(i, firstPossiblePosition));
               return;
            }
            
         }
         
         /**
          * As a fallback, we add the node at the end.
          */
         nodeList.addLast(node);    
   }

   /**
    * Places the given node at the first possible position in the nodeList
    * according to the binding requirements.
    * 
    * @param nodeList the list of nodes in which to place the given node
    * @param node the node to place
    * @param bindingReq the binding requirements for the node to place
    * 
    * @return a new list containing the node, placed at the first position
    *         that is possible according to the binding requirements
    */
   void placeAtFirstPossiblePosition(
         final LinkedList<IGroupMemberNode> nodeList, 
         final IGroupMemberNode node,
         final Map<IGroupMemberNode,VariableBindingInfo> bindingInfo, 
         final Set<IVariable<?>> externallyIncoming) {
      
      final Integer positionToPlace = 
         getFirstPossiblePosition(nodeList,node,bindingInfo,externallyIncoming);
      
      placeAtPosition(nodeList, node, positionToPlace);
     
   }

   /**
    * Places the node at the specified position in the list. If the position
    * is null, the node is added at the end.
    * 
    * @param nodeList
    * @param node
    * @param positionToPlace
    */
   void placeAtPosition(final LinkedList<IGroupMemberNode> nodeList,
         final IGroupMemberNode node, final Integer positionToPlace) {
      if (positionToPlace == null) {
         nodeList.addLast(node);
      } else {
         nodeList.add(positionToPlace, node);
      }
   }
   
   /**
    * Computes for the given node, the first possible position in the nodeList
    * according to the binding requirements.
    * 
    * @param nodeList the list of nodes in which to place the given node
    * @param node the node to place
    * @param bindingReq the binding requirements for the node to place
    * 
    * @return the position ID is integer, null if no matching position was found
    */
   Integer getFirstPossiblePosition(
         final LinkedList<IGroupMemberNode> nodeList, 
         final IGroupMemberNode node,
         final Map<IGroupMemberNode,VariableBindingInfo> bindingInfo, 
         final Set<IVariable<?>> externallyIncoming) {

      final HashSet<IVariable<?>> knownBound = 
            new HashSet<IVariable<?>>(externallyIncoming);

         /**
          * The binding requirements for the given node
          */
         final VariableBindingInfo bindingInfoForNode = bindingInfo.get(node);
         
         for (int i=0; i<nodeList.size(); i++) {

            // if no more variables need to be bound, we can place the node
            if (bindingInfoForNode.leftToBeBound(knownBound).isEmpty()) {
               return i;
            }
            
            // add the child's definitely produced bindings to the variables
            // that are known to be bound
            knownBound.addAll(bindingInfo.get(nodeList.get(i)).definitelyProduced);
         }
         
         /**
          * No suitable position found:
          */
         return null;   
   }




   /**
    * Classification of nodes. This class maintains
    * 
    * - Lists of nodes classified by types that are treated differently
    *   according to the standard, in particular (in original order)
    *   - One list for filter nodes.
    *   - One list of BIND and VALUES node.
    *   - One list containing other nodes with binding requirements.
    *   - One list containing the remaining nodes.
    *   
    * Note that, according to the SPARQL 1.1 semantics, use of BIND ends the
    * preceding graph pattern. Here, we place BIND nodes at the first position
    * where it can be evaluated, i.e. treat them the same way as we do with
    * FILTERs (giving priority to FILTERs). This might not always be 100%
    * inline with the standard, but is probably the most efficient way to
    * deal with them.
    *   
    * - A map, mapping from node type to a set of nodes with the respective type
    */
   protected static class NodeClassification {

      
      /**
       * Filter nodes.
       */
      List<FilterNode> filters;
      
      /**
       * SPARQL 1.1 bindings clause such as VALUES (?x ?y) { (1 2) (3 4) }
       */
      List<BindingsClause> valuesNodes;

      /**
       * SPARQL 1.1 BIND nodes.
       */
      List<AssignmentNode> bindNodes;
      
      /**
       * Can be freely reordered in the join group as long as 
       */
      List<ServiceNode> serviceNodesWithSpecialHandling;
      
      /**
       * All nodes that have binding requirements, i.e. must not be evaluated
       * before certain variables are bound. 
       */
      List<IGroupMemberNode> noneSpecialNodes;
      
            
      /**
       * Constructor.
       * 
       * @param __preFilters the pre filters known for the associated join group
       * @param __joinFilters the join filters known for the associated join group
       * @param __postFilters the post filters known for the associated join group
       */
      public NodeClassification() {
         
         filters = new ArrayList<FilterNode>();
         valuesNodes = new ArrayList<BindingsClause>();
         bindNodes = new ArrayList<AssignmentNode>();
         serviceNodesWithSpecialHandling = new ArrayList<ServiceNode>();
         noneSpecialNodes = new ArrayList<IGroupMemberNode>();
      }
      
      public void registerNode(
         final IGroupMemberNode node, final VariableBindingInfo vbc) {
         
         final ServiceFactory defaultSF = 
            ServiceRegistry.getInstance().getDefaultServiceFactory();
         
         if (node instanceof AssignmentNode) {

            bindNodes.add((AssignmentNode)node);
            
         } else if (node instanceof BindingsClause) {
            
            valuesNodes.add((BindingsClause)node);

         } else if (node instanceof FilterNode) {
            
            filters.add((FilterNode)node);

         } else if (node instanceof StatementPatternNode) {
            
            final StatementPatternNode spn = (StatementPatternNode)node;
            noneSpecialNodes.add(node);
            
         } else if (node instanceof NamedSubqueryInclude) {
            
            final NamedSubqueryInclude nsi = (NamedSubqueryInclude)node;

            noneSpecialNodes.add(node);
            
         } else if (node instanceof ArbitraryLengthPathNode ||
            node instanceof ZeroLengthPathNode || 
            node instanceof PropertyPathUnionNode) {
            
            noneSpecialNodes.add(node);
            
         // TODO: verify case of ASK subqueries
         } else if (node instanceof SubqueryRoot) {
            
            noneSpecialNodes.add(node);
               
         } else if (node instanceof ServiceNode) {

            final ServiceNode serviceNode = (ServiceNode)node;
            
            // add service node to the list it belongs to
            if (!vbc.leftToBeBound(new HashSet<IVariable<?>>()).isEmpty() ||
                  !serviceNode.getResponsibleServiceFactory().equals(defaultSF)) {
               
               serviceNodesWithSpecialHandling.add((ServiceNode)node);
               
            } else {
               
               // only SPARQL 1.1 SERVICE nodes with constant go here (they
               // can be treated strictly according to the semantics); note
               // that the SPARQL 1.1 standard doesn't give a concise semantics
               // for SPARQL 1.1 queries with variable endpoint
               noneSpecialNodes.add(node);
               
            }
            
         // NOTE: don't move around, this is in the end in purpose to allow
         // the treatment of more specialized cases
         } else if (node instanceof GraphPatternGroup<?>) {
            
            noneSpecialNodes.add(node);

         } else {
            
            noneSpecialNodes.add(node);
            
         }
      }      

   }
   

   /**
    * Class summarizing the variable binding requirements for a given node
    * (used for children in the join group).
    */
   protected static class VariableBindingInfo {

      /**
       * The node for which the information is valid.
       */
      final IGroupMemberNode node;
      
      /**
       * Variables that must not be bound upon evaluation of this node,
       * see {@link IVariableBindingRequirements} for detailed documentation.
       */
      Set<IVariable<?>> requiredBound;
      
      /**
       * Variables that are desired to be bound upon evaluation of this node,
       * see {@link IVariableBindingRequirements} for detailed documentation.
       */
      Set<IVariable<?>> desiredBound;

      /**
       * Variables that are possibly bound by this node.
       */
      Set<IVariable<?>> maybeProduced;

      /**
       * Variables that are definitely bound by this node.
       */
      Set<IVariable<?>> definitelyProduced;

      
      public VariableBindingInfo(
         final IGroupMemberNode node, final StaticAnalysis sa,
         final Set<IVariable<?>> definitelyIncomingBindings) {
         
         this.node = node;
         this.requiredBound = node.getRequiredBound(sa);
         this.desiredBound = node.getDesiredBound(sa);
         
         this.maybeProduced = new HashSet<IVariable<?>>();
         if (node instanceof IBindingProducerNode) {
            this.maybeProduced = 
               sa.getMaybeProducedBindings(
                  (IBindingProducerNode)node, maybeProduced, true);
         }
         
         this.definitelyProduced = new HashSet<IVariable<?>>();
         if (node instanceof IBindingProducerNode) {
            this.definitelyProduced = 
               sa.getDefinitelyProducedBindings(
                  (IBindingProducerNode)node, definitelyProduced, true);
         }
         
         System.out.println();
      }

      /**
       * Get variables that remain to be bound, assuming that the definitely
       * incoming variables plus the variables passed as parameter are bound
       * already.
       */
      public Set<IVariable<?>> leftToBeBound(Set<IVariable<?>> knownBound) {
         
         HashSet<IVariable<?>> toBeBound = new HashSet<IVariable<?>>();
         toBeBound.addAll(requiredBound);
         toBeBound.removeAll(knownBound);
         
         return toBeBound;
         
      }
      
   }


}
