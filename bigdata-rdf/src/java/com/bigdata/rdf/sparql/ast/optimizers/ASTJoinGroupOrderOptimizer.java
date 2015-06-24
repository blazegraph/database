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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfo;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfoMap;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
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
public class ASTJoinGroupOrderOptimizer extends AbstractJoinGroupOptimizer 
implements IASTOptimizer {
   
   @Override
   protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode joinGroup) {

      if (!ASTStaticJoinOptimizer.isStaticOptimizer(ctx, joinGroup))
         return;

      /**
       * Initialize summary containers with a single pass over the children;
       * they allow for efficient lookup of required information in the
       * following.
       */
            
      final Set<IVariable<?>> externallyIncoming = 
         sa.getDefinitelyIncomingBindings(
            joinGroup, new HashSet<IVariable<?>>());

      final ASTJoinGroupFilterExistsInfo fExInfo = 
            new ASTJoinGroupFilterExistsInfo(joinGroup);

      final GroupNodeVarBindingInfoMap bindingInfoMap =
         new GroupNodeVarBindingInfoMap(joinGroup, sa, fExInfo);
      
      
      /**
       * Filter out the filter nodes from the join group, they will receive
       * special handling in the end. Note that this includes ASK subqueries
       * which belong to FILTER expressions.
       */
      final ASTTypeBasedNodeClassifier filterNodeClassifier = 
         new ASTTypeBasedNodeClassifier(
            new Class<?>[]{ FilterNode.class, SubqueryRoot.class });
      
      /**
       * We're interested in the ASK subquery roots associated with some
       * EXISTS or NOT EXISTS filter, as recoreded in the fExInfo map.
       */
      filterNodeClassifier.addConstraintForType(SubqueryRoot.class, 
         new ASTTypeBasedNodeClassifierConstraint() {
         
            @Override
            boolean appliesTo(final IGroupMemberNode node) {
                  
               if (node instanceof SubqueryRoot) {
                  return fExInfo.containsSubqueryRoot((SubqueryRoot)node);
               }
                  
               // as a fallback return false
               return false; 
                  
            }
         });       
      
      filterNodeClassifier.registerNodes(joinGroup.getChildren());

      
      /**
       * Set up the partitions, ignoring the FILTER nodes (they'll be added
       * in the end, after having flattened the partitions again).
       */
      final ASTJoinGroupPartitions partitions = 
         new ASTJoinGroupPartitions(
            filterNodeClassifier.getUnclassifiedNodes() /* non filter nodes */,
            bindingInfoMap, externallyIncoming);
      
      /**
       * First, optimize across the partitions, trying to move forward
       * non-optional non-minus patterns wherever possible. It is important to
       * do this first, before reordering within partitions, as it shifts
       * nodes around across them.
       */
      optimizeAcrossPartitions(
            partitions, bindingInfoMap, externallyIncoming);
      
      /** 
       * Second, optimize within the individual partitions.
       */
      optimizeWithinPartitions(partitions);


      /**
       * Finally place the filters ans the associated ASK subqueries.
       */
      final List<IGroupMemberNode> filterNodes =
         filterNodeClassifier.get(FilterNode.class);
      final List<IGroupMemberNode> askSubqueries =
            filterNodeClassifier.get(SubqueryRoot.class);

      // first, place the ASK subqueries
      for (final IGroupMemberNode askSubquery : askSubqueries) {
         partitions.placeAtFirstPossiblePosition(askSubquery);
      }
      
      // second, place the (non-related) simple filters
      for (final IGroupMemberNode filerNode : filterNodes) {
         if (!fExInfo.containsFilter((FilterNode)filerNode)) {
            partitions.placeAtFirstPossiblePosition(filerNode);
            
         }
      }
      
      // third, place the FILTERs associated with the ASK subqueries, to
      // make sure that these FILTERs are directly following the ASK subqueries
      // (in order to avoid interaction with other FILTERs)
      for (final IGroupMemberNode askSubquery : askSubqueries) {
         partitions.placeAtFirstPossiblePosition(
            fExInfo.filterMap.get(askSubquery));
      }
      
      /**
       * Now, flatten the partitions again and replace the children of the
       * join group with the new list.
       */
      final LinkedList<IGroupMemberNode> nodeList = 
         partitions.extractNodeList();
      for (int i = 0; i < joinGroup.arity(); i++) {
          joinGroup.setArg(i, (BOp) nodeList.get(i));
      }

      // TODO: creation of subgroups at "borders"

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
     final ASTJoinGroupPartitions partitions,
     final GroupNodeVarBindingInfoMap bindingInfoMap,      
     final Set<IVariable<?>> externallyKnownProduced) {      

     
     final List<ASTJoinGroupPartition> partitionList = 
        partitions.getPartitionList();
     
     /**
      * In the following list, we store the variables that are definitely
      * produced *before* evaluating a partition. We maintain this set
      * for fast lookup.
      */
     final List<Set<IVariable<?>>> definitelyProducedUpToPartition =
         new ArrayList<Set<IVariable<?>>>(partitionList.size());
     
     final Set<IVariable<?>> producedUpToPartition =
        new HashSet<IVariable<?>>(externallyKnownProduced);
     for (int i=0; i<partitionList.size();i++) {

        // we start out with the second partitions, so this will succeed
        if (i>0) {
           producedUpToPartition.addAll(
              partitionList.get(i-1).getDefinitelyProduced());
        }        
        
        definitelyProducedUpToPartition.add(
           new HashSet<IVariable<?>>(producedUpToPartition));
        
     }
     
     /**
      * Having initialized the map now, we iterate over the patterns in the
      * partitions and try to shift them to the first possible partition.
      * My intuition is that the algorithm is optimal in the sense that,
      * after running it, every node is in the firstmost partition where
      * it can be safely placed.
      */
     for (int i=1; i<partitionList.size(); i++) {
        
        final ASTJoinGroupPartition partition = partitionList.get(i);
        
        final List<IGroupMemberNode> unmovableNodes = 
              new ArrayList<IGroupMemberNode>();
        for (IGroupMemberNode candidate : partition.nonOptionalNonMinusNodes) {
           
           // find the firstmost partition in which the node can be moved
           Integer partitionForCandidate = null;
           for (int j=i-1; j>=0; j--) {
              
              final ASTJoinGroupPartition candPartition = partitionList.get(j);
              
              /**
               * Calculate the conflicting vars as the intersection of the
               * maybe vars of the bordering OPTIONAL or MINUS with the maybe
               * vars of the node to move around, minus the nodes that are
               * known to be bound upfront.
               */
              final Set<IVariable<?>> conflictingVars;
              if (candPartition.optionalOrMinus == null) {
                 conflictingVars = new HashSet<IVariable<?>>();
              } else {
                 conflictingVars = 
                    new HashSet<IVariable<?>>(
                       bindingInfoMap.get(
                          candPartition.optionalOrMinus).getMaybeProduced());
              }

              final GroupNodeVarBindingInfo candidateBindingInfo = 
                 bindingInfoMap.get(candidate);
              conflictingVars.retainAll(candidateBindingInfo.getMaybeProduced());
              
              conflictingVars.removeAll(
                 definitelyProducedUpToPartition.get(j));
              
              if (conflictingVars.isEmpty() && 
                    definitelyProducedUpToPartition.get(j).containsAll(
                       candidateBindingInfo.getRequiredBound())) {
                 partitionForCandidate = j;
              } else {
                 // can't place here, abort
                 break;
              }
              
           }
           
           if (partitionForCandidate!=null) {

              // add the node to the partition (removal will be done later on)
              final ASTJoinGroupPartition partitionToMove = 
                 partitionList.get(partitionForCandidate);
              partitionToMove.addNonOptionalNonMinusNodeToPartition(candidate);
              
              /**
               * Given that the node has been moved to partitionForCandidate,
               * the definitelyProducedUpToPartition needs to be updated for
               * all partitions starting at the partition following the 
               * partitionForCandidate, up to the partition i, which contained
               * the node before (later partitions carry this info already).
               * 
               * Note that k<=i<partitions.size() guarantees that we don't run
               * into an index out of bound exception.
               */
             for (int k=partitionForCandidate+1; k<=i; k++) {
                 definitelyProducedUpToPartition.get(k).addAll(
                    bindingInfoMap.get(candidate).getDefinitelyProduced());
              }
              
              // the node will be removed from the current partition at the end
              
           } else {
              unmovableNodes.add(candidate);
           }
           
        }

        // the nodes that remain in this position are the unmovable nodes
        partition.replaceNonOptionalNonMinusNodesWith(unmovableNodes, true);
     }
     
  }

   /**
    * Optimize the order of nodes within a single partition. The nice thing
    * about partitions is that we can freely reorder the non-optional
    * non-minus nodes within them. 
    */
   void optimizeWithinPartitions(final ASTJoinGroupPartitions partitions) {
 
      final List<ASTJoinGroupPartition> partitionList = 
         partitions.getPartitionList();
      
      final Set<IVariable<?>> knownBoundFromPrevPartitions =
         new HashSet<IVariable<?>>();
      for (ASTJoinGroupPartition partition : partitionList) {
         
         final ASTTypeBasedNodeClassifier classifier = 
            new ASTTypeBasedNodeClassifier(
               new Class<?>[] { 
                  ServiceNode.class,
                  AssignmentNode.class,
                  BindingsClause.class,
               });
         
         /**
          * We only consider special service nodes for placement, all other
          * service nodes are treated through a standard reorder.
          */
         classifier.addConstraintForType(ServiceNode.class, 
            new ASTTypeBasedNodeClassifierConstraint() {
               @Override
               boolean appliesTo(final IGroupMemberNode node) {
                  
                  if (node instanceof ServiceNode) {

                     /**
                      * Return true if the service is not a SPARQL 1.1 SERVICE.
                      */
                     final ServiceNode sn = (ServiceNode)node;
                     if (!sn.getResponsibleServiceFactory().equals(
                           ServiceRegistry.getInstance().
                           getDefaultServiceFactory())) {
                        return true;
                     }
                     
                     /**
                      * Return true if it is a SPARQL 1.1 SERVICE, but the
                      * constant is not bound.
                      */
                     if (!sn.getServiceRef().isConstant()) {
                        return true;
                     }
                  }
                  
                  // as a fallback return false
                  return false; 
                  
               }
            });
  
         classifier.registerNodes(partition.extractNodeList());
         
         /**
          * In a first step, we remove service nodes, assignment nodes, and
          * bindings clauses from the partition. They will be handled in a
          * special way.
          */
         final List<IGroupMemberNode> toRemove = 
            new ArrayList<IGroupMemberNode>();
         toRemove.addAll(classifier.get(ServiceNode.class));
         toRemove.addAll(classifier.get(AssignmentNode.class));
         toRemove.addAll(classifier.get(BindingsClause.class));
         partition.removeNodesFromPartition(toRemove);
         
         // the remaining elements will be reordered based on their type
         final IASTJoinGroupPartitionReorderer reorderer =
               new TypeBasedASTJoinGroupPartitionReorderer();
         reorderer.reorderNodes(partition);
         
         /**
          * Place the special handled SERVICE nodes.
          */
         for (IGroupMemberNode node : classifier.get(ServiceNode.class)) {
            partition.placeAtFirstPossiblePosition(
               node,knownBoundFromPrevPartitions,false /* requires all bound */);
         }

         /**
          * Place the VALUES nodes.
          */
         for (IGroupMemberNode node : classifier.get(BindingsClause.class)) {
            partition.placeAtFirstContributingPosition(
               node,knownBoundFromPrevPartitions,false /* requires all bound */);
         }

         /**
          * Place the BIND nodes: it is important that we bring them into the
          * right order, e.g. if bind node 1 uses variables bound by bind node 2,
          * then we must insert node 2 first in order to be able to place node 1
          * after the first bind node.
          */
         final Set<IVariable<?>> knownBoundSomewhere =
            new HashSet<IVariable<?>>(partition.definitelyProduced);
         
         // ... order the bind nodes according to dependencies
         final List<AssignmentNode> bindNodesOrdered = 
            orderBindNodesByDependencies(
                  classifier.get(AssignmentNode.class), 
                  partition.bindingInfoMap, knownBoundSomewhere);
         
         // ... and place the bind nodes
         for (AssignmentNode node : bindNodesOrdered) {
            partition.placeAtFirstContributingPosition(
               node,knownBoundFromPrevPartitions, false /* requiresAllBound */);
         }       

         knownBoundFromPrevPartitions.addAll(partition.getDefinitelyProduced());
      }
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
    * @param bindingInfoMap hash map for binding info lookup
    * @param knownBoundSomewhere variables that are known to be bound somewhere
    *        in the node list in which  we want to place the BIND nodes
    *        
    * @return the ordered node set if exists, otherwise a "best effort" order
    */
   List<AssignmentNode> orderBindNodesByDependencies(
      final List<IGroupMemberNode> bindNodes,
      final GroupNodeVarBindingInfoMap bindingInfoMap,
      final Set<IVariable<?>> knownBoundSomewhere) {
      
      final List<AssignmentNode> ordered = 
         new ArrayList<AssignmentNode>(bindNodes.size());

      final LinkedList<AssignmentNode> toBePlaced = 
         new LinkedList<AssignmentNode>();
      for (int i=0; i<bindNodes.size(); i++) {
         final IGroupMemberNode assNodeCandidate = bindNodes.get(i);
         if (assNodeCandidate instanceof AssignmentNode) {
            toBePlaced.add((AssignmentNode)assNodeCandidate);
         }
      }

      final Set<IVariable<?>> knownBound =
         new HashSet<IVariable<?>>(knownBoundSomewhere);
      while (!toBePlaced.isEmpty()) {
         
         for (int i=0 ; i<toBePlaced.size(); i++) {
            
            final AssignmentNode node = toBePlaced.get(i);
            final GroupNodeVarBindingInfo nodeBindingInfo = bindingInfoMap.get(node);
            
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
               knownBound.addAll(nodeBindingInfo.getDefinitelyProduced());
               
               break;
            }
            
         }
         
      }
      
      return ordered;
      
   }
}
