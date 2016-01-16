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
 * Created on June 18, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfo;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfoMap;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;

/**
 * Partition of a join group, defined by a (possibly empty) list of
 * non-optional non-minus nodes, possibly closed by a single optional
 * or minus node. Each partition maintains a set of variables that are
 * definitely bound *after* evaluating the partition. Note that this
 * list is equivalent to the nodes definitely bound *after* evaluating the
 * non-optional non-minus nodes in the partition (i.e., the OPTIONAL/MINUS
 * will not contribute to this list). 
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTJoinGroupPartition {

   final GroupNodeVarBindingInfoMap bindingInfoMap;
   
   final LinkedList<IGroupMemberNode> nonOptionalNonMinusNodes;
   
   /**
    * Variables that are bound externally, i.e. from prior parts of the query
    * plan. Note that this does not include variables that are bound by prior
    * partitions (in case this partition is part of an 
    * {@link ASTJoinGroupPartitions} object. Once initialized, this should
    * never be changed.
    */
   final Set<IVariable<?>> externallyBound;

   /**
    * The optional of minus node marking the "border" (i.e., last node) of
    * the partition.
    */
   IGroupMemberNode optionalOrMinus;
   
   /**
    * The variables that are definitely produced by this partition, i.e.
    * can be assumed being bound in subsequent partitions. This includes
    * variables that are externally bound.
    */
   Set<IVariable<?>> definitelyProduced;
   
   /**
    * Constructs a new join group partition.
    * 
    * @param nonOptionalOrMinusNodes
    * @param optionalOrMinus
    * @param bindingInfoMap
    * @param externallyBound
    */
   ASTJoinGroupPartition(
      final LinkedList<IGroupMemberNode> nonOptionalNonMinusNodes,
      final IGroupMemberNode optionalOrMinus,
      final GroupNodeVarBindingInfoMap bindingInfoMap,         
      final Set<IVariable<?>> externallyBound) {

      this.nonOptionalNonMinusNodes = nonOptionalNonMinusNodes;
      this.optionalOrMinus = optionalOrMinus;
      this.bindingInfoMap = bindingInfoMap;
      this.externallyBound = externallyBound;
      
      recomputeDefinitelyProduced();
      
   }

   /**
    * @return the flat (ordered) list of nodes in the partition
    */
   public List<IGroupMemberNode> extractNodeList(
      final boolean includeOptionalOrMinusNode) {
      
      final List<IGroupMemberNode> nodeList =
         new ArrayList<IGroupMemberNode>();
      
      nodeList.addAll(nonOptionalNonMinusNodes);
      if (includeOptionalOrMinusNode && optionalOrMinus!=null)
         nodeList.add(optionalOrMinus);
      
      return nodeList;
   }

   /**
    * Adds a (non-optional non-minus) node to a join group partition and
    * updates the set of definitely produced variables accordingly.
    */
   public void addNonOptionalNonMinusNodeToPartition(IGroupMemberNode node) {
      nonOptionalNonMinusNodes.add(node);
      definitelyProduced.addAll(bindingInfoMap.get(node).getDefinitelyProduced());
   }
   
   /**
    * @return the variables definitely produced by this partition
    */
   public Set<IVariable<?>> getDefinitelyProduced() {
      return definitelyProduced;
   }

   /**
    * The new ordered list of non-optional non-minus nodes. If 
    * recomputedDefinitelyProduced variables is set to false, the definitely
    * produced variables will not be recomputed (this is a performance tweak
    * which can be exploited when reordering the nodes only, for instance).
    * 
    * @param ordered
    * @param recomputeDefinitelyProduced
    */
   public void replaceNonOptionalNonMinusNodesWith(
      final List<IGroupMemberNode> ordered,
      final boolean recomputeDefinitelyProduced) {
      
      nonOptionalNonMinusNodes.clear();
      nonOptionalNonMinusNodes.addAll(ordered);      

      if (recomputeDefinitelyProduced) {
         recomputeDefinitelyProduced();
      }
   }
   
   /**
    * Removes the given set of nodes and updates the internal data structures.
    */
   public void removeNodesFromPartition(List<IGroupMemberNode> nodesToRemove) {
      
      for (final IGroupMemberNode nodeToRemove : nodesToRemove) {
      
         if (nodeToRemove!=null) {
            if (!nonOptionalNonMinusNodes.remove(nodeToRemove)) {
               if (nodeToRemove.equals(optionalOrMinus)) {
                  optionalOrMinus=null;
               }
            }
            
         } // else ignore
      }
      
      recomputeDefinitelyProduced();
   }
   

   /**
    * Places the given node at the first position where, for the subsequent
    * child, at least one of the variables bound through the node is used.
    * Also considers the fact that this node must not be placed *before*
    * its first possible position according to the binding requirements.
    * 
    * NOTE: requires the node to be contained in the partitions binding info map.
    */
   void placeAtFirstContributingPosition(
         final IGroupMemberNode node, 
         final Set<IVariable<?>> additionalKnownBound,
         final boolean requiresAllBound) {

         final Integer firstPossiblePosition = 
            getFirstPossiblePosition(node, additionalKnownBound, requiresAllBound);

         /**
          * Special case (which simplifies subsequent code, as it asserts that
          * firstPossiblePosition indeed exists; if not, we skip analysis).
          */
         if (firstPossiblePosition==null) {
            placeAtPosition(node, firstPossiblePosition); // place at end
            return;
         }
         
         /**
          * The binding requirements for the given node
          */
         final GroupNodeVarBindingInfo bindingInfo = bindingInfoMap.get(node);
         final Set<IVariable<?>> maybeProducedByNode = 
            bindingInfo.getMaybeProduced();
         
         /**
          * If there is some overlap between the known bound variables and the
          * maybe produced variables by this node, than it might be good to
          * place this node right at the beginning, as this implies a join
          * that could restrict the intermediate result set.
          */
         final Set<IVariable<?>> intersectionWithExternallyIncomings = 
            new HashSet<IVariable<?>>();
         intersectionWithExternallyIncomings.addAll(externallyBound);
         intersectionWithExternallyIncomings.retainAll(maybeProducedByNode);
         
         if (!intersectionWithExternallyIncomings.isEmpty()) {
            placeAtPosition(node, firstPossiblePosition);
            return;
         }
         
         /**
          * If this is not the case, we watch out for the first construct using
          * one of the variables that may be produced by this node and place
          * the node right in front of it. This is a heuristics, of course,
          * which may be refined based on experiences that we make over time.
          */
         for (int i=0; i<nonOptionalNonMinusNodes.size(); i++) {

            final Set<IVariable<?>> desiredBound = 
               bindingInfoMap.
                 get(nonOptionalNonMinusNodes.get(i)).getDesiredBound();
               
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
               placeAtPosition(node, Math.max(i, firstPossiblePosition));
               return;
            }
            
         }
         
         /**
          * As a fallback, we add the node at the end.
          */
         nonOptionalNonMinusNodes.addLast(node);    
   }

   /**
    * Places the given node at the first possible position in the non-optional
    * non-minus list of the partition, where the first possible position is
    * derived from the binding requirements of the node.
    * 
    * NOTE: requires the node to be contained in the partitions binding info map.
    */
   void placeAtFirstPossiblePosition(
      final IGroupMemberNode node, 
      final Set<IVariable<?>> additionalKnownBound,
      final boolean requiresAllBound) {
      
      placeAtPosition(node, 
         getFirstPossiblePosition(
            node, additionalKnownBound, requiresAllBound));
      definitelyProduced.addAll(bindingInfoMap.get(node).getDefinitelyProduced());
   }

   /**
    * Places the node at the specified position in the list of non-optional
    * non-minus nodes. If the position is null, the node is added at the end
    * (this is, right in front of the bordering optional or minus node).
    */
   void placeAtPosition(
      final IGroupMemberNode node, final Integer positionToPlace) {
      
      if (positionToPlace == null) {
         nonOptionalNonMinusNodes.addLast(node);
      } else {
         nonOptionalNonMinusNodes.add(positionToPlace, node);
      }
   }
   
   /**
    * Computes for the given node, the first possible position in the partition
    * according to its binding requirements. The first possible position is
    * either the first position where all required variables of the node are
    * known to be bound or where we know that none of its required variables
    * may be bound anymore. The flag requiresAllBound can be used to turn off
    * the last condition, requiring that all variables need to be bound (and
    * returning null in case this condition is never satisfied). This flag
    * is useful when distributing FILTERs across partitions.
    * 
    * @param the node to place
    * @param additional variables that are known to be bound
    * @param requiresAllBound requires that all variables are bound, i.e. when
    *          set to true it is not sufficient that unbound variables can't
    *          be bound anymore
    * 
    * @return the position ID as integer, null if no matching position was found
    */
   Integer getFirstPossiblePosition(
      final IGroupMemberNode node, 
      final Set<IVariable<?>> additionalKnownBound,
      final boolean requiresAllBound) {

      /**
       * The binding requirements for the given node
       */
      final GroupNodeVarBindingInfo bindingInfo = bindingInfoMap.get(node);
      
      /**
       * knownBound is the set of variables that are known to be bound at 
       * a certain point in time. Initially, it contains the externallyBound
       * variables plus additionalKnownBound variable which can be passed in.
       * Variables will be added as we iterate over the non-optional non-minus
       * nodes in the partition,
       */
      final HashSet<IVariable<?>> knownBound = 
         new HashSet<IVariable<?>>(externallyBound);
      knownBound.addAll(additionalKnownBound);

      /**
       * remainingPossiblyBound is a multi set (where the integer represents
       * the cardinality) initially counting, for each variable, how often
       * it is maybe bound in the partition. Variables counts will be decreased
       * (and entries will be removed once we reach 0) as we iterate over the
       * non-optional non-minus nodes in the partition.
       */
      final Map<IVariable<?>,Integer> remainingMaybeBound = 
          new HashMap<IVariable<?>,Integer>();
      if (!requiresAllBound) { // save initialization effort if not used
         
         // both non-optional non-minus nodes generate maybe bound mappings ...
         for (final IGroupMemberNode nonmNode : nonOptionalNonMinusNodes) {
            addMaybeProducedToMultiset(remainingMaybeBound, nonmNode);
         }
         
         // ... as well as the optional or minus node in the partition
         if (optionalOrMinus!=null) {
            addMaybeProducedToMultiset(remainingMaybeBound, optionalOrMinus);
         }
      }
      
      /**
       * Now let's iterate over the non-optional and non-minus nodes and try
       * to iterate the first possible position for the node based on its
       * binding requirements. We are allowed to place the node at the first
       * position for which we know that either all of the node's required
       * variables are bound or no more of it required variables can be bound.
       * If no such position exists, the method returns null (in that case,
       * only the end of the partition may be a safe place for the node).
       */
      for (int i=0; i<nonOptionalNonMinusNodes.size(); i++) {
         
         if (canBePlacedAtPosition(
               requiresAllBound, bindingInfo, knownBound,remainingMaybeBound, i)) {
            return i; // we're done
         }
            
         // updade knownBound
         final IGroupMemberNode cur = nonOptionalNonMinusNodes.get(i);

         final Set<IVariable<?>> definitelyProducedByCur = 
            bindingInfoMap.get(cur).getDefinitelyProduced();
         knownBound.addAll(definitelyProducedByCur);
         
         // update remainingMaybeBound
         if (!requiresAllBound) {
            final Set<IVariable<?>> maybeProducedByCur = 
               bindingInfoMap.get(cur).getMaybeProduced();
            for (final IVariable<?> var : maybeProducedByCur) {
               if (remainingMaybeBound.containsKey(var)) { 
                  // decrease counter
                  remainingMaybeBound.put(var, remainingMaybeBound.get(var) - 1);
                  
                  // and fully remove var if counter reached zero
                  if (remainingMaybeBound.get(var)<=0) {
                     remainingMaybeBound.remove(var);
                  }
               }
            }
         }
      }
         
      /**
       * Check again for the last position (not covered by the for loop)
       */
      Integer lastPosition = nonOptionalNonMinusNodes.size();
      if (canBePlacedAtPosition(
            requiresAllBound, bindingInfo, knownBound, remainingMaybeBound,
            lastPosition)) {
         return lastPosition; // we're done
      }
      
      /**
       * No suitable position found:
       */
      return null;   
   }

   /**
    * Internal helper function to check whether a node can be placed at
    * a given position if the passed parameter constellation is satisfied.
    */
   private boolean canBePlacedAtPosition(final boolean requiresAllBound,
         final GroupNodeVarBindingInfo bindingInfo,
         final HashSet<IVariable<?>> knownBound,
         final Map<IVariable<?>, Integer> remainingMaybeBound, int i) {
      
      // if no more variables need to be bound, we can place the node
      final Set<IVariable<?>> leftToBeBound = 
         bindingInfo.leftToBeBound(knownBound);
      if (leftToBeBound.isEmpty()) {
         return true;
      }
      
      // another case in which we can place the node is if *none* of the
      // remaining variables can be bound anymore; so we try to identify
      // a witness that *can* be bound
      if (!requiresAllBound) {
         boolean moreCanBeBound = false;
         final Set<IVariable<?>> canBeBound = remainingMaybeBound.keySet();
         for (IVariable<?> leftToBeBoundVar : leftToBeBound) {
            moreCanBeBound |= canBeBound.contains(leftToBeBoundVar);
            if (moreCanBeBound) {
               break;
            }
         }
         if (!moreCanBeBound) {
            return true;
         }
      }
      
      return false;
   }

   /**
    * Adds the variables that are maybe produced in the node to the multi set.
    */
   private void addMaybeProducedToMultiset(
         final Map<IVariable<?>, Integer> multiset,
         IGroupMemberNode node) {
      
      final GroupNodeVarBindingInfo bi = bindingInfoMap.get(node);
      for (IVariable<?> var : bi.getMaybeProduced()) {
         if (!multiset.containsKey(var)) {
            multiset.put(var, 1);
         } else {
            multiset.put(var, multiset.get(var) + 1);               
         }
      }
   }
   

   /**
    * Recompute the definitely produced variables by this partition.
    */
   private void recomputeDefinitelyProduced() {
      
      definitelyProduced = new HashSet<IVariable<?>>();
      
      definitelyProduced.addAll(externallyBound);
      
      for (IGroupMemberNode node : nonOptionalNonMinusNodes) {
         definitelyProduced.addAll(bindingInfoMap.get(node).getDefinitelyProduced());
      }
      
   }

}
