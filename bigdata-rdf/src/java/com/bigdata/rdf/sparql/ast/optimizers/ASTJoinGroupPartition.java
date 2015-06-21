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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
   
   final Set<IVariable<?>> externallyBound;
   
   IGroupMemberNode optionalOrMinus;
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
   public List<IGroupMemberNode> extractNodeList() {
      
      final List<IGroupMemberNode> nodeList =
         new ArrayList<IGroupMemberNode>();
      
      nodeList.addAll(nonOptionalNonMinusNodes);
      if (optionalOrMinus!=null)
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
    * Removes the given set of nodes and updates the internal datastructures.
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
    * NOTE: currently, this methods requires the node to be contained in the
    * binding info map.
    */
   void placeAtFirstContributingPosition(final IGroupMemberNode node) {

         final Integer firstPossiblePosition = getFirstPossiblePosition(node);

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
    * NOTE: currently, this methods requires the node to be contained in the
    * binding info map.
    */
   void placeAtFirstPossiblePosition(final IGroupMemberNode node) {
      
      placeAtPosition(node, getFirstPossiblePosition(node));
      definitelyProduced.addAll(bindingInfoMap.get(node).getDefinitelyProduced());
   }

   /**
    * Places the node at the specified position in the list of non-optional
    * non-minus nodes. If the position is null, the node is added at the end.
    * Must be a valid position within the list.
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
    * according to its binding requirements.
    * 
    * @return the position ID as integer, null if no matching position was found
    */
   Integer getFirstPossiblePosition(final IGroupMemberNode node) {

      final HashSet<IVariable<?>> knownBound = 
            new HashSet<IVariable<?>>(definitelyProduced);

         /**
          * The binding requirements for the given node
          */
         final GroupNodeVarBindingInfo bindingInfo = bindingInfoMap.get(node);
         
         for (int i=0; i<nonOptionalNonMinusNodes.size(); i++) {

            // if no more variables need to be bound, we can place the node
            if (bindingInfo.leftToBeBound(knownBound).isEmpty()) {
               return i;
            }
            
            // add the child's definitely produced bindings to the variables
            // that are known to be bound
            knownBound.addAll(
               bindingInfoMap.get(
                  nonOptionalNonMinusNodes.get(i)).getDefinitelyProduced());
         }
         
         /**
          * No suitable position found:
          */
         return null;   
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