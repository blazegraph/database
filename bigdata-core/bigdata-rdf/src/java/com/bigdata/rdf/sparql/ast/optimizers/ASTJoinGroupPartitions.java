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
 * Created on June 20, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfoMap;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;

/**
 * Class representing an ordered list of {@link ASTJoinGroupPartition}s.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTJoinGroupPartitions {

   /**
    * The ordered list (main data structure).
    */
   List<ASTJoinGroupPartition> partitions;
   
   /**
    * Constructor, creating partitions for a list of {@link IGroupMemberNode}
    * objects along OPTIONAL and MINUS nodes. The idea of this partitioning
    * is that we are not freely allowed to reorder nodes across partitions.
    * 
    * @param nodes the list of nodes to partition
    * @param bindingInfo the bindingInfo for these nodes
    * @param externallyKnownProduced variables that are known to be bound when
    *        starting processing the join group defined by the node list
    */
   public ASTJoinGroupPartitions(      
      final List<IGroupMemberNode> nodes,
      final GroupNodeVarBindingInfoMap bindingInfo,
      final Set<IVariable<?>> externallyKnownProduced) {

      partitions = new ArrayList<ASTJoinGroupPartition>();

      final Set<IVariable<?>> tmpKnownProduced = 
         new HashSet<IVariable<?>>(externallyKnownProduced);
      
      final List<IGroupMemberNode> tmpNonOptionalOrMinusNodes = 
         new ArrayList<IGroupMemberNode>();
      IGroupMemberNode tmpOptionalOrMinus = null;
      for (int i = 0; i < nodes.size(); i++) {

         final IGroupMemberNode node = nodes.get(i);

         final Boolean isOptionalOrMinus = 
            StaticAnalysis.isMinusOrOptional(node);

         /**
          * Either add the node to the non-optional non-minus node list or
          * assign it to the temporary tmpOptionalOrMinus variable.
          */
         if (!isOptionalOrMinus) {
            tmpNonOptionalOrMinusNodes.add(node);
         } else {
            tmpOptionalOrMinus = node;
         }

         /**
          * If the tmpOptionalOrMinus node has been set or we reached the end,
          * we build and record the partition and reset the temporary variables.
          */
         if (tmpOptionalOrMinus != null || i + 1 == nodes.size()) {

            // create partition
            final ASTJoinGroupPartition partition = new ASTJoinGroupPartition(
                  new LinkedList<IGroupMemberNode>(tmpNonOptionalOrMinusNodes),
                  tmpOptionalOrMinus /* may be null */, bindingInfo,
                  new HashSet<IVariable<?>>(tmpKnownProduced));

            // record partition
            partitions.add(partition);

            // re-initialize the tmp arrays for future iterations
            tmpKnownProduced.addAll(partition.definitelyProduced);
            tmpNonOptionalOrMinusNodes.clear();
            tmpOptionalOrMinus = null;
         }
      }
      
      // special handling: there's no node -> create a dummy partition
      if (partitions.isEmpty()) {
         final ASTJoinGroupPartition partition = new ASTJoinGroupPartition(
               new LinkedList<IGroupMemberNode>(tmpNonOptionalOrMinusNodes),
               null, bindingInfo, new HashSet<IVariable<?>>(tmpKnownProduced));         
         partitions.add(partition);
      }
   }

   /**
    * Return the inner list of partitions.
    */
   public List<ASTJoinGroupPartition> getPartitionList() {
      return partitions;
   }
   
   /**
    * Extracts all nodes in all partitions, in order.
    */
   public LinkedList<IGroupMemberNode> extractNodeList(
      final boolean includeOptionalOrMinusNode) {
      
      LinkedList<IGroupMemberNode> res = new LinkedList<IGroupMemberNode>();
      for (int i=0; i<partitions.size(); i++) {
         res.addAll(partitions.get(i).extractNodeList(includeOptionalOrMinusNode));
      }
      
      return res;
      
   }

   /**
    * Places the node at the first possible position across all partitions.
    * 
    * @param node
    */
   public void placeAtFirstPossiblePosition(IGroupMemberNode node) {
      
      final Set<IVariable<?>> knownBoundFromPrevPartitions =
            new HashSet<IVariable<?>>();
      for (int i=0; i<partitions.size(); i++) {
         
         final ASTJoinGroupPartition partition = partitions.get(i);
         
         /**
          * Try to place the node in the partition. As long as we do not reach
          * the last partition (i<partitions.size()-1), we must require that 
          * all required variables of the node are bound, i.e. the "right"
          * position might be in a later partition.
          */
         final Integer position = 
            partition.getFirstPossiblePosition(
               node, knownBoundFromPrevPartitions, 
               i<partitions.size()-1 /* requireAllBound */);

         // if position found:
         if (position!=null) {
            
            partition.placeAtPosition(node,position);
            return;
            
         // if reached the end:
         } else if (i+1==partitions.size()) {
            
            if (partition.optionalOrMinus==null) {
               
               // in this case it is safe to place the node at the end
               partition.placeAtPosition(node,null);
               
            } else {

               // append a dummy partition containing the node
               final LinkedList<IGroupMemberNode> listWithNode =
                  new LinkedList<IGroupMemberNode>();
               listWithNode.add(node);
               
               final ASTJoinGroupPartition dummyPartition = 
                  new ASTJoinGroupPartition(listWithNode, null,
                     partition.bindingInfoMap, partition.externallyBound);
               partitions.add(dummyPartition);
               
            }
            
            return;
         }
         
         knownBoundFromPrevPartitions.addAll(partition.getDefinitelyProduced());
      }
      
      // if the node has not been succesfully placed, at it into a new
      // partition at the end
      
   }
}
