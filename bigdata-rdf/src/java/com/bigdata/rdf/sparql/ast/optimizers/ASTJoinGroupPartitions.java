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
            node.getProperty("optional", false)
            || node.getProperty("minus", false);

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
   public LinkedList<IGroupMemberNode> extractNodeList() {
      
      LinkedList<IGroupMemberNode> res = new LinkedList<IGroupMemberNode>();
      for (int i=0; i<partitions.size(); i++) {
         res.addAll(partitions.get(i).extractNodeList());
      }
      
      return res;
      
   }

   /**
    * Places the node at the first possible position across all partitions.
    * 
    * @param node
    */
   public void placeAtFirstPossiblePosition(IGroupMemberNode node) {
      
      for (int i=0; i<partitions.size(); i++) {
         
         final ASTJoinGroupPartition partition = partitions.get(i);
         Integer position = partition.getFirstPossiblePosition(node);
         
         if (position!=null || i+1==partitions.size()) {
            partition.placeAtFirstPossiblePosition(node);
         }
      }
      
   }
}