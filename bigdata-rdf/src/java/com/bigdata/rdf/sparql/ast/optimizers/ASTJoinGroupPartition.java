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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.GroupNodeVarBindingInfo;
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

   
   final Map<IGroupMemberNode,GroupNodeVarBindingInfo> bindingInfo;
   
   final List<IGroupMemberNode> nonOptionalNonMinusNodes;
   final IGroupMemberNode optionalOrMinus;
   
   final Set<IVariable<?>> externallyBound;
   Set<IVariable<?>> definitelyProduced;
   
   /**
    * Constructs a new join group partition.
    * 
    * @param nonOptionalOrMinusNodes
    * @param optionalOrMinus
    * @param bindingInfo
    * @param externallyBound
    */
   ASTJoinGroupPartition(
      final List<IGroupMemberNode> nonOptionalNonMinusNodes,
      final IGroupMemberNode optionalOrMinus,
      final Map<IGroupMemberNode,GroupNodeVarBindingInfo> bindingInfo,         
      final Set<IVariable<?>> externallyBound) {

      this.nonOptionalNonMinusNodes = nonOptionalNonMinusNodes;
      this.optionalOrMinus = optionalOrMinus;
      this.bindingInfo = bindingInfo;
      this.externallyBound = externallyBound;
      
      recomputeDefinitelyProduced();
      
   }

   /**
    * @return the flat (ordered) list of nodes in the partition
    */
   List<IGroupMemberNode> extractNodeList() {
      
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
   void addNonOptionalNonMinusNodeToPartition(IGroupMemberNode node) {
      nonOptionalNonMinusNodes.add(node);
      definitelyProduced.addAll(bindingInfo.get(node).getDefinitelyProduced());
   }
   
   /**
    * @return the variables definitely produced by this partition
    */
   Set<IVariable<?>> getDefinitelyProduced() {
      return definitelyProduced;
   }
   
   /**
    * TODO: test cases, incl. OPTIONAL & MINUS
    * 
    * Partitions a join group node list along OPTIONAL and MINUS nodes.
    * 
    * @param nodes
    * @param bindingInfo
    * @return
    */
   public static List<ASTJoinGroupPartition> partition(
      final List<IGroupMemberNode> nodes,
      final Map<IGroupMemberNode,GroupNodeVarBindingInfo> bindingInfo,
      final Set<IVariable<?>> externallyKnownProduced) {
      
      final List<ASTJoinGroupPartition> partitions = 
         new ArrayList<ASTJoinGroupPartition>();
      
      final Set<IVariable<?>> tmpKnownProduced = 
         new HashSet<IVariable<?>>(externallyKnownProduced);
      final List<IGroupMemberNode> tmpNonOptionalOrMinusNodes = 
         new ArrayList<IGroupMemberNode>();
      IGroupMemberNode tmpOptionalOrMinus = null;
      
      for (int i=0; i<nodes.size(); i++) {
         
         final IGroupMemberNode node = nodes.get(i);
         
         // TODO: verify with Bryan
         final Boolean isOptionalOrMinus = 
            node.getProperty("optional", false) || 
            node.getProperty("minus", false);
            
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
         if (tmpOptionalOrMinus!=null || i+1==nodes.size()) {
            
            // create partition
            final ASTJoinGroupPartition partition = 
               new ASTJoinGroupPartition(
                  new ArrayList<IGroupMemberNode>(tmpNonOptionalOrMinusNodes),
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
      
      return partitions;
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
   

   private void recomputeDefinitelyProduced() {
      
      definitelyProduced = new HashSet<IVariable<?>>();
      definitelyProduced.addAll(externallyBound);
      for (IGroupMemberNode node : nonOptionalNonMinusNodes) {
         definitelyProduced.addAll(bindingInfo.get(node).getDefinitelyProduced());
      }
      
   }
}