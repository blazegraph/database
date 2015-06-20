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
import java.util.Set;

import com.bigdata.bop.IVariable;
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
   
   final List<IGroupMemberNode> nonOptionalNonMinusNodes;
   final IGroupMemberNode optionalOrMinus;
   
   final Set<IVariable<?>> externallyBound;
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
      final List<IGroupMemberNode> nonOptionalNonMinusNodes,
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
      definitelyProduced.addAll(bindingInfoMap.get(node).getDefinitelyProduced());
   }
   
   /**
    * @return the variables definitely produced by this partition
    */
   Set<IVariable<?>> getDefinitelyProduced() {
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
   

   private void recomputeDefinitelyProduced() {
      
      definitelyProduced = new HashSet<IVariable<?>>();
      definitelyProduced.addAll(externallyBound);
      for (IGroupMemberNode node : nonOptionalNonMinusNodes) {
         definitelyProduced.addAll(bindingInfoMap.get(node).getDefinitelyProduced());
      }
      
   }
}