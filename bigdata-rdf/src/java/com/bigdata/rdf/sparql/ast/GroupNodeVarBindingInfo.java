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
package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.Set;

import com.bigdata.bop.IVariable;



/**
 * Class summarizing the variable binding requirements for a given node
 * (used for children in the join group), thus providing easy access to its
 * variable binding information {@link IGroupMemberNode}s.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GroupNodeVarBindingInfo {
   /**
    * The node for which the information is valid.
    */
   final IGroupMemberNode node;
   
   /**
    * Variables that must be bound upon evaluation of this node,
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


   /**
    * Constructor
    * @param node
    * @param sa
    */
   public GroupNodeVarBindingInfo(
      final IGroupMemberNode node, final StaticAnalysis sa) {
      
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
         
         if (!(node.getProperty("optional", false) || 
               node.getProperty("minus", false))) {
            
            this.definitelyProduced = 
               sa.getDefinitelyProducedBindings(
                  (IBindingProducerNode)node, definitelyProduced, true);
         }
      }

   }
   
   public IGroupMemberNode getNode() {
      return node;
   }

   public Set<IVariable<?>> getRequiredBound() {
      return requiredBound;
   }

   public Set<IVariable<?>> getDesiredBound() {
      return desiredBound;
   }

   public Set<IVariable<?>> getMaybeProduced() {
      return maybeProduced;
   }

   public Set<IVariable<?>> getDefinitelyProduced() {
      return definitelyProduced;
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
