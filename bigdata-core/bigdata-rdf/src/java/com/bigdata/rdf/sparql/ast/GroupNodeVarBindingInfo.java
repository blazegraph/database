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
package com.bigdata.rdf.sparql.ast;

import java.util.HashSet;
import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinGroupFilterExistsInfo;



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
   final private IGroupMemberNode node;
   
   /**
    * Variables that must be bound upon evaluation of this node,
    * see {@link IVariableBindingRequirements} for detailed documentation.
    */
   final private Set<IVariable<?>> requiredBound;
   
   /**
    * Variables that are desired to be bound upon evaluation of this node,
    * see {@link IVariableBindingRequirements} for detailed documentation.
    */
   final private Set<IVariable<?>> desiredBound;

   /**
    * Variables that are possibly bound by this node.
    */
   final private Set<IVariable<?>> maybeProduced;

   /**
    * Variables that are definitely bound by this node.
    */
   final private Set<IVariable<?>> definitelyProduced;


   /**
    * Constructor
    * @param node
    * @param sa
    */
   public GroupNodeVarBindingInfo(
      final IGroupMemberNode node, 
      final StaticAnalysis sa,
      final ASTJoinGroupFilterExistsInfo fExInfo) {
      
      this.node = node;
      this.requiredBound = node.getRequiredBound(sa);
      this.desiredBound = node.getDesiredBound(sa);
      
      this.maybeProduced = new HashSet<IVariable<?>>();
      if (node instanceof IBindingProducerNode) {
         sa.getMaybeProducedBindings(
            (IBindingProducerNode)node, maybeProduced, true);
      }
      
      this.definitelyProduced = new HashSet<IVariable<?>>();
      if (node instanceof IBindingProducerNode) {
         
         if (!StaticAnalysis.isMinusOrOptional(node)) {
            
            sa.getDefinitelyProducedBindings(
               (IBindingProducerNode)node, definitelyProduced, true);
         }
      }

      /**
       * Special handing for ASK subqueries. See also ticket #BLZG-1284
       * and the associated test in TestTickets for why this is necessary.
       */
      if (fExInfo!=null && node instanceof SubqueryRoot && 
            fExInfo.containsSubqueryRoot((SubqueryRoot)node)) {
         
         final SubqueryRoot sqr = (SubqueryRoot)node;

         // there are no variables that are desired bound (all are required)
         this.desiredBound.clear();
         
         // in addition to what's reported by getDefinitelyProducedBindings,
         // the askVar of the subquery must be considered
         this.definitelyProduced.add(sqr.getAskVar());
         
         // required are all projection vars minus the ask var, i.e. these
         // must be bound when evaluating the FILTER (NOT) EXISTS node
         sqr.getProjectedVars(this.requiredBound);
         this.requiredBound.remove(sqr.getAskVar());
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
