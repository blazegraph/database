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
 * Created on June 24, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.List;

import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;

/**
 * Class allowing for precise placement of FILTER expressions within
 * join group.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
class ASTFilterPlacer {
   
   final ASTJoinGroupFilterExistsInfo fExInfo;
   
   final ASTTypeBasedNodeClassifier filterNodeClassifier;
   
   public ASTFilterPlacer(
      final Iterable<IGroupMemberNode> nodeList, 
      final ASTJoinGroupFilterExistsInfo fExInfo) {
      
      this.fExInfo = fExInfo;

      /**
       * Filter out the filter nodes from the join group, they will receive
       * special handling in the end. Note that this includes ASK subqueries
       * which belong to FILTER expressions.
       */
      filterNodeClassifier = 
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
      
      filterNodeClassifier.registerNodes(nodeList);

   }
   
   /**
    * @return the non filter nodes in the group
    */
   public List<IGroupMemberNode> getNonFilterNodes() {
      return filterNodeClassifier.getUnclassifiedNodes();
   }

   /**
    * Places the FILTERs of this {@link ASTFilterPlacer} in the given
    * partitions object.
    */
   public void placeFiltersInPartitions(
      final ASTJoinGroupPartitions partitions) {

      final List<FilterNode> filterNodes =
         filterNodeClassifier.get(FilterNode.class);
      final List<SubqueryRoot> askSubqueries =
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
   }

}
