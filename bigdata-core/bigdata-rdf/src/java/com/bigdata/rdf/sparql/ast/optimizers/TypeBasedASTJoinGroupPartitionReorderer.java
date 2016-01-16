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

import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;


/**
 * Reorders nodes based on their types.
 * 
 * The heuristics is as follows:
 * 
 * 1. Named subquery includes (given that subqueries are typically used to enforce order)
 * 2. Statement patterns
 * 3. Subgroups
 * 4. ALP nodes
 * 5. Subqueries
 * 6. SERVICE calls
 * 7. OTHER
 */
@SuppressWarnings("deprecation")
public class TypeBasedASTJoinGroupPartitionReorderer 
implements IASTJoinGroupPartitionReorderer {

   /**
    * Reorders the given partition in a semantics-preserving way.
    */
   @Override
   public void reorderNodes(ASTJoinGroupPartition partition) {
      
      final ASTTypeBasedNodeClassifier nodeClassifier = 
            new ASTTypeBasedNodeClassifier(
               new Class<?>[]{ 
                  NamedSubqueryInclude.class,
                  StatementPatternNode.class,
                  ZeroLengthPathNode.class,
                  PropertyPathUnionNode.class,
                  ArbitraryLengthPathNode.class,
                  SubqueryRoot.class,
                  ServiceNode.class,
                  GraphPatternGroup.class
               }, 
               partition.nonOptionalNonMinusNodes);

      // split service nodes into runFirst and runLast service nodes
      final List<ServiceNode> runFirstSNs = new LinkedList<ServiceNode>();
      final List<ServiceNode> runLastSNs = new LinkedList<ServiceNode>();
      for (ServiceNode sn : nodeClassifier.get(ServiceNode.class)) {
         if (sn.getResponsibleServiceFactory().getServiceOptions().isRunFirst()) {
            runFirstSNs.add(sn);
         } else {
            runLastSNs.add(sn);            
         }
      }

      // order the nodes based on their types
      final List<IGroupMemberNode> ordered = new LinkedList<IGroupMemberNode>();
      ordered.addAll(runFirstSNs);
      ordered.addAll(nodeClassifier.get(NamedSubqueryInclude.class));
      ordered.addAll(nodeClassifier.get(StatementPatternNode.class));
      ordered.addAll(nodeClassifier.get(GraphPatternGroup.class));
      ordered.addAll(nodeClassifier.get(ZeroLengthPathNode.class));
      ordered.addAll(nodeClassifier.get(PropertyPathUnionNode.class));
      ordered.addAll(nodeClassifier.get(ArbitraryLengthPathNode.class));
      ordered.addAll(nodeClassifier.get(SubqueryRoot.class));
      ordered.addAll(runLastSNs);
      ordered.addAll(nodeClassifier.getUnclassifiedNodes());

      // just replace the order in the partition
      partition.replaceNonOptionalNonMinusNodesWith(ordered, false);
      
   }

}
