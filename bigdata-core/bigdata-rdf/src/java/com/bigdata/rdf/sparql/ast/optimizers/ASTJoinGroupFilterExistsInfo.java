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
 * Created on June 23, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;


/**
 * Information about FILTER (NOT) EXISTS patterns within a given join
 * group. A FILTER (NOT) EXIST pattern is internally represented through
 * an ASK subquery (testing the inner statement pattern and binding a
 * variable --exists-i) and a subsequent FILTER expression guaranteeing 
 * that the variable is true (FILTER EXISTS) or false (FILTER NOT EXISTS).
 *
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTJoinGroupFilterExistsInfo {
   
   /**
    * Mapping from the subqueries to the filter nodes.
    */
   final Map<SubqueryRoot, FilterNode> filterMap;
   
   public ASTJoinGroupFilterExistsInfo(final Iterable<IGroupMemberNode> nodeList) {

      filterMap = new HashMap<SubqueryRoot, FilterNode>();

      
      /**
       * extract ASK subqueries and Exists() + NotExists() filter nodes
       */
      final Map<IVariable<?>,FilterNode> filters = 
         new HashMap<IVariable<?>,FilterNode>();
      final Set<SubqueryRoot> askSubqueries = new HashSet<SubqueryRoot>();
      for (final IGroupMemberNode node : nodeList) {
         
         if (node instanceof FilterNode) {
            final FilterNode filter = (FilterNode)node;
            IValueExpressionNode inner = filter.getValueExpressionNode();
            
            if (inner instanceof ExistsNode || 
                  inner instanceof NotExistsNode) {
               final SubqueryFunctionNodeBase existsOrNotExists = 
                  (SubqueryFunctionNodeBase)inner;
               
               if (existsOrNotExists.arity()==1) {
                  final BOp varAsBop = existsOrNotExists.get(0);
                  if (varAsBop instanceof VarNode) {
                     final VarNode varNode = (VarNode)varAsBop;
                     if (varNode.getValueExpression()!=null) {
                        filters.put(varNode.getValueExpression(),filter);
                     }
                  }
               }
               
            } 
            
         } else if (node instanceof SubqueryRoot) {
            
            final SubqueryRoot sqr = (SubqueryRoot)node;
            if (sqr.getQueryType().equals(QueryType.ASK)) {
               askSubqueries.add(sqr);
            }
         }
      }
      
      /**
       * Associate the subqueries with the FILTERs
       */
      for (final SubqueryRoot sqr : askSubqueries) {
         final IVariable<?> askVar = sqr.getAskVar();
         if (filters.containsKey(askVar)) {
            filterMap.put(sqr, filters.get(askVar));
         }
      }
   }
   
   /**
    * Checks whether the SubqueryRoot at hand is contained in the
    * FilterExistsInfo, i.e. whether it is part of a FILTER (NOT) EXISTS.
    */
   public boolean containsSubqueryRoot(final SubqueryRoot sqr) {
      return filterMap.keySet().contains(sqr);
   }
   
   /**
    * Checks whether the Filter at hand is contained in the
    * FilterExistsInfo, i.e. whether it is part of a FILTER (NOT) EXISTS.
    */
   public boolean containsFilter(final FilterNode filter) {
      return filterMap.values().contains(filter);
   }
}
