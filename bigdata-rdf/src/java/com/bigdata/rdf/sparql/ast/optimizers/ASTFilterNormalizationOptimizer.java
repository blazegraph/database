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
 * Created on June 10, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * <p>
 * The {@link ASTFilterNormalizationOptimizer} is responsible for the static decomposition
 * and optimization of filter expressions. In it's first version, it decomposes
 * conjunctive FILTER expressions, enabling (i) a more precise placement of
 * filter expressions in join groups and (ii) clears the way for static
 * binding analysis through the {@link ASTStaticBindingsOptimizer}.
 * </p>
 * 
 * <p>
 * Further, it removes duplicate filters (inside the identical join group), also
 * if such duplicates might pop up through conversion to CNF and eliminates
 * certain redundant FILTER expressions based on heuristics.
 * </p>
 * 
 * <p>
 * Note that this optimizer disregards filter placement issues; these should
 * be tackled by optimizers running later in the pipeline.
 * </p>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 * @version $Id$
 */
public class ASTFilterNormalizationOptimizer extends AbstractJoinGroupOptimizer {

   @Override
   protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode group) {
      
      // replacement tasks (collected in upcoming for loop)
      final Map<FilterNode, List<List<FilterNode>>> replacements = 
         new HashMap<FilterNode, List<List<FilterNode>>>();
      
      for (IGroupMemberNode child : group) {
         
         if (child instanceof FilterNode) {
            
            final FilterNode filterNode = (FilterNode)child;
            final IValueExpressionNode vexprNode = 
               filterNode.getValueExpressionNode();

            // don't even try if not optimizable:
            if (!isDecomposable(vexprNode)) {
               continue; // skip
            }
            
            // otherwise, check if the node is already in CNF
            final IValueExpressionNode filterAsCNF = 
               StaticAnalysis.isCNF(vexprNode) ? 
               vexprNode : StaticAnalysis.toCNF(vexprNode);

            if (filterAsCNF!=null) {

               final List<FilterNode> filterNodes = 
                  constructFiltersForValueExpressionNode(filterAsCNF, new ArrayList<FilterNode>());
               
               if (filterNodes!=null) {
                  
                  if (!replacements.containsKey(filterNode)) {
                     replacements.put(filterNode,new ArrayList<List<FilterNode>>());
                  }
                  
                  final List<List<FilterNode>> values = replacements.get(filterNode);
                  values.add(filterNodes);
                  
               } // else: something went wrong in conversion, be conservative
               
            } // else: something wrong in conversion, be conservative
            
            if (filterAsCNF!=null) {
               
               // decompose filter
            }
         }
      }

      // remove the filter nodes
      for (final FilterNode filterNode : replacements.keySet()) {

         // remove child as often as we encountered the filter node
         for (int i=0; i<replacements.get(filterNode).size(); i++) {
            group.removeChild(filterNode);
         }
      }

      // remove duplicates amonst filter node
      for (final List<List<FilterNode>> filterNodeListList : replacements.values()) {
         for (final List<FilterNode> filterNodeList : filterNodeListList) {
            for (final FilterNode filterNode : filterNodeList) {
               group.addChild(filterNode);
            }
         }
      }

      // TODO
      // remove duplicates
//      Set<FilterNode> distinctFilters = new HashSet<FilterNode>();

   }

   
   /**
    * Checks if there might be potential for decomposing the filter
    * (overestimation).
    * 
    * @param filterNode
    * @return
    */
   public boolean isDecomposable(final IValueExpressionNode vexpr) {
      
      if(!(vexpr instanceof FunctionNode)) {
         return false;
      }
      
      final FunctionNode functionNode = (FunctionNode)vexpr;
      final URI functionURI = functionNode.getFunctionURI();
      
      if (functionURI.equals(FunctionRegistry.AND) || 
          functionURI.equals(FunctionRegistry.OR)) {

         return true;
         
      } else if (functionURI.equals(FunctionRegistry.NOT)) { 
         
         final BOp bop = functionNode.get(0);
         if (bop instanceof FunctionNode) {
            
            return isDecomposable((FunctionNode)bop);
            
         }
      } 

      // fallback: no decomposition opportunities identified
      return false;
   }

   /** 
    * Construct FILTERs for the given value expression node, exploiting
    * AND nodes at the top to split the node into multiple filters (if
    * possible). Particularly useful to get filter expressions for all
    * conjuncts when given a {@link ValueExpressionNode} in CNF as input. 
    * 
    * @param vexpNode the value expression node
    * @param filters set where to collect filters in
    * 
    * @return the array of filters
    */
   public List<FilterNode> constructFiltersForValueExpressionNode(
      final IValueExpressionNode vexp, final List<FilterNode> filters) {
      
      final List<IValueExpressionNode> topLevelConjuncts = 
         StaticAnalysis.extractToplevelConjuncts(
            vexp, new ArrayList<IValueExpressionNode>());
      
      for (IValueExpressionNode toplevelConjunct : topLevelConjuncts) {
         filters.add(new FilterNode(toplevelConjunct));
      }

      return filters;
   }
   
}
