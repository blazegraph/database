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
 * Created on June 10, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
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
 * <p>
 * Possible extensions: (i) there are much more patterns for checking for
 * trivially satisfied filters (however I'm not sure it's worth to put upfront
 * effort in this here); (ii) we may want to check for filters that are
 * trivially not satisfied and void the whole join group if one detected.
 * </p>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 * @version $Id$
 */
public class ASTFilterNormalizationOptimizer extends AbstractJoinGroupOptimizer {

   
   @Override
   protected void optimizeJoinGroup(
         AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode group) {  
      
      /**
       * The FILTER decomposition optimizer can be disabled with a query hint.
       */
      if (!group.getProperty(
         QueryHints.NORMALIZE_FILTER_EXPRESSIONS, 
         QueryHints.DEFAULT_NORMALIZE_FILTER_EXPRESSIONS))
         return;

      normalizeAndDecomposeFilters(ctx, sa, bSets, group);
      removeDuplicatesAndTautologies(ctx, sa, bSets, group);
      
   }
   
   /**
    * Bring all FILTERs into CNF and split them at top-level to contain
    * simpler FILTER expressions.
    */
   protected void normalizeAndDecomposeFilters(
         AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode group) {  
      
      // substitution map (collected in upcoming for loop)
      final Map<FilterNode, List<List<FilterNode>>> subst = 
         new HashMap<FilterNode, List<List<FilterNode>>>();
 
      for (IGroupMemberNode child : group) {
         
         if (child instanceof FilterNode) {
            
            final FilterNode filterNode = (FilterNode)child;
            final IValueExpressionNode vexprNode =  
               filterNode.getValueExpressionNode();
            

            // don't even try if not optimizable
            if (isDecomposable(vexprNode)) {
   
               // otherwise, check if the node is already in CNF
               final IValueExpressionNode filterAsCNF = 
                  StaticAnalysis.isCNF(vexprNode) ? 
                  vexprNode : StaticAnalysis.toCNF(vexprNode);
   
               if (filterAsCNF!=null) {
   
                  final List<FilterNode> splittedFilterNodes = 
                     constructFiltersForValueExpressionNode(
                        filterAsCNF, new ArrayList<FilterNode>());
                                          
                  if (splittedFilterNodes!=null) {
                        
                     if (!subst.containsKey(filterNode)) {
                        subst.put(filterNode,new ArrayList<List<FilterNode>>());
                     }
                        
                     final List<List<FilterNode>> values = subst.get(filterNode);
                     values.add(splittedFilterNodes);
                        
                  } // else: something went wrong in conversion, be conservative
                     
               } // else: something wrong in conversion, be conservative
                  
            }
         }
      }

      // remove the original filter nodes that were decomposed
      for (final FilterNode filterNode : subst.keySet()) {

         // remove child as often as we encountered the filter node
         for (int i=0; i<subst.get(filterNode).size(); i++) {
            group.removeChild(filterNode);
         }
      }

      for (final List<List<FilterNode>> filterNodeListList : subst.values()) {
         for (final List<FilterNode> filterNodeList : filterNodeListList) {
            for (final FilterNode filterNode : filterNodeList) {
               
               group.addChild(filterNode);
            }
         }
      }
   }
   
   /**
    * Remove duplicate FILTERs and tautologies
    */
   protected void removeDuplicatesAndTautologies(
         AST2BOpContext ctx, StaticAnalysis sa,
         IBindingSet[] bSets, JoinGroupNode group) {  
      
       // variables that are definitely bound *after* executing the group
      final Set<IVariable<?>> definitelyProd = new HashSet<IVariable<?>>();
      sa.getDefinitelyIncomingBindings(group, definitelyProd);
      sa.getDefinitelyProducedBindings(group, definitelyProd, true);
      
      // variables that are maybe bound *after* executing the group
      final Set<IVariable<?>> maybeProd = new HashSet<IVariable<?>>();
      sa.getMaybeIncomingBindings(group, maybeProd);
      sa.getMaybeProducedBindings(group, maybeProd, true);

      // record the filters we've already seen, to remove duplicates
      final Set<FilterNode> alreadySeen = new HashSet<FilterNode>();
      final List<FilterNode> filtersToRemove = new ArrayList<FilterNode>();
      for (int i=group.size()-1; i>=0; i--) {
         
         final BOp child = group.get(i);
         if (child instanceof FilterNode) {
            
            final FilterNode filterNode = (FilterNode)child;
            
            /**
             * First check whether the FILTER is trivially satisfied or has
             * bee encountered before.
             */
            if (filterAlwaysSatisifed(filterNode, definitelyProd, maybeProd) 
                || alreadySeen.contains(filterNode)) {
               
               filtersToRemove.add(filterNode);
               
            }
            
            alreadySeen.add(filterNode); // mark as already seen
         }
      }

      /*
       * Remove the original filter nodes that were decomposed.
       * 
       * The reason why we iterate in inverse order is a small problem (bug)
       * in the removeChild method: the method always removes the first
       * matching child, but resets the parent pointer of the passed argument.
       * To keep this in synch, we need to make sure that we pass arguments in
       * order; given that the arguments were extracten in inverse order, this
       * can be reached by inverse iteration again.
       */
      for (int i=filtersToRemove.size()-1; i>=0; i--) {
         
         final FilterNode filterNode = filtersToRemove.get(i);
         group.removeChild(filterNode);
      }

   }

   
   /**
    * Checks whether the given filter node is trivially satisfied
    * 
    * @param filterNode
    * @param definiteVars
    * @param maybeVars
    * @return
    */
   boolean filterAlwaysSatisifed(FilterNode filterNode,
         Set<IVariable<?>> definiteVars, Set<IVariable<?>> maybeVars) {
      
      boolean alwaysSatisifed = false;
      
      alwaysSatisifed |=
         boundAlwaysSatisifed(filterNode.getValueExpressionNode(),definiteVars);
      
      alwaysSatisifed |=
         notBoundAlwaysSatisifed(filterNode.getValueExpressionNode(),maybeVars);
      
      // note: you may add further special cases here in future ...
      
      return alwaysSatisifed;
   }

   /**
    * Checks whether the value expression node vexp is of the form
    * <tt>bound(?var)</tt> where ?var is contained in the definite vars,
    * meaning that we encountered a bound expression that is always true.
    * 
    * @param vexp
    * @param definiteVars
    */
   private boolean boundAlwaysSatisifed(
      final IValueExpressionNode vexp, final Set<IVariable<?>> definiteVars) {
      
      if (!(vexp instanceof FunctionNode)) {
         return false; //wrong pattern
      }
      
      final FunctionNode functionNode = (FunctionNode)vexp;
      final URI functionURI = functionNode.getFunctionURI();
      
      if (functionURI.equals(FunctionRegistry.BOUND)) {
         
         if (functionNode.arity()==1) {
            BOp varBop = functionNode.get(0);
            if (varBop instanceof VarNode) {
               VarNode varNode = (VarNode)varBop;
               return definiteVars.contains(varNode.getValueExpression());
            }
         }
         
      }

      return false; // pattern not matched
   }

   /**
    * Checks whether the value expression node vexp is of the form
    * <tt>not(bound(?var))</tt> where ?var is not contained in the maybe vars,
    * meaning that we encountered a not bound expression that is always true.
    * 
    * @param vexp
    * @param maybeVars
    */
   private boolean notBoundAlwaysSatisifed(
      final IValueExpressionNode vexp, final Set<IVariable<?>> maybeVars) {
      
      if (!(vexp instanceof FunctionNode)) {
         return false; //wrong pattern
      }
      
      final FunctionNode functionNode = (FunctionNode)vexp;
      final URI functionURI = functionNode.getFunctionURI();
      
      if (functionURI.equals(FunctionRegistry.NOT)) {
         
         final ValueExpressionNode innerVexp =
            (ValueExpressionNode)functionNode.get(0);
         
         if (!(innerVexp instanceof FunctionNode)) {
            return false; // wrong pattern
         }
         
         final FunctionNode innerFunctionNode = (FunctionNode)innerVexp;
         final URI innerFunctionURI = innerFunctionNode.getFunctionURI();

         if (innerFunctionURI.equals(FunctionRegistry.BOUND)) {

            if (innerFunctionNode.arity()==1) {
               BOp varBop = innerFunctionNode.get(0);
               if (varBop instanceof VarNode) {
                  VarNode varNode = (VarNode)varBop;
                  return !maybeVars.contains(varNode.getValueExpression());
               }
            }

         }
      }

      return false; // pattern not matched      
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
