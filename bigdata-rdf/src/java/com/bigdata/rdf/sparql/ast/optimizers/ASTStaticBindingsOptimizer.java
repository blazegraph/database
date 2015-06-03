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
 * Created on May 14, 2015
 */
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ComputedMaterializationRequirement;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupMemberValueExpressionNodeBase;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SolutionSetStatserator;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * <p>
 * Optimizer that aims at the optimization of SPARQL 1.1 pattern detecting
 * static (i.e., non runtime dependent) binding for variables, moving
 * them to the top-level of the query where possible, and attaching them with
 * occurrences of the variable.
 * </p>
 * 
 * <p>
 * The optimizer may apply to the following construct:
 * 
 * (A) BIND + VALUES clauses with constant values; 
 * (B) FILTER IN clauses with URIs; 
 * (C) FILTERs with sameTerm(?x,<http://someUri>) and ?x=<http://someUri> clauses
 * </p>
 * 
 * <p>
 * It proceeds as follows:
 * </p>
 * 
 * <p>
 * 1. Identify the constructs mentioned above in the query and record identified
 *    static bindings. We distinguish between produced static bindings, namely
 *    constructs that introduce bindings (case A above) and enforced static
 *    bindings (cases B and C) above. See class {@link StaticBindingInfo},
 *    which is used to store binding information. For case A, we also remove
 *    the construct producing the bindings, as they will later be added to
 *    as static bindings to the query top level (see step 3a below).
 * </p>
 * 
 * <p>
 * 2. Along the way, we record usages of all variables, see class 
 *    {@link VariableUsageInfo}. These usages (i) may be used for inlining
 *    in a later step, whenever we detected static bindings and (b) are used
 *    to decide whether static bindings can be considered global. There are
 *    some pitfalls here, in particular with FILTER expressions, see the code
 *    for in-depth comments.
 * </p>
 * 
 * <p>
 * 3. Having extracted the static bindings and the associated variable usages,
 *    we proceed as follows: 
 *    
 *    3a. Produced bindings are moved to the top-level. For the main query
 *        this means inserting them into the set of exogeneous bindings,
 *        for subqueries we construct a VALUES clause. 
 *    
 *    3b. Both the produced and enforced bindings are "inlined" to identified
 *        usages of the variable. This essentially means replacing occurrences
 *        of the variable through a hybrid of Variable+Constant. This info
 *        can then later be used in further optimization steps and at runtime.
 * </p>
 * 
 * <p>
 * 4. As a side effect, the optimizer re-initializes the {@link StaticAnalysis}
 *    class with the statically known bindings for the top-level query, which
 *    againg may be valuable input for further optimization.
 * </p>
 * 
 * <p>
 * Note: This optimizer generalizes the {@link ASTSimpleBindingsOptimizer}, the
 *       {@link ASTValuesOptimizer}, and the {@link ASTBindingAssigner}, which
 *       have been disabled and marked deprecated
 * </p>
 *     
 * <p>
 * The following extensions are not considered crucial for now, but might be
 * subject to future work on this optimizer: (i) we may want to decompose
 * FILTERs prior to running this optimizer, which may be useful to identify.
 * </p>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTStaticBindingsOptimizer implements IASTOptimizer {

   @Override
   public QueryNodeWithBindingSet optimize(
      final AST2BOpContext context, final QueryNodeWithBindingSet input) {

      final IQueryNode queryNode = input.getQueryNode();
      final IBindingSet[] bindingSets = input.getBindingSets();     


      /**
       * We collect statically enforced bindings in this binding set, which
       * will later be post processed and injected (by joining all bindings).
       */
      final StaticBindingInfo staticBindingInfo = 
         new StaticBindingInfo(bindingSets); 

      /**
       * In case the binding sets variable is null, nothing needs to be done 
       * (query will return empty result anyways). Also, this optimizer is
       * only applicable to top-level queries.
       */
      if (bindingSets == null || !(queryNode instanceof QueryRoot)) {
         return new QueryNodeWithBindingSet(queryNode, bindingSets);
      }

      final QueryRoot queryRoot = (QueryRoot) queryNode;
      
      final VariableUsageInfo varUsageInfo = new VariableUsageInfo();

      final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);
      final Set<InlineTasks> inlineTasks = new HashSet<InlineTasks>();
      
      /**
       * Setup inlining tasks for existing bindings in the binding set
       */
      ISolutionSetStats stats = SolutionSetStatserator.get(bindingSets);
      final Set<IVariable<?>> usedVars = stats.getUsedVars();
      if (!usedVars.isEmpty()) {
         // extract information about used vars from a top-level perspective
         final VariableUsageInfo childVarUsageInfo = new VariableUsageInfo();
         childVarUsageInfo.extractVarSPUsageInfoChildrenOrSelf(queryRoot.getWhereClause());

         // set up inlining task
         for (IVariable<?> var : usedVars) {
            if (childVarUsageInfo.varUsed(var)) {
               inlineTasks.add(
                  new InlineTasks(var, childVarUsageInfo.getVarUsages(var)));
            }
         }         
      }
      
      /**
       * Apply the optimization (eliminating static binding producing constructs
       * from the query and collecting the staticBindings set)
       */
      final IBindingSet[] bindingSetsOut =
         optimize(
            sa, queryRoot, staticBindingInfo, varUsageInfo,
            inlineTasks, true /* isRootQuery */, 
            queryRoot.getBindingsClause());
      
     /**
       * Propagate information about changed binding sets to the context,
       * for later reuse.
       */
      context.setSolutionSetStats(SolutionSetStatserator.get(bindingSetsOut));
      
      return new QueryNodeWithBindingSet(queryRoot, bindingSetsOut);
   }
   
   
   /**
    * Applies the optimization (main entry point).
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   private IBindingSet[] optimize(
         final StaticAnalysis sa,
         final QueryBase queryRoot, final StaticBindingInfo staticBindingInfo,
         final VariableUsageInfo ancestorVarUsageInfo,
         final Set<InlineTasks> inlineTasks, boolean isRootQuery,
         final BindingsClause bindingsClause) {
      
      /**
       * First, collect static bindings from the VALUES clause. In case of
       * the root query, those will be added to the exogeneous binding set;
       * in case of subqueries, we'll write a new VALUES clause in the end.
       */
      if (bindingsClause!=null) {
         
         List<IBindingSet> bs = bindingsClause.getBindingSets();
         IBindingSet[] bsList = bs.toArray(new IBindingSet[bs.size()]);

         // register produced bindings
         staticBindingInfo.addProduced(bs);
         
         // remove the bindings clause
         queryRoot.setBindingsClause(null);

         // extract information about used vars from a top-level perspective
         final VariableUsageInfo childVarUsageInfo = new VariableUsageInfo();
         childVarUsageInfo.extractVarSPUsageInfoChildrenOrSelf(queryRoot.getWhereClause());
         
         // set up inlining task
         final Set<IVariable<?>> usedVars = SolutionSetStatserator.get(bsList).getUsedVars();
         for (IVariable<?> var : usedVars) {
            if (childVarUsageInfo.varUsed(var)) {
               inlineTasks.add(
                  new InlineTasks(var, childVarUsageInfo.getVarUsages(var)));
            }
         }
         
      }
      
      optimize(
         sa, queryRoot.getWhereClause(), staticBindingInfo,
         ancestorVarUsageInfo, inlineTasks);


      /**
       * Apply inlining tasks
       */
      final Map<IVariable<?>, IConstant<?>> constantVars = 
         SolutionSetStatserator.get(staticBindingInfo.joinAll()).getConstants();
      for (InlineTasks inlineTask : inlineTasks) {
         
         final IVariable<IV> var = inlineTask.getVar();
         if (constantVars.containsKey(var)) {

            inlineTask.apply((Constant<IV>)constantVars.get(var));
         
         } // else: not applicable
      }
      
      /**
       * Return the binding set, leaving the caller the decision on what to
       * do with it (i.e., adding it to the exogeneous bindings or constructing
       * a VALUES clause).
       */
      
      final IBindingSet[] bindingSetsOut = staticBindingInfo.joinProduced();
      return bindingSetsOut;
   }
   
   /**
    * Applies the optimization to a given subquery root.
    */
   private IBindingSet[] optimize(
         final StaticAnalysis sa, final SubqueryRoot subqueryRoot, 
         final StaticBindingInfo staticBindingInfo,
         final VariableUsageInfo ancestorVarUsageInfo,
         final Set<InlineTasks> inlineTasks) {
      
      final IBindingSet[] staticBindings =
         optimize(
            sa, subqueryRoot, staticBindingInfo, ancestorVarUsageInfo,
            inlineTasks, false /* isRootQuery */, 
            subqueryRoot.getBindingsClause());


      // record static bindings in subquery VALUES clause, if any
      final LinkedHashSet<IVariable<?>> bcVars = new LinkedHashSet<IVariable<?>>();
      bcVars.addAll(SolutionSetStatserator.get(staticBindings).getUsedVars());
         
      final List<IBindingSet> bcBindings = Arrays.asList(staticBindings);

      if (!bcVars.isEmpty()) {
         final BindingsClause bc = new BindingsClause(bcVars,bcBindings);
         
         subqueryRoot.setBindingsClause(bc);
      }
      
      return staticBindings;
   }

   /**
    * Applies the optimization to all constructs delivering definitely produced
    * bindings, namely (i) eliminates these constructs while (ii) recording the
    * static bindings as List<IBindingSet> in the staticBindings parameter.
    * 
    * @param group a join group
    * @param staticBindings the list of detected static bindings
    * @return
    */
   @SuppressWarnings({ "rawtypes", "unchecked" })
   private void optimize(
      final StaticAnalysis sa, final GroupNodeBase<?> group, 
      final StaticBindingInfo staticBindingInfo,
      final VariableUsageInfo ancestorVarUsageInfo,
      final Set<InlineTasks> inlineTasks) {
      
      if (group == null) {
         return;
      }
              
      // only process non-optional patterns
      if (group instanceof JoinGroupNode && ((JoinGroupNode) group).isOptional()) {
         return;
      }
      
      // can't do anything here in the general case
      if (group instanceof UnionNode) {
         // Note: we may look up BIND/VALUES clauses that co-appear in both 
         //       parts of the UNION here. This is considered an edge case
         //       though, so we omit the implementation for now.
         return;
      }
     
      /*
       * Collect information about which variables are used where, for later
       * inlining. Further, this information is required to make sure that
       * we do not make static bindings exogeneous for some variable ?x for
       * which a filter exists in the same scope or in the parent scope,
       * which would lead to the case that this variable is effectively
       * considered bound in the filter scope where it shouldn't (such cases
       * will be captured by rewritings in the ASTBottomUpOptimizer later on).
       **/
      final VariableUsageInfo selfVarUsageInfo = new VariableUsageInfo();
      selfVarUsageInfo.extractVarUsageInfoSelf(group);
      final VariableUsageInfo ancOrSelfVarUsageInfo = 
         VariableUsageInfo.merge(ancestorVarUsageInfo, selfVarUsageInfo);


      /**
       * Compute usage info for the node's children (to be reused later).
       */
      final VariableUsageInfo childVarUsageInfo = new VariableUsageInfo();
      childVarUsageInfo.extractVarSPUsageInfoChildren(group);

      
      /**
       * Collect the children that introduce static bindings for later removal.
       */
      final List<IGroupMemberNode> toRemove = new ArrayList<IGroupMemberNode>();
      for (IGroupMemberNode child : group) {
                  
         if (child instanceof AssignmentNode) {
            
            final AssignmentNode an = (AssignmentNode)child;
            final IValueExpression<?> ve = an.getValueExpression();
            
            /**
             * We apply the optimization in case (i) the value expression is a 
             * constant that is (ii) represented through a ConstantNode. Note
             * that value expressions not represented through ConstantNodes
             * (such as, e.g., CONCAT("a", "b") in principle are amenable to
             * the optimization as well, but their constructed values have not
             * yet been joined against the dictionary and later joins might
             * fail. We could add this dictionary resolving step for the
             * constructed static bindings (i.e., the exogeneous bindings),
             * but this is something we'd need to do before applying the
             * inlining. For now, this is out of scope. Maybe a simpler to
             * implement strategy would be to resolve these nodes in a prior
             * phase and look them up, then leaving this code here unchanged.
             **/
            if (ve instanceof IConstant && 
                  an.args().get(1) instanceof ConstantNode) {

               IVariable<?> boundVar = an.getVar();
               
               if (!ancOrSelfVarUsageInfo.varUsedInFilterOrAssignment(boundVar)) {
                  
                  final IBindingSet bs = new ListBindingSet();
                  bs.set(boundVar, (IConstant)an.getValueExpression());
                  staticBindingInfo.addProduced(bs);
                  
                  toRemove.add(child);
                  
                  final VariableUsageInfo usageInfo =
                     VariableUsageInfo.merge(
                        ancOrSelfVarUsageInfo, childVarUsageInfo);
                  
                  // add inline tasks for variable
                  if (usageInfo.varUsed(boundVar)) {
                     inlineTasks.add(
                        new InlineTasks(
                           boundVar, usageInfo.getVarUsages(boundVar)));
                  }
               }               
            }
                        
         // case 2: optimize in-line bindings clauses
         } else if (child instanceof BindingsClause) {

            final BindingsClause bc = (BindingsClause)child;
            
            final List<IBindingSet> bss = bc.getBindingSets();
            final Set<IVariable<?>> bssVars = varsInBindingSet(bss);
            
            boolean someVarUsedInFilter = false;
            for (IVariable<?> bssVar : bssVars) {
               
               someVarUsedInFilter |= 
                  ancOrSelfVarUsageInfo.varUsedInFilterOrAssignment(bssVar);
               
            }
            
            boolean inline = !someVarUsedInFilter;
            if (inline) {

               staticBindingInfo.addProduced(bc.getBindingSets());
               toRemove.add(child);
               
               final VariableUsageInfo usageInfo =
                     VariableUsageInfo.merge(
                        ancOrSelfVarUsageInfo, childVarUsageInfo);
               
               // add inline tasks for all variables, where applicable
               for (IVariable<?> bssVar : bssVars) {
                  
                  if (usageInfo.varUsed(bssVar)) {
                     inlineTasks.add(
                        new InlineTasks(bssVar, usageInfo.getVarUsages(bssVar))); 
                  }
               }
            }
            
         // case 3: optimize filter nodes inducing static bindings
         } else if (child instanceof FilterNode) {
            
            FilterNode filter = (FilterNode)child;
            final IValueExpressionNode vexpr = filter.getValueExpressionNode();
            
            if(!(vexpr instanceof FunctionNode))
                return;
            
            final FunctionNode functionNode = (FunctionNode) vexpr;
            
            final URI functionURI = functionNode.getFunctionURI();
            
            // these are the constructs that may lead to static bindings
            if (functionURI.equals(FunctionRegistry.SAME_TERM) ||
                functionURI.equals(FunctionRegistry.EQ)) {
               
               final IValueExpressionNode left =
                  (IValueExpressionNode) functionNode.get(0);
             
               final IValueExpressionNode right = 
                  (IValueExpressionNode) functionNode.get(1);
               
               final IBindingSet bs = new ListBindingSet();
               if (left instanceof VarNode && right instanceof ConstantNode) {
                  
                  final IV constant = ((ConstantNode) right).getValueExpression().get();
                  
                  // we cannot do the inline for EQ when then constant is a literal
                  if (functionURI.equals(FunctionRegistry.EQ) && constant.isLiteral())
                     return;
                  
                  bs.set(
                     (IVariable)left.getValueExpression(),
                     (IConstant)right.getValueExpression());
                  
               } else if (left instanceof ConstantNode && right instanceof VarNode) {
                  
                  final IV constant = ((ConstantNode) left).getValueExpression().get();
                  
                  // we cannot do the inline for EQ when then constant is a literal
                  if (functionURI.equals(FunctionRegistry.EQ) && constant.isLiteral())
                     return;
                  
                  bs.set(
                     (IVariable)right.getValueExpression(),
                     (IConstant)left.getValueExpression());

               }
               
               if (!bs.isEmpty() && bs.size()==1) { // true by construction

                  staticBindingInfo.addEnforced(bs);
                  
                  final IVariable<?> var = bs.vars().next();
                  
                  final VariableUsageInfo usageInfo =
                        VariableUsageInfo.merge(
                           ancOrSelfVarUsageInfo, childVarUsageInfo);
                     
                  // add inline tasks for variable
                  if (usageInfo.varUsed(var)) {
                     inlineTasks.add(
                        new InlineTasks(var, usageInfo.getVarUsages(var)));
                  }
                  
               } // just in case: can't handle, ignore
               
            } else if (functionURI.equals(FunctionRegistry.IN)) {
               
               final int arity = functionNode.arity();
               
               final List<IBindingSet> bsList =
                     new ArrayList<IBindingSet>(arity-1);
               int i=0;
               VarNode varNode = (VarNode)functionNode.get(i++);
               final IVariable<IV> var = (IVariable<IV>)(varNode.getValueExpression());

               // collect constants and check for literals
               final List<IConstant> constants = new ArrayList<IConstant>();
               boolean containsLiteral = false;
               while (i<arity) {

                  final BOp inValue = functionNode.get(i++);
                  if (inValue instanceof ConstantNode) {
                     ConstantNode in = (ConstantNode)inValue;
                     containsLiteral |= in.getValueExpression().get().isLiteral();
                  }
               }

               // we only perform changes if there's no literal
               if (!containsLiteral) {
                  for (IConstant c : constants) {
                     IBindingSet bs = new ListBindingSet();
                     bs.set(var,c);
                     bsList.add(bs);                        
                  }
         
                  staticBindingInfo.addEnforced(bsList);
                  
                  final VariableUsageInfo usageInfo =
                        VariableUsageInfo.merge(
                           ancOrSelfVarUsageInfo, childVarUsageInfo);
                     
                  // add inline tasks for variable
                  if (usageInfo.varUsed(var)) {
                     inlineTasks.add(
                        new InlineTasks(var, usageInfo.getVarUsages(var))); 
                  }
               } 
               
            } // else: there's nothing obvious we can do

         }
      
      }  
      
      /**
       * Remove the children for which static bindings were extracted
       */
      for (IGroupMemberNode node : toRemove) {
         while (group.removeArg(node)) {
            // repeat
         }
      }
              
      // recurse into the childen
      for (IGroupMemberNode child : group) {
         
         if (child instanceof GroupNodeBase) {
            
            optimize(sa,
               (GroupNodeBase<?>) child, staticBindingInfo, 
               ancOrSelfVarUsageInfo, inlineTasks);
            
         } else if (child instanceof SubqueryRoot) {

            /**
             * Apply to subquery, voiding all collected information to acocunt
             * for the new scope induced by the subquery.
             */
            optimize(sa,
                  (SubqueryRoot) child, 
                  new StaticBindingInfo(), 
                  new VariableUsageInfo(), 
                  new HashSet<InlineTasks>());
         }
      }
   }

   @SuppressWarnings("rawtypes")
   private Set<IVariable<?>> varsInBindingSet(final List<IBindingSet> bss) {
      Set<IVariable<?>> bssVars = new HashSet<IVariable<?>>();
      for (int i=0; i<bss.size(); i++) {
         IBindingSet bs = bss.get(i);
         
         Iterator<IVariable> bsVars = bs.vars();
         while (bsVars.hasNext()) {
            bssVars.add(bsVars.next());
         }
         
      }
      return bssVars;
   }

   /**
    * Helper class used to record usage of a given variable, i.e. linking
    * variables to constructs in which they occur.
    * 
    * @author msc
    */
   public static class VariableUsageInfo {
      
      Map<IVariable<?>,List<IQueryNode>> usageMap;
      
      public VariableUsageInfo() {
         usageMap = new HashMap<IVariable<?>,List<IQueryNode>>();
      }
      
      /**
       * Returns true if a usage record for the given variable inside a FILTER
       * or BIND/VALUES node has been recorded. The rationale of this check is
       * as follows: if a variable used inside a static binding is queries in
       * such an ancestor filter, the binding must not be considered global.
       * As an example, consider the following query:
       * 
       * <pre>
       * SELECT * WHERE {
       *    FILTER(!bound(?x))
       *    {
       *      BIND(1 AS ?x)
       *    }
       *  } 
       * 
       * Expected result is the empty set, according to bottom-up semantics.
       * We must *not* rewrite this query as 
       * 
       * SELECT * WHERE {
       *   FILTER(!bound(?x))
       *  } VALUES ?x { 1 }
       *  
       * The reason is that in this case the FILTER will pass, while ?x in the
       * original query the BIND(1 AS ?x) is in scope and the filter fails.
       *  
       * The same holds for queries where the FILTER is at the same level
       * as the BIND/VALUES clause, such as:
       * 
       * SELECT * WHERE {
       *    FILTER(!bound(?x))
       *    {
       *      BIND(1 AS ?x)
       *    }
       *  } 
       *  
       * @param var
       * @return
       */
      public boolean varUsedInFilterOrAssignment(IVariable<?> var) {
         
         if (!usageMap.containsKey(var)) {
            return false;
         }
         
         List<IQueryNode> varOccurrences = usageMap.get(var);
         for (int i=0; i<varOccurrences.size(); i++) {
            IQueryNode n = varOccurrences.get(i);
            if (n instanceof FilterNode || n instanceof AssignmentNode) {
               return true;
            }
         }
         
         return false; // no filter occurrence detected
      }

      /**
       * Returns true iff usage records for the var are available.
       * 
       * @param var
       * @return
       */
      public boolean varUsed(IVariable<?> var) {
         
         return usageMap.containsKey(var) && !usageMap.get(var).isEmpty();
         
      }
      
      /**
       * Returns the list of variable usages.
       * 
       * @param var
       * @return
       */
      public List<IQueryNode> getVarUsages(IVariable<?> var) {
         
         return usageMap.get(var);
      }
      
      /**
       * Gets the map recoding all usages of all variables (internal data
       * structure maintained by the class).
       * 
       * @return
       */
      public Map<IVariable<?>,List<IQueryNode>> getUsageMap() {
         return usageMap;
      }
      
      /**
       * Extracts information (variables and #occurrences) of variables used
       * inside filters or the value expressions of assignment nodes in the
       * group node base, not recursing into children.
       * 
       * @param group the group where to extract filter var info (non-recursively)
       * 
       * @return has map with mapping filter variables to their number of
       *          occurrences, containing all filter vars occurring >= 1 times
       */
      public void extractVarUsageInfoSelf(GroupNodeBase<?> group) {
         
         for (IQueryNode node : group) {

            if (node instanceof FilterNode || node instanceof AssignmentNode) {

               final GroupMemberValueExpressionNodeBase filter = 
                     (GroupMemberValueExpressionNodeBase) node;
      
               final IValueExpressionNode vexpr = filter.getValueExpressionNode();
               
               extractVarUsageInfo(node, (IValueExpressionNode)vexpr);
               
            } else if (node instanceof StatementPatternNode) {
               
               StatementPatternNode spn = (StatementPatternNode)node;

               for (IVariable<?> spnVar : spn.getProducedBindings()) {
                  
                  // init list, if necessary
                  if (!usageMap.containsKey(spnVar)) {
                     usageMap.put(spnVar, new ArrayList<IQueryNode>());
                  }
      
                  // add node to list
                  usageMap.get(spnVar).add(node);
               }
               
            }

         }
      }
      
      private void extractVarUsageInfo(IQueryNode toRewrite, IValueExpressionNode node) {

         final BOp nodeAsBop = (BOp)node;
         final int arity = nodeAsBop.arity();
         for (int i=0; i<arity; i++) {
            
            final BOp child = nodeAsBop.get(i);
            if (child instanceof VarNode) {
               
               final VarNode varNode = (VarNode)child;
               final IVariable<?> iVar = varNode.getValueExpression();
               if (!usageMap.containsKey(iVar)) {
                  usageMap.put(iVar, new ArrayList<IQueryNode>());
               }
   
               // add node to list
               usageMap.get(iVar).add(toRewrite);
               
            } else if (child instanceof IValueExpressionNode) {
               
               // recurse
               extractVarUsageInfo(toRewrite, (IValueExpressionNode)child);
               
            }
         }
      }



      /**
       * Extracts usage information for the variable inside statement patterns
       * to children of the current node (ignoring the node itself).
       * 
       * Note that we do not report variable usage in filters, as those
       * might be not in scope and inlining is not safe (interference with
       * {@link ASTBottomUpOptimizer#handleFiltersWithVariablesNotInScope})!).
       * This maybe somewhat too strict (leading to situations where we do
       * not inline static variable bindings), but is safe.
       * 
       * @param group
       */
      @SuppressWarnings("rawtypes")
      public void extractVarSPUsageInfoChildren(GroupNodeBase<?> group) {

         for (IQueryNode child : group) {
            
            if (child instanceof GroupNodeBase) {

               extractVarSPUsageInfoChildrenOrSelf((GroupNodeBase)child);
               
            }
         }
      }
      
      /**
       * Extracts usage information for the variable inside statement patterns
       * to the current node itself and its children.
       * 
       * Note that we do not report variable usage in filters, as those
       * might be not in scope and inlining is not safe (interference with
       * {@link ASTBottomUpOptimizer#handleFiltersWithVariablesNotInScope})!).
       * This maybe somewhat too strict (leading to situations where we do
       * not inline static variable bindings), but is safe.
       * 
       * @param group
       */
      @SuppressWarnings("rawtypes")
      public void extractVarSPUsageInfoChildrenOrSelf(GroupNodeBase<?> group) {
         
         // abort for optional patterns
         if (group instanceof JoinGroupNode && ((JoinGroupNode) group).isOptional()) {
            return;
         }
         
         for (IQueryNode child : group) {
            
            if (child instanceof GroupNodeBase) {

               extractVarSPUsageInfoChildrenOrSelf((GroupNodeBase)child);
               
            } else if (child instanceof StatementPatternNode) {
               
               StatementPatternNode spn = (StatementPatternNode)child;

               for (IVariable<?> spnVar : spn.getProducedBindings()) {
                  
                  // init list, if necessary
                  if (!usageMap.containsKey(spnVar)) {
                     usageMap.put(spnVar, new ArrayList<IQueryNode>());
                  }
      
                  // add node to list
                  usageMap.get(spnVar).add(child);
                  
               }
               
            } else if (child instanceof GroupNodeBase) {

               extractVarSPUsageInfoChildrenOrSelf((GroupNodeBase)child);
               
            }
         }
      }
      
      
      /**
       * Merges two {@link VariableUsageInfo}, creating a new one containing
       * the merged information. The original objects are not modified.
       */
      public static VariableUsageInfo merge(
         final VariableUsageInfo x, final VariableUsageInfo y) {
         
         final VariableUsageInfo merged = new VariableUsageInfo();
         final Map<IVariable<?>, List<IQueryNode>> usageMap =
               merged.getUsageMap();
         
         final Map<IVariable<?>, List<IQueryNode>> xUsageMap = x.getUsageMap();
         for (IVariable<?> var : xUsageMap.keySet()) {
            
            // make sure there's an entry for the key ...
            if (!usageMap.containsKey(var))
               usageMap.put(var, new ArrayList<IQueryNode>());
            
            // ... and perform merge
            usageMap.get(var).addAll(xUsageMap.get(var));
         }
         
         final Map<IVariable<?>, List<IQueryNode>> yUsageMap = y.getUsageMap();
         for (IVariable<?> var : yUsageMap.keySet()) {
            
            // make sure there's an entry for the key ...
            if (!usageMap.containsKey(var))
               usageMap.put(var, new ArrayList<IQueryNode>());
            
            // ... and perform merge
            usageMap.get(var).addAll(yUsageMap.get(var));
         }

         return merged;
      }
   }
   
   /**
    * We distinguish between static bindings that are produced (via a BIND
    * or VALUES node) and static bindings that are enforced (via FILTER nodes).
    * 
    * Constructs implying produced static bindings can be eliminated when
    * moving static bindings to the query top-level, enforced static bindings
    * cannot (removing them to the top-level may induce duplicates).
    * 
    * Both produced and enforced static bindings can be applied to variables
    * in the query.
    * 
    * @author msc
    */
   public static class StaticBindingInfo {
      
      final List<List<IBindingSet>> produced;
      final List<List<IBindingSet>> enforced;
      
      final IBindingSet[] queryInput;

      /**
       * Constructor with empty input binding set.
       */
      public StaticBindingInfo() {
         
         this.produced = new ArrayList<List<IBindingSet>>();
         this.enforced = new ArrayList<List<IBindingSet>>();
         this.queryInput = new IBindingSet[] { new ListBindingSet() };
      }

      /**
       * Constructor with given input binding set.
       */
      public StaticBindingInfo(IBindingSet[] queryInput) {
         
         this.produced = new ArrayList<List<IBindingSet>>();
         this.enforced = new ArrayList<List<IBindingSet>>();
         this.queryInput = queryInput;
         
      }

      public void addProduced(IBindingSet bs) {
         produced.add(wrap(bs));
      }

      public void addProduced(List<IBindingSet> bsList) {
         produced.add(bsList);
      }

      public void addEnforced(IBindingSet bs) {
         enforced.add(wrap(bs));
      }

      public void addEnforced(List<IBindingSet> bsList) {
         enforced.add(bsList);
      }

      public List<List<IBindingSet>> getProduced() {
         return produced;
      }
      
      public List<List<IBindingSet>> getEnforced() {
         return enforced;
      }

      /**
       * @return both the produced and enforced static bindings
       */
      public List<List<IBindingSet>> getAll() {
         List<List<IBindingSet>> all = new ArrayList<List<IBindingSet>>();
         all.addAll(produced);
         all.addAll(enforced);
         return all;
      }
      
      public IBindingSet[] joinProduced() {
         return join(produced);
      }

      public IBindingSet[] joinEnforced() {
         return join(enforced);
      }

      public IBindingSet[] joinAll() {
         return join(getAll());         
      }
      
      private List<IBindingSet> wrap(IBindingSet bs) {
         final List<IBindingSet> bsList = new ArrayList<IBindingSet>();
         bsList.add(bs);
         return bsList;
      }

      /**
       * Joins the staticBindings with the queryInput binding set, returning
       * the resulting binding set.
       */
      private IBindingSet[] join(List<List<IBindingSet>> staticBindings) {
            
         if (queryInput == null || queryInput.length == 0) {
            return queryInput; // nothing to be done
         }

         // we join everything together the statically derived bindings in a
         // nested loop fashion; typically, we may expect one binding, but this
         // may also result in multiple binding sets (or the empty binding set)
         // in the general case
         List<IBindingSet> leftBindingSets = Arrays.asList(queryInput);

         for (List<IBindingSet> staticBinding : staticBindings) {

            final List<IBindingSet> tmp = new LinkedList<IBindingSet>();

            for (IBindingSet left : leftBindingSets) {

               final Iterator<IBindingSet> rightItr = staticBinding.iterator();

               while (rightItr.hasNext()) {

                  final IBindingSet right = rightItr.next();
                  final IBindingSet join = BOpContext.bind(left, right,
                        null /* constraints */, null /* varsToKeep */);

                  if (join != null) {
                     tmp.add(join);
                  }
               }
            }

            leftBindingSets = tmp; // prepare for next iteration
         }

         return leftBindingSets
               .toArray(new IBindingSet[leftBindingSets.size()]);
      }  
   }
   
   /**
    * Task specifying the inlining opportunities for a given variable in the
    * form of a list of query nodes in which a statically derived value for
    * the given variable can be inlined.
    * 
    * Can be applied by calling the apply method for a given constant.
    * 
    * @author msc
    */
   public static class InlineTasks {
      
      @SuppressWarnings("rawtypes")
      final private IVariable var;
      final private List<IQueryNode> nodes;
      
      @SuppressWarnings("rawtypes")
      public InlineTasks(
         final IVariable var,    
         final List<IQueryNode> nodes) {
         
         this.var = var; 
         this.nodes = nodes;
              
      }
      
      @SuppressWarnings("rawtypes")
      public IVariable getVar() {
         return var;
      }
      
      /**
       * Applies the {@link InlineTasks} for the variable through the given
       * constant to the patterns specified in the task.
       */
      @SuppressWarnings("rawtypes")
      public void apply(IConstant<IV> constant) {
         
         final IV val = constant.get();
         
         for (IQueryNode node : nodes) {
            
            apply(val, node);
         }
         
      }

      /**
       * Applies the {@link InlineTasks} for the class variable with the
       * parameter val to the given query node.
       * 
       * @param val
       * @param node
       */
      @SuppressWarnings("rawtypes")
      private void apply(final IV val, IQueryNode node) {
         
         if (node instanceof FilterNode) {
            
            FilterNode filter = (FilterNode)node;
            final IValueExpressionNode vexpr = filter.getValueExpressionNode();
            applyToValueExpressionNode(val, vexpr);
            
         } else if (node instanceof AssignmentNode) {

            applyToAssignmentNode(val, (AssignmentNode)node);
            
         } else if (node instanceof StatementPatternNode) {
            
            applyToStatementPattern(val, (StatementPatternNode)node);

         } else {
            
            // other patterns have not been recorded
            throw new IllegalArgumentException("Unexpected node type for " + node);

         }
      }

      /**
       * Applies the {@link InlineTasks} for the class variable with the
       * parameter val to the given assignment node.
       * 
       * @param val
       * @param an
       */
      @SuppressWarnings("rawtypes")
      private void applyToAssignmentNode(final IV val, final AssignmentNode an) {
         
         final IValueExpressionNode vexpr = an.getValueExpressionNode();
         applyToValueExpressionNode(val, vexpr);
         
      }

      /**
       * Applies the {@link InlineTasks} for the class variable with the
       * parameter val to the given statement pattern node.
       * 
       * @param val
       * @param spn
       */
      @SuppressWarnings("rawtypes")
      private void applyToStatementPattern(
         final IV val, final StatementPatternNode spn) {

         TermNode s = spn.s();
         TermNode p = spn.p();
         TermNode o = spn.o();
         TermNode c = spn.c();
         
         if (s!=null && s instanceof VarNode && s.get(0).equals(var)) {
            
            VarNode sVar = (VarNode)s;
            final ConstantNode constNode = 
                  new ConstantNode(
                     new Constant<IV>(sVar.getValueExpression(),val));
            spn.setArg(0, constNode);
            
         }
         
         if (p!=null && p instanceof VarNode && p.get(0).equals(var)) {

            VarNode pVar = (VarNode)p;
            final ConstantNode constNode = 
                  new ConstantNode(
                     new Constant<IV>(pVar.getValueExpression(),val));
            spn.setArg(1, constNode);
            
         }

         if (o!=null && o instanceof VarNode && o.get(0).equals(var)) {
            
            VarNode oVar = (VarNode)o;
            final ConstantNode constNode = 
                  new ConstantNode(
                     new Constant<IV>(oVar.getValueExpression(),val));
            spn.setArg(2, constNode);
            
         }

         if (c!=null && c instanceof VarNode && c.get(0).equals(var)) {
            
            VarNode cVar = (VarNode)c;
            final ConstantNode constNode = 
                  new ConstantNode(
                     new Constant<IV>(cVar.getValueExpression(),val));
            spn.setArg(3, constNode);
            
         }
      }

      /**
       * Applies the {@link InlineTasks} for the class variable with the
       * parameter val to the value expression node statement pattern node.
       * 
       * @param val
       * @param spn
       */
      @SuppressWarnings("rawtypes")
      private void applyToValueExpressionNode(
         final IV val, final IValueExpressionNode vexpr) {
         
         if(vexpr==null || !(vexpr instanceof FunctionNode))
            return;
         
         final FunctionNode functionNode = (FunctionNode) vexpr;

         // TODO: what about unary functions? will this crash -> test case
         final IValueExpressionNode left = 
            (IValueExpressionNode) functionNode.get(0);
         final IValueExpressionNode right = 
               (IValueExpressionNode) functionNode.get(1);

         if (left instanceof VarNode &&
               ((VarNode)left).get(0).equals(var)) {
            
            final ConstantNode constNode = 
               new ConstantNode(
                  new Constant<IV>(
                     ((VarNode)left).getValueExpression(),val));
            
            functionNode.setArg(0, constNode);
            
         } else if (right instanceof VarNode && 
                     ((VarNode)right).get(0).equals(var)) {

            final ConstantNode constNode = 
               new ConstantNode(
                  new Constant<IV>(
                     ((VarNode)right).getValueExpression(),val));
            
            functionNode.setArg(1, constNode);
            
         } else {
            
            // recurse (e.g. logical and, or, ...)
            applyToValueExpressionNode(val, left);
            applyToValueExpressionNode(val, right);
         }
      }
   }
   
}