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
import java.util.Map.Entry;
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
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupMemberValueExpressionNodeBase;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.PropertyPathNode;
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
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;

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
 *    bindings (cases B and C) above. Produced bindings are recorded in class
 *    {@link StaticBindingInfo}, and for them we also remove the construct
 *    producing the bindings, as they will later be added to as static bindings
 *    to the query top level (see step 3a below). See step 3b below for the
 *    treatment of enforced bindings
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
 * FILTERs prior to running this optimizer, which may be useful to identify;
 * (ii) in some cases it is also valid to propagate static bindings to
 * optional patterns; implementing this might increase the benefit of the
 * optimizer; (iii) we could implement some special handling for the variables
 * reported by {@link IEvaluationContext#getGloballyScopedVariables()}:
 * currently, they're just treated as "normal" variables and hence only inlined
 * at top-level, but it would be possible to inline them into subqueries and
 * in any nested scope as well, giving us a (in general) a better evaluation
 * plan.
 * </p>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * 
 * @version $Id$
 */
public class ASTStaticBindingsOptimizer implements IASTOptimizer {

   @SuppressWarnings({ "rawtypes", "unused", "unchecked" })
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

      // initialize variables used throughout the optimizer
      final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);
      final VariableUsageInfo varUsageInfo = new VariableUsageInfo();
      final Set<InlineTasks> inlineTasks = new HashSet<InlineTasks>();
      
      /**
       * Setup inlining tasks for existing bindings in the binding set
       */
      // extract information about used vars from a top-level perspective
      final VariableUsageInfo childVarUsageInfo = new VariableUsageInfo();
      childVarUsageInfo.extractVarSPUsageInfoChildrenOrSelf(
         queryRoot.getWhereClause());
      
      final ISolutionSetStats stats = SolutionSetStatserator.get(bindingSets);
      final Map<IVariable<?>, IConstant<?>> staticVars = 
            SolutionSetStatserator.get(bindingSets).getConstants();
      for (IVariable var : staticVars.keySet()) {
         
         if (childVarUsageInfo.varUsed(var)) {
            final IConstant value = staticVars.get(var);
            inlineTasks.add(
               new InlineTasks(var, value, childVarUsageInfo.getVarUsages(var)));
         }         
      }
      
      /**
       * Apply the optimization (eliminating static binding producing constructs
       * from the query and collecting the staticBindings set)
       */
      final IBindingSet[] bindingSetsOut =
         optimize(
            sa, queryRoot, staticBindingInfo, varUsageInfo,
            inlineTasks, queryRoot.getBindingsClause());
      
     /**
       * Propagate information about changed binding sets to the context.
       */
      context.setSolutionSetStats(SolutionSetStatserator.get(bindingSetsOut));
      
      return new QueryNodeWithBindingSet(queryRoot, bindingSetsOut);
   }
   
   
   /**
    * Applies the optimization to the given query root.
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   private IBindingSet[] optimize(
         final StaticAnalysis sa,
         final QueryBase queryRoot, final StaticBindingInfo staticBindingInfo,
         final VariableUsageInfo ancestorVarUsageInfo,
         final Set<InlineTasks> inlineTasks, 
         final BindingsClause bindingsClause) {
      
      /**
       * First, collect static bindings from the outer VALUES clause. In case 
       * of the root query, those will be added to the exogeneous binding set
       * later on; in case of subqueries, we'll merge them with other produced
       * static bindings into a new VALUES clause in the end.
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
         final Map<IVariable<?>, IConstant<?>> staticVars = 
            SolutionSetStatserator.get(bsList).getConstants();
         for (IVariable<?> var : staticVars.keySet()) {
            if (childVarUsageInfo.varUsed(var)) {
               final IConstant value = staticVars.get(var);
               inlineTasks.add(new InlineTasks(
                  var, value, childVarUsageInfo.getVarUsages(var)));
            }
         }
         
      }
      
      /**
       * The optimization phase performs rewriting & extraction of static
       * binding infos and inline tasks.
       */
      optimize(
         sa, queryRoot.getWhereClause(), staticBindingInfo,
         ancestorVarUsageInfo, inlineTasks);

      /**
       * Having gathered the inline tasks, let's apply them now.
       */
      for (InlineTasks inlineTask : inlineTasks) {
         inlineTask.apply();
      }
      
      /**
       * Return the binding set, leaving the caller the decision on what to
       * do with it (i.e., adding it to the exogeneous bindings or constructing
       * a VALUES clause). The binding set is constructed by joining all
       * produced bindings. Note that this is the case for the outer query;
       * bindings for inner queries (where we add them to their VALUES clause
       * are treated in the optimize method for the subquery root).
       */
      final IBindingSet[] bindingSetsOut = staticBindingInfo.joinProduced();
      return bindingSetsOut;
   }
   
   /**
    * Applies the optimization to a given subquery root (wrapper around
    * the main entry point).
    */
   private IBindingSet[] optimize(
         final StaticAnalysis sa, final SubqueryRoot subqueryRoot, 
         final StaticBindingInfo staticBindingInfo,
         final VariableUsageInfo ancestorVarUsageInfo,
         final Set<InlineTasks> inlineTasks) {
      
      final IBindingSet[] staticBindings =
         optimize(
            sa, subqueryRoot, staticBindingInfo, ancestorVarUsageInfo,
            inlineTasks, subqueryRoot.getBindingsClause());


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
    * Applies the optimization to any {@link GroupNodeBase}.
    * 
    * @param group a join group
    * @param staticBindings the list of detected static bindings
    * @return
    */
   @SuppressWarnings({ "rawtypes", "unchecked", "unused" })
   private void optimize(
      final StaticAnalysis sa, final GroupNodeBase<?> group, 
      final StaticBindingInfo staticBindingInfo,
      final VariableUsageInfo ancestorVarUsageInfo,
      final Set<InlineTasks> inlineTasks) {
      
      if (group == null) {
         return;
      }
              
      // only process non-optional patterns
      if (group instanceof JoinGroupNode && 
            (((JoinGroupNode) group).isOptional() ||
            ((JoinGroupNode) group).isMinus())) {
         return;
      }
      
      // can't do anything here in the general case
      if (group instanceof UnionNode) {
         // Note: we may look up BIND/VALUES clauses that co-appear in both 
         //       parts of the UNION here. This is considered an edge case
         //       though, so we omit the implementation for now.
         return;
      }
     
      /**
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
      
      // ticket 933b: the prerequisite for static binding of a variable is that
      // there is no preceding OPTIONAL or MINUS possibly binding the variable
      // within the join group, since the OPTIONAL or MINUS would be logically 
      // evaluated first
      final Set<IVariable<?>> optOrMinusVars = new HashSet<IVariable<?>>();
      
      for (IGroupMemberNode child : group) {
         
         if (child instanceof AssignmentNode) {
            
            final AssignmentNode an = (AssignmentNode)child;
            final IValueExpression<?> ve = an.getValueExpression();
            
            /**
             * We can optimize cases where (i) the value expression is a 
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

               final IVariable<?> boundVar = an.getVar();
               
               // pull out the expression to the top of the query root, if
               // possible (the check is necessary to avoid scoping problems
               // caused by bottom-up semantics, i.e. unsafe filter expressions)
               if (!ancOrSelfVarUsageInfo.varUsedInFilterOrAssignment(boundVar)
                     && !optOrMinusVars.contains(boundVar)) {
                  
                  final IBindingSet bs = new ListBindingSet();
                  bs.set(boundVar, (IConstant)an.getValueExpression());
                  staticBindingInfo.addProduced(bs);
                  
                  toRemove.add(child);
                  
               } 
               
               // next, we inline the task; note that inlining is possible, 
               // no matter whether we pull the bindings to the top or not
               final VariableUsageInfo usageInfo =
                     VariableUsageInfo.merge(
                        ancOrSelfVarUsageInfo, childVarUsageInfo);
                  
               // add inline tasks for variable
               if (usageInfo.varUsed(boundVar)) {
                  inlineTasks.add(
                     new InlineTasks(
                        boundVar,
                        (IConstant) an.getValueExpression(),
                        usageInfo.getVarUsages(boundVar)));
               }
               
            }
                        
         // case 2: optimize in-line bindings clauses
         } else if (child instanceof BindingsClause) {

            final BindingsClause bc = (BindingsClause)child;
            
            final List<IBindingSet> bss = bc.getBindingSets();
            final Set<IVariable<?>> bssVars = sa.getVarsInBindingSet(bss);
            
            boolean someVarUsedInFilterOrPrevOptOrMinus = false;
            for (IVariable<?> bssVar : bssVars) {
               
               someVarUsedInFilterOrPrevOptOrMinus |= 
                  ancOrSelfVarUsageInfo.varUsedInFilterOrAssignment(bssVar)
                  || optOrMinusVars.contains(bssVar);
               
            }
            
            // in case none of the vars is used in a filter below, we can
            // safely pull it out
            if (!someVarUsedInFilterOrPrevOptOrMinus) { 

               staticBindingInfo.addProduced(bc.getBindingSets());
               toRemove.add(child);
               
               final VariableUsageInfo usageInfo =
                     VariableUsageInfo.merge(
                        ancOrSelfVarUsageInfo, childVarUsageInfo);
            } 
            
            /*
             * In the following, we set up inline tasks for vars with unique
             * value in the specified binding set.
             */
            final VariableUsageInfo usageInfo =
               VariableUsageInfo.merge(ancOrSelfVarUsageInfo, childVarUsageInfo);
                  
            final IBindingSet[] bs = bss.toArray(new IBindingSet[bss.size()]);
            final Map<IVariable<?>, IConstant<?>> constantVars = 
               SolutionSetStatserator.get(bs).getConstants();
            
            for (IVariable<?> var : constantVars.keySet()) {
               
               if (usageInfo.varUsed(var)) {
                  final IConstant<?> constantVal = constantVars.get(var);
                     inlineTasks.add(new InlineTasks(
                        var, (IConstant)constantVal,usageInfo.getVarUsages(var)));     
               }
            }
            
         // case 3: optimize filter nodes inducing static bindings; not that
         //         (unless the two cases before) FILTER nodes are not producing
         //         any bindings, so they are not removed (but we only create
         //         inline tasks for them, where possible).
         } else if (child instanceof FilterNode) {
            
            FilterNode filter = (FilterNode)child;
            final IValueExpressionNode vexpr = filter.getValueExpressionNode();
            
            if(!(vexpr instanceof FunctionNode))
                return;
            
            final FunctionNode functionNode = (FunctionNode) vexpr;
            
            final URI functionURI = functionNode.getFunctionURI();
            
            // case 3.1: FILTER ?x=<http://uri> or sameTerm(?x,<http://uri>)
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
               
               /*
                * In case the filter describes a single static mapping, we also
                * schedule an inline task.
                */
               if (!bs.isEmpty() && bs.size()==1) {

                  final Entry<IVariable,IConstant> entry = bs.iterator().next();
                  final IVariable<IV> var = entry.getKey();
                  final IConstant<IV> val = entry.getValue();
                  
                  final VariableUsageInfo usageInfo =
                        VariableUsageInfo.merge(
                           ancOrSelfVarUsageInfo, childVarUsageInfo);
                     
                  // add inline tasks for variable
                  if (usageInfo.varUsed(var)) {
                     inlineTasks.add(
                        new InlineTasks(var, val, usageInfo.getVarUsages(var)));
                  }
                  
               } // just in case: can't handle, ignore
               
            // case 3.1: FILTER with unary IN expression
            } else if (functionURI.equals(FunctionRegistry.IN)) {
               
               final int arity = functionNode.arity();
               
               // we're only interested in unary IN expressions
               if (arity==2) {
               
                  final BOp varNodeCandidate = functionNode.get(0);
                  if (varNodeCandidate instanceof VarNode) {
                  
                     final VarNode varNode = (VarNode)varNodeCandidate;
                     final IVariable<IV> var = 
                           (IVariable<IV>)(varNode.getValueExpression());

                     final BOp valueBOp = functionNode.get(1);
                     if (valueBOp instanceof ConstantNode) {
                        final ConstantNode valueNode = (ConstantNode)valueBOp;
                        final IConstant<IV> value = valueNode.getValueExpression();
                        
                        if (value.get().isURI()) {

                           final VariableUsageInfo usageInfo =
                                 VariableUsageInfo.merge(
                                    ancOrSelfVarUsageInfo, childVarUsageInfo);
                           
                           if (usageInfo.varUsed(var)) {
                              inlineTasks.add(new InlineTasks(
                                 var, value, usageInfo.getVarUsages(var))); 
                           }
                        }
                     }
                  }                 
               }
            } // else: there's nothing obvious we can do

         }
         
         
         if (child instanceof IBindingProducerNode && 
               StaticAnalysis.isMinusOrOptional(child)) {
            
            sa.getMaybeProducedBindings(
               (IBindingProducerNode)child, optOrMinusVars, true);
         }
         
         
      }  
      
      /**
       * Remove the children for which static bindings were extracted (they
       * were recorded in the prior iteration over the group.
       */
      for (IGroupMemberNode node : toRemove) {
         while (group.removeArg(node)) {
            // repeat
         }
      }
              
      // recurse into the (remaining) childen
      for (IGroupMemberNode child : group) {
         
         if (child instanceof GroupNodeBase) {
            
            /**
             * Recursive application of optimization, starting out from what.
             */
            optimize(sa,
               (GroupNodeBase<?>) child, staticBindingInfo, 
               ancOrSelfVarUsageInfo, inlineTasks);
            
         } else if (child instanceof SubqueryRoot) {

            /**
             * Apply to subquery, voiding all collected information to account
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

   /**
    * Helper class used to record usage of a given variable, i.e. linking
    * variables to constructs in which they occur.
    */
   public static class VariableUsageInfo {
      
      /**
       * Map recording variable usages
       */
      final Map<IVariable<?>,List<IQueryNode>> usageMap;
      
      /**
       * Constructor creating an empty object (no usages).
       */
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
         
         final List<IQueryNode> varOccurrences = usageMap.get(var);
         for (int i=0; i<varOccurrences.size(); i++) {
            final IQueryNode n = varOccurrences.get(i);
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
       * Extracts variable usage information (variables and the nodes they
       * occur in), investigating FILTERs, assignment nodes, and statement
       * pattern nodes, but /not/ recursing into children.
       * 
       * @param group the group where to extract filter var info (non-recursively)
       */
      public void extractVarUsageInfoSelf(final GroupNodeBase<?> group) {
         
         for (IQueryNode node : group) {

            // Note: there's no need to handle BindingsClause here, since
            //       a BindingsClause always binds to constant values
            if (node instanceof FilterNode || node instanceof AssignmentNode) {

               final GroupMemberValueExpressionNodeBase filter = 
                     (GroupMemberValueExpressionNodeBase) node;
      
               final IValueExpressionNode vexpr = filter.getValueExpressionNode();
               
               extractVarUsageInfo(node, (IValueExpressionNode)vexpr);

            } else if (node instanceof StatementPatternNode) {
               
               final StatementPatternNode spn = (StatementPatternNode)node;

               for (IVariable<?> spnVar : spn.getProducedBindings()) {
                  
                   registerVarToChildMappingInUsageMap(spnVar, node);
                   
               }
           
            // BLZG-2042: inline BIND information into property path nodes
            } else if (node instanceof PropertyPathNode) {
                
                extractVarUsageInfoForPropertyPathNode((PropertyPathNode)node);
            }
         }
      }

        private void extractVarUsageInfoForPropertyPathNode(
                final PropertyPathNode ppNode) {

            // cover subject and object position variable
            if (ppNode != null && ppNode.arity() >= 3 /* should always be true, just in case */) {

                final BOp subjectNode = ppNode.get(0);
                if (subjectNode instanceof VarNode) {
                    registerVarToChildMappingInUsageMap(((VarNode) subjectNode).getValueExpression(), ppNode);
                }

                final BOp objectNode = ppNode.get(2);
                if (objectNode instanceof VarNode) {
                    registerVarToChildMappingInUsageMap(((VarNode) objectNode).getValueExpression(), ppNode);
                }

            }
            
            // cover context position variable, if defined
            if (ppNode.arity() >= 4) {
                final BOp contextNode = ppNode.get(3);
                if (contextNode instanceof VarNode) {
                    registerVarToChildMappingInUsageMap(((VarNode) contextNode).getValueExpression(), ppNode);
                }
            }            
        }
      
      /**
       * Extracts variable usage information from an {@link IValueExpressionNode}.
       * 
       * @param node the "parent" node which is reported back
       * @param the value expression node to investigate
       */
      private void extractVarUsageInfo(
         final IQueryNode node, final IValueExpressionNode vexpNode) {

         /**
          * If the vexpNode is a VarNode, we're done and add it to the map
          * (if it has not yet been recorded before).
          */
         if (vexpNode instanceof VarNode) {
            final VarNode varNode = (VarNode)vexpNode;
            final IVariable<?> iVar = varNode.getValueExpression();
            
            if (!usageMap.containsKey(iVar)) {
               usageMap.put(iVar, new ArrayList<IQueryNode>());
            }
            
            // add node to list
            usageMap.get(iVar).add(node);
            
            return;
         }
         
         /**
          * Otherwise, we scan for recursively nested var nodes
          */
         final BOp nodeAsBop = (BOp)vexpNode;
         final int arity = nodeAsBop.arity();
         for (int i=0; i<arity; i++) {
            
            final BOp child = nodeAsBop.get(i);
            if (child instanceof IValueExpressionNode) {
               
               // recurse
               extractVarUsageInfo(node, (IValueExpressionNode)child);
               
            }
         }
      }



      /**
       * Extracts usage information for the variable from statement patterns
       * being direct children of the current node (ignoring the node itself).
       * 
       * @param group the group node base in which we perform the lookup
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
       * from the current node itself and its children (recursively).
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
         
         if (group==null) {
            return;
         }
         
         // abort for optional patterns
         if (group instanceof JoinGroupNode && ((JoinGroupNode) group).isOptional()) {
            return;
         }
         
         for (IQueryNode child : group) {
            
            if (child instanceof GroupNodeBase) {

               extractVarSPUsageInfoChildrenOrSelf((GroupNodeBase)child);
               
            } else if (child instanceof StatementPatternNode) {
               
               final StatementPatternNode spn = (StatementPatternNode)child;

               for (IVariable<?> spnVar : spn.getProducedBindings()) {
                  
                   registerVarToChildMappingInUsageMap(spnVar,child);
                  
               }
               
            } else if (child instanceof GroupNodeBase) {

               extractVarSPUsageInfoChildrenOrSelf((GroupNodeBase)child);
               
            // BLZG-2042: inline BIND information into property path nodes
            } else if (child instanceof PropertyPathNode) {
                
                extractVarUsageInfoForPropertyPathNode((PropertyPathNode)child);
                
            }
         }
      }
      
      /**
       * Registers the mapping between the variable var and the child in the usage map. If var is null
       * or the child is null, no action will be taken.
       * 
       * @param var the variable
       * @param child the child using the variable
       */
      private void registerVarToChildMappingInUsageMap(final IVariable<?> var, final IQueryNode child) {
          
          if (var==null)
              return;
          
          if (!usageMap.containsKey(var)) {
              usageMap.put(var, new ArrayList<IQueryNode>());
           }

           // add node to list
           usageMap.get(var).add(child);
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
    * Class that helps to keep track of static bindings that have been spotted
    * during query analysis. The class maintains a list of "produced" static
    * bindings, i.e. such derived from assignment nodes etc.
    */
   public static class StaticBindingInfo {
      
      final List<List<IBindingSet>> produced;
      
      final IBindingSet[] queryInput;

      /**
       * Constructor with empty input binding set.
       */
      public StaticBindingInfo() {
         
         this.produced = new ArrayList<List<IBindingSet>>();
         this.queryInput = new IBindingSet[] { new ListBindingSet() };
      }

      /**
       * Constructor with given input binding set.
       */
      public StaticBindingInfo(IBindingSet[] queryInput) {
         
         this.produced = new ArrayList<List<IBindingSet>>();
         this.queryInput = queryInput;
         
      }

      public void addProduced(IBindingSet bs) {
         produced.add(wrap(bs));
      }

      public void addProduced(List<IBindingSet> bsList) {
         produced.add(bsList);
      }

      public List<List<IBindingSet>> getProduced() {
         return produced;
      }
      
      public IBindingSet[] joinProduced() {
         return join(produced);
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
    * Can be applied by calling the {@link InlineTasks#apply()} method.
    */
   public static class InlineTasks {
      
      @SuppressWarnings("rawtypes")
      final private IVariable var;
      
      @SuppressWarnings("rawtypes")
      final private IConstant<IV> val;

      /**
       * The nodes in which the variable can be inlined.
       */
      final private List<IQueryNode> nodes;


      /**
       * Construct an inline task
       * 
       * @param var the variable to inline
       * @param val the known value for the variable
       * @param nodes the nodes in which inlining is valid
       */
      @SuppressWarnings("rawtypes")
      public InlineTasks(
         final IVariable var,    
         final IConstant<IV> val,
         final List<IQueryNode> nodes) {
         
         this.var = var; 
         this.nodes = nodes;
         this.val = val;
              
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
      public void apply() {
         
         final IV valIV = val.get();
         
         for (IQueryNode node : nodes) {
            
            apply(valIV, node);
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
            
            final FilterNode filter = (FilterNode)node;
            final IValueExpressionNode vexpr = filter.getValueExpressionNode();
            applyToValueExpressionNode(val, vexpr);
            
         } else if (node instanceof AssignmentNode) {

            applyToAssignmentNode(val, (AssignmentNode)node);
            
         } else if (node instanceof StatementPatternNode) {
            
            applyToStatementPattern(val, (StatementPatternNode)node);

         // BLZG-2042: inline BIND information into property path nodes
         } else if (node instanceof PropertyPathNode) {

            applyToPropertyPathNode(val, (PropertyPathNode)node);

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

         final TermNode s = spn.s();
         final TermNode p = spn.p();
         final TermNode o = spn.o();
         final TermNode c = spn.c();
         
         if (s!=null && s instanceof VarNode && s.get(0).equals(var)) {
            
            final VarNode sVar = (VarNode)s;
            final ConstantNode constNode = new ConstantNode(
               new Constant<IV>(sVar.getValueExpression(),val));
            spn.setArg(0, constNode);
            
         }
         
         if (p!=null && p instanceof VarNode && p.get(0).equals(var)) {

            final VarNode pVar = (VarNode)p;
            final ConstantNode constNode = new ConstantNode(
               new Constant<IV>(pVar.getValueExpression(),val));
            spn.setArg(1, constNode);
            
         }

         if (o!=null && o instanceof VarNode && o.get(0).equals(var)) {
            
            final VarNode oVar = (VarNode)o;
            final ConstantNode constNode = new ConstantNode(
               new Constant<IV>(oVar.getValueExpression(),val));
            spn.setArg(2, constNode);
            
         }

         if (c!=null && c instanceof VarNode && c.get(0).equals(var)) {
            
            final VarNode cVar = (VarNode)c;
            final ConstantNode constNode = new ConstantNode(
               new Constant<IV>(cVar.getValueExpression(),val));
            spn.setArg(3, constNode);
            
         }
      }
      
      /**
       * Applies the {@link InlineTasks} for the class variable with the
       * parameter val to the given statement pattern node.
       * 
       * @param val
       * @param spn
       */
      @SuppressWarnings("rawtypes")
      private void applyToPropertyPathNode(
         final IV val, final PropertyPathNode ppn) {

         if (ppn.arity()>=3 /* should always be true, just in case */) {

             // cover subject variable replacement
             final BOp s = ppn.get(0);
             if (s!=null && s instanceof VarNode && s.get(0).equals(var)) {
           
                 final VarNode sVar = (VarNode)s;
                 final ConstantNode constNode = new ConstantNode(
                         new Constant<IV>(sVar.getValueExpression(),val));
                 ppn.setArg(0, constNode);
           
             }
             
             // cover object variable replacement
             final BOp o = ppn.get(2);
             if (o!=null && o instanceof VarNode && o.get(0).equals(var)) {
           
                 final VarNode oVar = (VarNode)o;
                 final ConstantNode constNode = new ConstantNode(
                         new Constant<IV>(oVar.getValueExpression(),val));
                 ppn.setArg(2, constNode);
           
             }
             
         }
         
         // cover context variable replacement
         if (ppn.arity()>=4) {
             
             final BOp c = ppn.get(3);
             if (c!=null && c instanceof VarNode && c.get(0).equals(var)) {
           
                 final VarNode cVar = (VarNode)c;
                 final ConstantNode constNode = new ConstantNode(
                         new Constant<IV>(cVar.getValueExpression(),val));
                 ppn.setArg(3, constNode);
           
             }
             
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

         for (int i=0; i<functionNode.arity(); i++) {

            final IValueExpressionNode cur = 
               (IValueExpressionNode) functionNode.get(i);
            
            if (cur instanceof VarNode && ((VarNode)cur).get(0).equals(var)) {
               
               final ConstantNode constNode = 
                  new ConstantNode(
                     new Constant<IV>(((VarNode)cur).getValueExpression(),val));
               
               functionNode.setArg(i, constNode);
               
            } else {
               
               applyToValueExpressionNode(val, cur); // recurse
               
            }
         }
      }
   }
}
