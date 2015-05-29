package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.sparql.ast.ASTOptimizerResult;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SolutionSetStatserator;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizer that aims at the optimization of SPARQL 1.1 pattern introducing
 * static (i.e., non runtime dependent) binding of variables. Note that this
 * optimizer must run *first* in the pipeline, as other optimizers might use
 * the statically known bindings for optimization.
 * 
 * This optimizer may apply to the following construct:
 * 
 * - BIND + VALUES clauses with constant values
 * - FILTER IN clauses with URIs // TODO: can we also support literals here?
 * - FILTERs with simple sameTerm(?x,"someConstant") clauses
 * 
 * It applies only to patterns that are known to definitely be bound (this
 * means: whenever the variables that are introduced are definitely bound
 * variables).
 * 
 * The optimization approach taken is the following:
 * 
 * 1. Analyse the above mentioned constructs to identify the mapping from
 *    variables at top-level that will be definitely statically bound to their
 *    values.
 * 2. Modify the bindingSets[] to include these static bindings.
 * 3. Remove all constructs introducing statically defined bindings, as they
 *    have become redundant now.
 * 4. TODO: initialize StaticAnalysis properly?!
 * 
 * Note: this optimizer generalizes the {@link ASTSimpleBindingsOptimizer},
 *       which has been disabled and marked as deprecated.
 *       // TODO: should we throw out the latter optimizer completely?
 *       
 * The following extensions are not considered crucial for now, but might be
 * subject to future work on this optimizer:
 * 
 * TODO: we may want to decompose FILTERs prior to running this optimizer
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ASTStaticBindingsOptimizer implements IASTOptimizer {

   @Override
   public ASTOptimizerResult optimize(AST2BOpContext context, IQueryNode queryNode,
         IBindingSet[] bindingSets) {

      /**
       * We collect statically enforced bindings in this binding set, which
       * will later be post processed and injected (by joining all bindings).
       */
      final List<List<IBindingSet>> staticBindings = 
         new ArrayList<List<IBindingSet>>();


      /**
       * In case the binding sets variable is null, nothing needs to be done 
       * (query will return empty result anyways). Also, this optimizer is
       * only applicable to top-level queries.
       */
      if (bindingSets == null || !(queryNode instanceof QueryRoot)) {
         return new ASTOptimizerResult(queryNode, bindingSets);
      }


      final QueryRoot queryRoot = (QueryRoot) queryNode;
      
      /**
       * Apply the optimization (eliminating static binding producing constructs
       * from the query and collecting the staticBindings set)
       */
      optimizeDefinitelyProducedBindingsClauses(queryRoot, staticBindings);
      
      /**
       * Join static binding sets with incoming binding sets.
       */
      final IBindingSet[] bindingSetsOut = 
         joinStaticBindings(staticBindings, bindingSets);
      
      
      // TODO: is this correct -> updating of the solution set statistics
      //       for upcoming operators, to be verified
      context.setSolutionSetStats(SolutionSetStatserator.get(bindingSetsOut));
      
      return new ASTOptimizerResult(queryRoot, bindingSetsOut);
   }
   
   /**
    * Applies the optimization to all constructs delivering definitely produced
    * bindings, namely (i) eliminates these constructs while (ii) recording the
    * static bindings as List<IBindingSet> in the staticBindings parameter.
    * 
    * @param queryRoot the query root
    * @param staticBindings the list of detected static bindings
    * @return
    */
   private void optimizeDefinitelyProducedBindingsClauses(
         final QueryRoot queryRoot, final List<List<IBindingSet>> staticBindings) {
      
      /**
       * First, collect static bindings from the main root binding clause.
       */
      final BindingsClause rootBindingsClause = queryRoot.getBindingsClause();
      if (rootBindingsClause!=null) {
         staticBindings.add(rootBindingsClause.getBindingSets());
         queryRoot.setBindingsClause(null); // remove the bindings clause
      }
      
      optimizeDefinitelyProducedBindings(
         queryRoot.getWhereClause(), staticBindings);

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
   private void optimizeDefinitelyProducedBindings(
      final GroupNodeBase<?> group, final List<List<IBindingSet>> staticBindings) {
      
      if (group == null) {
         return;
      }
              
      if (group instanceof JoinGroupNode && ((JoinGroupNode) group).isOptional()) {
         return;
      }
              
      if (group instanceof UnionNode) {
          return;
      }
      
      /**
       * Collect the children that introduce static bindings for later removal.
       */
      final List<IGroupMemberNode> toRemove = new ArrayList<IGroupMemberNode>();
      for (IGroupMemberNode child : group) {
                  
         if (child instanceof SubqueryRoot) {

            /**
             *  TODO: Think about the following:
             *        We may want to apply this optimization at subquery level
             *        as well, but this might become somewhat more complex as we
             *        cannot inject resulting mappings into the top-level
             *        queries binding set directly. For now, this is not
             *        considered essential and this case is left unimplemented.
             *        One idea could be to apply this optimizer to the subquery
             *        as a whole with empty bindingsSet as input, and set the
             *        subqueries VALUES clause. This could be pretty much
             *        straightforward, but I'm not 100% sure if that's valid
             *        in all cases.
             *        
             *        Note that it is definitely safe *not* to apply any
             *        optimization here, so skipping this case does not harm
             *        correctness of the optimizer.
             */
            
         // case 1: optimize away assignments to constants
         } else if (child instanceof AssignmentNode) {
            
            final AssignmentNode an = (AssignmentNode)child;
            final IValueExpression<?> ve = an.getValueExpression();
            
            if (ve instanceof IConstant) {

               final IBindingSet bs = new ListBindingSet();
               bs.set(an.getVar(), (IConstant)an.getValueExpression());
               final List<IBindingSet> bsList = new ArrayList<IBindingSet>(1);
               bsList.add(bs);
               staticBindings.add(bsList);
               
               toRemove.add(child);
               
            }
                        
         // case 2: optimize in-line bindings clauses
         } else if (child instanceof BindingsClause) {

            final BindingsClause bc = (BindingsClause)child;
            staticBindings.add(bc.getBindingSets());
            toRemove.add(child);
            
         // case 3: optimize filter nodes inducing static bindings
         } else if (child instanceof FilterNode) {
            
            // FilterNode with FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#sameTerm
            // FilterNode with IN?!            
            // TODO: to be implemented

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
//      for (IGroupMemberNode child : group) {
//         
//         if (child instanceof GroupNodeBase) {
//            optimizeDefinitelyProducedBindings(
//               (GroupNodeBase<?>) child, staticBindings);
//         }
//         
//      }
      
   }
   

   /**
    * Joins the staticBindings with the queryInput binding set, returning
    * the resulting binding set.
    */
   private  IBindingSet[] joinStaticBindings(
      List<List<IBindingSet>> staticBindings, IBindingSet[] queryInput) {
      
      if (queryInput==null || queryInput.length==0) {
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
               final IBindingSet join = 
                  BOpContext.bind(
                     left, 
                     right, 
                     null /* constraints */, 
                     null /* varsToKeep */);

               if (join != null) {
                  tmp.add(join);
               }
               
            }
         
         }
         
         leftBindingSets = tmp; // prepare for next iteration
         
      }
      
      return leftBindingSets.toArray(new IBindingSet[leftBindingSets.size()]);
      
   }   
   
}
