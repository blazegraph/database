/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Jun 26, 2008
 */

package com.bigdata.join;

import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * The evaluation order is determined by analysis of the propagation of
 * bindings. The most selective predicate is choosen first (having the fewest
 * unbound variables with ties broken by a range count on the data) and "fake"
 * bindings are propagated to the other predicates in the tail. This process is
 * repeated until all variables are bound and an evaluation order has been
 * determined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultEvaluationPlan implements IEvaluationPlan {

    protected static final Logger log = Logger.getLogger(DefaultEvaluationPlan.class);
    
    private final IJoinNexus joinNexus;

    private final IRule rule;

    private final IBindingSet bindingSet;

    private final int tailCount;

    private final boolean[] used;

    private final int[] order;

    public int[] getOrder() {
        
        return order;
        
    }

    /*
     * FIXME Cache the range counts if they are obtained (use -1L if not
     * computed) and compute [empty] based on the range counts. Then use
     * isEmpty() to avoid rule execution when there will be no solutions.
     */
//    /**
//     * True iff a range count on the tails finds finds that there is at least
//     * one tail that does not match anything in the data and, hence, that there
//     * must be no solutions for the rule in the data.
//     * <p>
//     * This determination, like the evaluation order itself, is only good for
//     * the commit time when it is evaluated. If the rule will be executed at a
//     * later point then its evaluation order SHOULD be re-computed.
//     * 
//     * @return
//     */
//    public boolean isEmpty() {
//        
//        return empty;
//        
//    }
//    private final boolean empty;
//    private final long[] rangeCount;
    
    /**
     * Some interesting metadata about the plan.
     */
    public String toString() {
        
        return getClass().getName() + "{order=" + Arrays.toString(order)
                + ", rule=" + rule + "}";
        
    }
    
    /**
     * FIXME When there is NO data the first tail predicate (this is the only
     * one where we have the exact bindings and therefore a correct upper bound
     * on the range count) then the evaluation plan SHOULD be a NOP since
     * nothing can be entailed by the rule. There needs to be a way to indicate
     * this. Likewise, the API here should keep more metadata about the access
     * path, including basic range counts. Those things really change pretty
     * slowly, so it would be nice if they could be shared across some service.
     * 
     * @param joinNexus
     * @param rule
     */
    public DefaultEvaluationPlan(IJoinNexus joinNexus, IRule rule) {
        
        if (joinNexus == null)
            throw new IllegalArgumentException();

        if (rule == null)
            throw new IllegalArgumentException();
        
        this.joinNexus = joinNexus;
        
        this.rule = rule;
    
        if(log.isDebugEnabled()) {
            
            log.debug("rule=" + rule);
            
        }
        
        bindingSet = new ArrayBindingSet(rule.getVariableCount());

        tailCount = rule.getTailCount();

        order = new int[tailCount];

        used = new boolean[tailCount];

        if (tailCount == 1) {

            order[0] = 0;

            return;
            
        }
        
        // clear array. -1 is used to detect logic errors.
        for (int i = 0; i < order.length; i++)
            order[i] = -1;

        for (int i = 0; i < tailCount; i++) {

            int index = -1;
            int minVarCount = Integer.MAX_VALUE;
            long minRangeCount = Long.MAX_VALUE;
            
            for (int j = 0; j < tailCount; j++) {
                
                if(used[j]) continue; // already in the evaluation order. 
                
                final int varCount = rule.getTail(j)
                        .getVariableCount();

                if (varCount < minVarCount) {

                    index = j;

                    minVarCount = varCount;

                } else if (varCount == minVarCount) {

                    /*
                     * Tweaks the evaluation order for predicates where the #of
                     * variable bindings is the same by examining the range
                     * counts.
                     * 
                     * Note: In doing this, we disregard the bindings that were
                     * propagated since they are -1 and will NOT match anything
                     * anywhere!
                     * 
                     * Note: In the case where some other predicate is already
                     * first in the evaluation order by the virtue of having
                     * more variables bound this tweak is purely heuristic
                     * regardless of the fact that it considers the data. The
                     * reason is that the actual bindings that are propagated
                     * during the execution of the rule will determine which of
                     * the two predicates under consideration is, in fact, more
                     * selective in the data. However, in the special case where
                     * the two predicates are competing for the 1st position in
                     * the evaluation order, this "tweak" is exact.
                     * 
                     * @todo Some tails use the same constant pattern in both
                     * predicates. E.g., rdfs11 (u subClassOf v) (v subClassOf
                     * x). In these cases comparing range counts is pointless
                     * and could be avoided by testing for this pattern.
                     */
                    
                    if(minRangeCount == Long.MAX_VALUE) {
                        
                        // range count of the current best choice (computed
                        // lazily).
                        minRangeCount = getAccessPath(index, null/* bindingSet */)
                                .rangeCount(false/*exact*/);

                    }

                    // range count of the current predicate under
                    // examination.
                    final long rangeCount = getAccessPath(j, null/* bindingSet */)
                            .rangeCount(false/* exact */);
                    
                    if (rangeCount < minRangeCount) {

                        /*
                         * choose the predicate that is more selective given
                         * the variable bindings.
                         */
                        
                        index = j;
                        
                        minVarCount = varCount;
                        
                        minRangeCount = rangeCount;
                        
                    }
                    
                }
                
            }

            if (index == -1)
                throw new AssertionError();
            
            if (used[index])
                throw new AssertionError("Attempting to reuse predicate: index="+i+"\n"+this);
            
            order[i] = index;
            
            used[index] = true;
            
            // set fake bindings for this predicate.
            {
                
                final IPredicate pred = rule.getTail(index);

                if (rule.isFullyBound(index, bindingSet)) {

                    /*
                     * Note: If a predicate is fully bound (whether by constants
                     * or as a side-effect of the bindings that would have been
                     * propagated by the previous predicates in the evaluation
                     * order) then it is just an existance test.
                     * 
                     * Note: A predicate that is fully bound with constants will
                     * be ordered before any predicate that has an unbound
                     * variable. Predicates that become fully bound due to the
                     * evaluation order are somewhat (but not too) different
                     * since the existence test is for an element whose bindings
                     * are not known until runtime.
                     * 
                     * @todo optimize rule execution for existence tests.
                     */
                    
                    if(log.isDebugEnabled()) {

                        log.debug("predicate will be fully bound: " + pred);
                        
                    }
                    
                    continue;
                    
                }
                
                final int arity = pred.arity();

                for (int z = 0; z < arity; z++) {

                    final IVariableOrConstant v = pred.get(z);

                    if (v.isVar() && !bindingSet.isBound((IVariable)v)) {

                        bindingSet.set((IVariable) v, FAKE);

                    }

                }
                
            }
            
        } // next tail predicate.

        if (log.isDebugEnabled()) {

            log.debug(toString());
            
        }
        
    }
    
    /**
     * Used to mark fake bindings by {@link #computeEvaluationOrder()}.
     */
    private static final transient IConstant<String> FAKE = new Constant<String>(
            "Fake");

    /**
     * Return the {@link IAccessPath} that would be used to read from the
     * selected {@link IPredicate} (no caching).
     * 
     * @param index
     *            The index of the {@link IPredicate} in the tail of the
     *            {@link Rule}.
     * @param bindingSet
     *            When non-<code>null</code>, the bindings will be used to
     *            generate the {@link IAccessPath}. When <code>null</code>
     *            the {@link IAccessPath} will use wildcards in every position
     *            where the predicate declares a variable.
     * 
     * @return The {@link IAccessPath}.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is out of bounds.
     * @throws RuntimeException
     *             if the name of the relation can not be resolved by the
     *             {@link IJoinNexus} to an {@link IRelation} instance.
     */
    protected IAccessPath getAccessPath(final int index,
            final IBindingSet bindingSet) {

        final IPredicate predicate;

        if (bindingSet != null) {

            // based on the current bindings.

            predicate = rule.getTail(index).asBound(bindingSet);

        } else {

            // as declared by the predicate (no bindings).

            predicate = rule.getTail(index);

        }

        // The name of the relation that the predicate will query.
        final IRelationName relationName = predicate.getRelationName();
        
        // Resolve the relation name to the IRelation object.
        final IRelation relation = joinNexus.getRelationLocator().getRelation(
                relationName);
        
        // find the best access path for the predicate for that relation.
        final IAccessPath accessPath = relation.getAccessPath(predicate);
        
        // return that access path.
        return accessPath;

    }

//  if (focusStore != null && focusIndex > 0) {
//  /*
//   * Note: This stuff is specific to the RDF DB, including the ROM
//   * assumptions for the focusStore vs the DB. With the refactor the
//   * focusStore and the fusedView are already encoded into the relation
//   * associated with each tail and the order will therefore reflect the
//   * actual (or approximate) range counts for each access path.
//   */
//      
//      /*
//       * The rule of thumb is to always evaluate the predicate that
//       * will read from the focusStore 1st since we presume that any
//       * triple pattern reading on the [focusStore] will be
//       * significantly smaller than any other triple pattern reading
//       * on the fused view [focusStore + database].
//       * 
//       * However, there are counter examples. Loading schema and then
//       * data into a database is one, e.g., the wordnet ontology
//       * followed by the wordnet nouns data. Another is when the 2nd
//       * predicate in the body is none bound - you are often better
//       * off NOT running the all none bound predicate 1st.
//       * 
//       * Note: All of this logic is executed IFF the focusIndex is >
//       * 0. When the focusIndex is 0, the focusStore is already being
//       * read by the predicate that is first in the evaluation order.
//       */
//
//      /*
//       * In this alternative, we simply refuse to order a 3-unbound
//       * predicate to the 1st position in the evaluation order.
//       */
//      
//      if (false && rule.getTailPredicate(order[focusIndex])
//              .getVariableCount() < 3) {
//
//          /*
//           * Note: This does not work since we may have already done a
//           * rangeCount (w/o bindings) when computing the evaluation
//           * order so we can not undo that purely by considering the
//           * variable bindings. An alternative is to mark as "pinned"
//           * any part of the evaluation order that have already been
//           * tested in the data, e.g., by retaining the rangeCounts if
//           * computed by computeEvaluationOrder.  If we do that then
//           * we could try this simpler test again.
//           */
//          
////              /*
////               * Swap the places of those predicates in the evaluation
////               * order such that we will evaluate the predicate at the
////               * focusIndex 1st.
////               */
////
////              int tmp = order[0];
////
////              order[0] = order[focusIndex];
////
////              order[focusIndex] = tmp;
//
//      } else {
//
//          /*
//           * In order to catch those counter examples we range count
//           * the predicate that is already 1st in the evaluate order
//           * against [focusStore+database] and then the predicate at
//           * the focusIndex against the [focusStore]. We then reorder
//           * the predicate at the focusIndex 1st iff it has a smaller
//           * range count that the predicate that is already first in
//           * the evaluation order.
//           * 
//           * Note: range counts here could be expensive when the index
//           * is remote or partitioned.
//           */
//          
//          /*
//           * Range count for the predicate that is 1st in the
//           * evaluation order. This will read against [focusStore +
//           * database].
//           */
//
//          final long rangeCount1 = getAccessPath(order[0], null/* bindingSet */)
//                  .rangeCount();
//
//          /*
//           * Range count for the predicate at the focusIndex. This
//           * will read against [focusStore].
//           */
//
//          final long rangeCount2 = getAccessPath(order[focusIndex],
//                  null/* bindingSet */).rangeCount();
//
//          if (rangeCount2 < rangeCount1) {
//
//              /*
//               * Swap the places of those predicates in the evaluation
//               * order such that we will evaluate the predicate at the
//               * focusIndex 1st.
//               */
//
//              int tmp = order[0];
//
//              order[0] = order[focusIndex];
//
//              order[focusIndex] = tmp;
//
//          }
//
//      }
//      
//  }

}
