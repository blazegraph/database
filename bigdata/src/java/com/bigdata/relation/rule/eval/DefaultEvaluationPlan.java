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

package com.bigdata.relation.rule.eval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;

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

    protected static final transient Logger log = Logger.getLogger(DefaultEvaluationPlan.class);
    
    protected static final transient boolean INFO = log.isInfoEnabled();

    protected static final transient boolean DEBUG = log.isDebugEnabled();
    
    private final IJoinNexus joinNexus;

    private final IRule rule;

    /**
     * Used to propagate fake bindings so that the tail predicates appear to
     * become increasingly bound as each successive predicates is assigned to
     * its place in the evaluation order.
     */
    private final IBindingSet bindingSet;

    /**
     * The #of predicates in the tail of the rule.
     */
    private final int tailCount;

    /**
     * Tail predicates are marked as {@link #used} as they are assigned a place
     * in the evaluation order. This keeps any given tail predicate from being
     * assigned to more than one index position in the evaluation order.
     */
    private final boolean[/*tailIndex*/] used;

    /**
     * The computed evaluation order. The elements in this array are the order
     * in which each tail predicate will be evaluated. The index into the array
     * is the index of the tail predicate whose evaluation order you want. So
     * <code>[2,0,1]</code> says that the predicates will be evaluated in the
     * order tail[2], then tail[0], then tail[1]. 
     */
    private final int[/* order */] order;

    public int[] getOrder() {
        
        calc();
        
        return order;
        
    }

    /**
     * True iff a range count on any of the tail predicates finds that there is
     * at least one tail that does not match anything in the data and, hence,
     * that there must be no solutions for the rule in the data.
     * <p>
     * This determination, like the evaluation order itself, is only good for
     * the commit time when it is evaluated. If the rule will be executed at a
     * later point then its evaluation order SHOULD be re-computed.
     * 
     * @return true iff the rule was proven to have NO solutions based on the
     *         range counts that were obtained.
     */
    public boolean isEmpty() {
        
        calc();
        
        return empty;
        
    }
    /**
     * Assumes not empty unless proven otherwise when computing the evaluation
     * order.
     */
    private boolean empty = false;

    /**
     * The variables that will be bound when it comes time to evaluate the
     * predicate at each point in the computed evaluation order.
     */
    protected final List<IVariable>[/*tailIndex*/] varsBoundInRule;
    
    /**
     * The variables bound by the predicate in the computed evaluation order.
     */
    final protected List<IVariable>[/*tailIndex*/] varsBoundByPred;
    
    /**
     * The #of unbound variables remaining at each point in the computed
     * evaluation order.
     */
    protected final int[/*tailIndex*/] varCount;
    
    /**
     * Return the #of unbound variables for the tail predicate given the
     * computed evaluation order.
     * 
     * @param tailIndex
     *            The index of the predicate in the tail of the rule.
     * 
     * @return The #of variables that will not yet be bound when it is time to
     *         evaluate the predicate.
     */
    public int getVariableCount(int tailIndex) {
        
        calc();
        
        return varCount[tailIndex];
        
    }
    
    /**
     * Return <code>true</code> iff the predicate will be fully bound when it
     * is evaluated (hence, a point test rather than a range scan).
     * 
     * @param tailIndex
     *            The index of a predicate in the tail of the rule.
     * 
     * @return <code>true</code> iff the predicate will be fully bound when it
     *         is evaluated in the given evaluation order.
     */
    public boolean isFullyBound(int tailIndex) {
        
        calc();
        
        return varCount[tailIndex] == 0;
        
    }
    
    /**
     * Cache of the computed range counts for the predicates in the tail. The
     * elements of this array are initialized to -1L, which indicates that the
     * range count has NOT been computed. Range counts are computed on demand
     * and MAY be zero. Only an approximate range count is obtained. Such
     * approximate range counts are an upper bound on the #of elements that are
     * spanned by the access pattern. Therefore if the range count reports ZERO
     * (0L) it is a real zero and the access pattern does not match anything in
     * the data. The only other caveat is that the range counts are valid as of
     * the commit point on which the access pattern is reading. If you obtain
     * them for {@link ITx#READ_COMMITTED} or {@link ITx#UNISOLATED} views then
     * they could be invalidated by concurrent writers.
     */
    private final long[/*tailIndex*/] rangeCount;
    
    /**
     * Some interesting metadata about the plan.
     * <p>
     * Note: Do not invoke methods that would call {@link #calc()} from within
     * this implementation.
     */
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        final String delim = "\t";
        
        sb.append(getClass().getName() + //
                "{ empty="+empty + //
                ", rule=\"" + rule.getName() + "\"" +//
                "\n"
                );

        sb.append("tailIndex" + delim + "predicate" + delim + "evalOrder"
                + delim + "rngcnt" + delim + "varcnt" + delim
                + "binds" + delim + "given" + "\n");
        
        for (int tailIndex = 0; tailIndex < tailCount; tailIndex++) {

            sb.append(Integer.toString(tailIndex));

            sb.append(delim + "\"" + rule.getTail(tailIndex) + "\"");

            sb.append(delim + order[tailIndex]);

            // note: will be cached.
            sb.append(delim + rangeCount(tailIndex));

            sb.append(delim + varCount[tailIndex]);

            sb.append(delim);
            toString(sb, varsBoundByPred[tailIndex]);

            sb.append(delim);
            toString(sb, varsBoundInRule[tailIndex]);

            sb.append("\n");
            
        }
        
        sb.append("}");

        return sb.toString();
        
    }
    
    /**
     * A string representation of a list without commas between the elements.
     * 
     * @param sb
     *            The representation will be appended to this buffer.
     * @param l
     *            The list.
     */
    private void toString(StringBuilder sb, List l) {

        if (l == null || l.isEmpty()) {
            
            sb.append("[ ]");
            
            return;
            
        }
        
        Object[] a = l.toArray();

        Arrays.sort(a);
        
        sb.append("[");
        
        for(Object o : a) {
            
            sb.append(" "+o);
            
        }
        
        sb.append(" ");
        
        sb.append("]");
        
    }
    
    /**
     * Ctor required by {@link IEvaluationPlanFactory}.
     * 
     * @param joinNexus
     *            The join nexus.
     * @param rule
     *            The rule.
     */
    public DefaultEvaluationPlan(IJoinNexus joinNexus, IRule rule) {
        
        if (joinNexus == null)
            throw new IllegalArgumentException();

        if (rule == null)
            throw new IllegalArgumentException();
        
        this.joinNexus = joinNexus;
        
        this.rule = rule;
    
        bindingSet = new ArrayBindingSet(rule.getVariableCount());

        tailCount = rule.getTailCount();

        order = new int[tailCount];

        used = new boolean[tailCount];

        rangeCount = new long[tailCount];
        
        varCount = new int[tailCount];

        varsBoundInRule = new LinkedList[tailCount];
        
        varsBoundByPred = new LinkedList[tailCount];

        calc();
        
        if(log.isInfoEnabled()) {
            
            log.info(toString());
            
        }
        
    }
    
    /**
     * Compute the evaluation order (NOP if already computed).
     */
    private void calc() {
        
        if (used[0]) return;

        if (tailCount == 1) {

            order[0] = 0;
            
            used[0] = true;

            // not proven empty since range count not taken.
            empty = false;
            
            return;
            
        }
        
        // clear arrays.
        for (int i = 0; i < tailCount; i++) {

            order[i] = -1; // -1 is used to detect logic errors.
            
            varCount[i] = -1; // -1 indicates not processed yet.
            
            rangeCount[i] = -1L;  // -1L indicates no range count yet.

            varsBoundInRule[i] = new LinkedList<IVariable>();
            
            varsBoundByPred[i] = new LinkedList<IVariable>();
            
        }

        /*
         * Process each tail predicate in order, comparing its selectivity (#of
         * unbound variables and possibly its range count) to each non-comsumed
         * (!used) tail predicate in turn. As each predicate is assigned in turn
         * to the evaluation order, we propagate fake bindings so that
         * downstream predicates will appear to be more bound - much as they
         * would if the rule were being run against real data.
         */
        for (int evalOrder = 0; evalOrder < tailCount; evalOrder++) {

            if (DEBUG)
                log.debug("Choosing predicate to evaluate next: evalOrder="
                        + evalOrder + ", order=" + Arrays.toString(order));
            
            // choosen as the remaining tail predicate that is most selective.
            int tailIndex = -1;

            int minVarCount = Integer.MAX_VALUE;
            
            // consider the remaining tail predicates.
            for (int j = 0; j < tailCount; j++) {
                
                if(used[j]) continue; // already in the evaluation order. 
                
                /*
                 * The #of variables remaining in this tail predicate given the
                 * propagated bindings.
                 */
                final int varCount = rule.getTail(j).asBound(bindingSet)
                        .getVariableCount();

                if (DEBUG)
                    log.debug("j=" + j + ", tailIndex=" + tailIndex
                            + ", varCount=" + varCount + ", bindingSet="
                            + bindingSet);
                
                /*
                 * Range count of the current predicate under examination
                 * WITHOUT the propagated bindings (cached).
                 * 
                 * Note: We disregard the bindings that were propagated since
                 * they will NOT match anything anywhere and CAN NOT be used to
                 * obtain useful range counts.
                 */
                final long rangeCount = rangeCount(j);
                
                if (!empty && rangeCount == 0L) {

                    /*
                     * There will be no solutions, but we continue to assign an
                     * evaluation order in case the rule execution logic fails
                     * to test isEmpty().
                     */
                    
                    log.warn("Query is proven empty - no solutions for tail="
                            + rule.getTail(j));
                    
                    empty = true;
                    
                }
                
                if (varCount < minVarCount) {

                    if(DEBUG) {

                        log.debug("j=" + j + ", tailIndex=" + tailIndex
                                + ", varCount=" + varCount + ", minVarCount="
                                + minVarCount + ", rangeCount=" + rangeCount);
                        
                    }
                    
                    tailIndex = j;

                    minVarCount = varCount;

                } else if (varCount == minVarCount) {

                    /*
                     * Tweaks the evaluation order for predicates where the #of
                     * variable bindings is the same by examining the range
                     * counts.
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
                     * @todo Some tails use the same pattern of variables and
                     * constants in both predicates. E.g., rdfs11 is (u
                     * subClassOf v) AND (v subClassOf x). In these cases
                     * comparing range counts is pointless and could be avoided
                     * by testing for this pattern.
                     */
                    
                    // range count of the current best choice (cached).
                    final long minRangeCount = rangeCount(tailIndex);

                    if (DEBUG) {

                        log.debug("j=" + j + ", tailIndex=" + tailIndex
                                + ", varCount=" + varCount + ", minVarCount="
                                + minVarCount + ", rangeCount=" + rangeCount
                                + ", minRangeCount=" + minRangeCount);

                    }

                    if (rangeCount < minRangeCount) {

                        /*
                         * choose the predicate that is more selective in the
                         * data.
                         */
                        
                        tailIndex = j;
                        
                        minVarCount = varCount;
                        
                    }
                    
                } // end else if
                
            } // next j

            /*
             * Note: [tailIndex] is the index of the tail predicate that was
             * choosen as being the most selective out of those remaining.
             */
            
            if (tailIndex == -1)
                throw new AssertionError();
            
            if (used[tailIndex])
                throw new AssertionError(
                        "Attempting to reuse predicate: tailIndex=" + tailIndex
                                + "\n" + this);

            order[evalOrder] = tailIndex;
            
            if (DEBUG) {

                log.debug("choose tailIndex=" + tailIndex + " : minVarCount="
                        + minVarCount + ", rangeCount=" + rangeCount(tailIndex)
                        + ", order=" + Arrays.toString(order));
                
            }
            
            used[tailIndex] = true;
                    
            varCount[tailIndex] = minVarCount;

            /*
             * Populate varsBoundInRule given the current bindings. This
             * reflects the bindings on hand when we choose this predicate as
             * being the most selective out of those remaining BEFORE we
             * propagate the fake bindings that reflect the impact of evaluating
             * the choose predicate.
             */
            for (Iterator<IVariable> itr = rule.getVariables(); itr.hasNext();) {

                final IVariable var = itr.next();

                if (bindingSet.isBound(var)) {

                    varsBoundInRule[tailIndex].add(var);

                }

            }

            // set fake bindings for this predicate.
            {
                
                final IPredicate pred = rule.getTail(tailIndex);

                final int arity = pred.arity();

                for (int z = 0; z < arity; z++) {

                    final IVariableOrConstant v = pred.get(z);

                    if (v.isVar() && !bindingSet.isBound((IVariable)v)) {

                        if (DEBUG)
                            log.debug("binding: var=" + v + ", tailIndex="
                                    + tailIndex + ", bindingSet=" + bindingSet);
                        
                        bindingSet.set((IVariable) v, FAKE);

                        varsBoundByPred[tailIndex].add((IVariable)v);
                        
                    }

                }
                
            }
            
        } // choose the tail predicate for the next position in the evaluation order.

        if (INFO) {

            log.info(toString());
            
        }

    }
    
    /**
     * Used to mark fake bindings by {@link #computeEvaluationOrder()}.
     */
    private static final transient IConstant<String> FAKE = new Constant<String>(
            "Fake");

    /**
     * Return the range count for the predicate, ignoring any bindings. The
     * range count for the tail predicate is cached the first time it is
     * requested and returned from the cache thereafter.
     * 
     * @param tailIndex
     *            The index of the predicate in the tail of the rule.
     * 
     * @return The range count for that tail predicate.
     */
    public long rangeCount(final int tailIndex) {

        if (rangeCount[tailIndex] == -1L) {

            rangeCount[tailIndex] = joinNexus.getRangeCountFactory()
                    .rangeCount(rule.getTail(tailIndex));
            
        }

        return rangeCount[tailIndex];

    }

}
