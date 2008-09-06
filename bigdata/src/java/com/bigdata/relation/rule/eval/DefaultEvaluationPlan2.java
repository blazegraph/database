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
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.relation.rule.IRule;
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
public class DefaultEvaluationPlan2 implements IEvaluationPlan {

    protected static final Logger log = Logger.getLogger(DefaultEvaluationPlan2.class);
    
    protected static final boolean DEBUG = log.isDebugEnabled();
    
    private final IJoinNexus joinNexus;

    private final IRule rule;

    private final int tailCount;
    
    /**
     * The computed evaluation order. The elements in this array are the order
     * in which each tail predicate will be evaluated. The index into the array
     * is the index of the tail predicate whose evaluation order you want. So
     * <code>[2,0,1]</code> says that the predicates will be evaluated in the
     * order tail[2], then tail[0], then tail[1]. 
     */
    private int[/* order */] order;

    public int[] getOrder() {
        
        calc();
        
        return order;
        
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
    private long[/*tailIndex*/] rangeCount;
    
    /**
     * Keeps track of which tails have been used already and which still need
     * to be evaluated.
     */
    private boolean[/*tailIndex*/] used;
    
    /**
     * <code>true</code> iff the rule was proven to have no solutions.
     */
    private boolean empty = false;
    
    public boolean isEmpty() {
        return empty;
    }
    
    /**
     * Computes an evaluation plan for the rule.
     * 
     * @param joinNexus
     *            The join nexus.
     * @param rule
     *            The rule.
     */
    public DefaultEvaluationPlan2(IJoinNexus joinNexus, IRule rule) {
        
        if (joinNexus == null)
            throw new IllegalArgumentException();

        if (rule == null)
            throw new IllegalArgumentException();
        
        this.joinNexus = joinNexus;
        
        this.rule = rule;
        
        this.tailCount = rule.getTailCount();
    
        if(log.isDebugEnabled()) {
            
            log.debug("rule=" + rule);
            
        }
        
        calc();
        
    }
    
    /**
     * Compute the evaluation order.
     */
    protected void calc() {

        if (order != null)
            return;

        order = new int[tailCount];

        rangeCount = new long[tailCount];
        
        // initialize to -1L as indication that rangeCount was not taken yet.
        for(int i=0; i<tailCount; i++)
            rangeCount[i] = -1L;
        
        used = new boolean[tailCount];
        
        if (tailCount == 1) {
            order[0] = 0;
            return;
        }
        
        if (tailCount == 2) {
            order[0] = rangeCount(0) <= rangeCount(1) ? 0 : 1;
            order[1] = rangeCount(0) <= rangeCount(1) ? 1 : 0;
            return;
        }
        
        // clear arrays.
        for (int i = 0; i < tailCount; i++) {
            order[i] = -1; // -1 is used to detect logic errors.
            rangeCount[i] = -1L;  // -1L indicates no range count yet.
            used[i] = false;  // not yet evaluated
        }

        /*
         * There will be (tails-1) joins, we just need to figure out what
         * they should be.
         */
        Join join = getFirstJoin();
        int t1 = ((Tail) join.getD1()).getTail();
        int t2 = ((Tail) join.getD2()).getTail();
        order[0] = cardinality(t1) <= cardinality(t2) ? t1 : t2;
        order[1] = cardinality(t1) <= cardinality(t2) ? t2 : t1;
        used[order[0]] = true;
        used[order[1]] = true;
        for (int i = 2; i < tailCount; i++) {
            join = getNextJoin(join);
            order[i] = ((Tail) join.getD2()).getTail();
            used[order[i]] = true;
        }
        
    }
    
    /**
     * Start by looking at every possible initial join. Take every tail and
     * match it with every other tail to find the lowest possible cardinality.
     * See {@link #computeJoinCardinality(com.bigdata.relation.rule.eval.DefaultEvaluationPlan2.IJoinDimension, com.bigdata.relation.rule.eval.DefaultEvaluationPlan2.IJoinDimension)}
     * for more on this. 
     */
    private Join getFirstJoin() {
        if (DEBUG) {
            log.debug("evaluating first join");
        }
        long minCardinality = Long.MAX_VALUE;
        Tail minD1 = null;
        Tail minD2 = null;
        for (int i = 0; i < tailCount; i++) {
            Tail d1 = new Tail(i, rangeCount(i), getVars(i));
            for (int j = 0; j < tailCount; j++) {
                if (i == j) {
                    continue;
                }
                Tail d2 = new Tail(j, rangeCount(j), getVars(j));
                long joinCardinality = computeJoinCardinality(d1, d2);
                if(DEBUG) log.debug("evaluating " + i + " X " + j + ": cardinality= " + joinCardinality);
                if (joinCardinality < minCardinality) {
                    if(DEBUG) log.debug("found a new min: " + joinCardinality);
                    minCardinality = joinCardinality;
                    minD1 = d1;
                    minD2 = d2;
                }
            }
        }
        // the join variables is the union of the join dimensions' variables
        Set<String> vars = new HashSet<String>();
        vars.addAll(minD1.getVars());
        vars.addAll(minD2.getVars());
        return new Join(minD1, minD2, minCardinality, vars);
    }
    
    /**
     * Similar to {@link #getFirstJoin()}, but we have one join dimension
     * already calculated.
     * 
     * @param d1 
     *          the first join dimension
     * @return 
     *          the new join with the lowest cardinality from the remaining tails
     */
    private Join getNextJoin(IJoinDimension d1) {
        if (DEBUG) {
            log.debug("evaluating next join");
        }
        long minCardinality = Long.MAX_VALUE;
        Tail minTail = null;
        for (int i = 0; i < tailCount; i++) {
            // only check unused tails
            if (used[i]) {
                continue;
            }
            Tail tail = new Tail(i, rangeCount(i), getVars(i));
            long joinCardinality = computeJoinCardinality(d1, tail);
            if(DEBUG) log.debug("evaluating " + d1.toJoinString() + " X " + i + ": cardinality= " + joinCardinality);
            if (joinCardinality < minCardinality) {
                if(DEBUG) log.debug("found a new min: " + joinCardinality);
                minCardinality = joinCardinality;
                minTail = tail;
            }
        }
        // the join variables is the union of the join dimensions' variables
        Set<String> vars = new HashSet<String>();
        vars.addAll(d1.getVars());
        vars.addAll(minTail.getVars());
        return new Join(d1, minTail, minCardinality, vars);
    }
    
    /**
     * Return the range count for the predicate, ignoring any bindings. The
     * range count for the tail predicate is cached the first time it is
     * requested and returned from the cache thereafter. The range counts are
     * requested using the "non-exact" range count query, so the range counts
     * are actually the upper bound. However, if the upper bound is ZERO (0)
     * then the range count really is ZERO (0).
     * 
     * @param tailIndex
     *            The index of the predicate in the tail of the rule.
     * 
     * @return The range count for that tail predicate.
     */
    public long rangeCount(final int tailIndex) {

        if (rangeCount[tailIndex] == -1L) {

            final long rangeCount = joinNexus.getRangeCountFactory()
                    .rangeCount(rule.getTail(tailIndex));

            this.rangeCount[tailIndex] = rangeCount;

        }

        return rangeCount[tailIndex];

    }

    /**
     * Return the cardinality of a particular tail, which is the range count
     * if not optional and infinite if optional.
     */
    public long cardinality(final int tailIndex) {
        if (rule.getTail(tailIndex).isOptional()) {
            return Long.MAX_VALUE;
        } else {
            return rangeCount(tailIndex);
        }
    }
    
    public String toString() {
        return Arrays.toString(getOrder());
    }
    
    /**
     * This is the secret sauce.  There are three possibilities for computing
     * the join cardinality, which we are defining as the upper-bound for
     * solutions for a particular join.  First, if there are no shared variables
     * then the cardinality will just be the simple sum of the cardinality of
     * each join dimension.  If there are shared variables but no unshared
     * variables, then the cardinality will be the minimum cardinality from
     * the join dimensions.  If there are shared variables but also some
     * unshared variables, then the join cardinality will be the maximum
     * cardinality from each join dimension.
     * <p>
     * Any join involving an optional will have infinite cardinality, so that
     * optionals get placed at the end.
     *  
     * @param d1
     *          the first join dimension
     * @param d2
     *          the second join dimension
     * @return
     *          the join cardinality
     */
    protected long computeJoinCardinality(IJoinDimension d1, IJoinDimension d2) {
        if (d1.isOptional() || d2.isOptional()) {
            return Long.MAX_VALUE-1;
        }
        final boolean sharedVars = hasSharedVars(d1, d2);
        final boolean unsharedVars = hasUnsharedVars(d1, d2);
        final long joinCardinality;
        if (sharedVars == false) {
            // no shared vars - take the sum
            // joinCardinality = d1.getCardinality() + d2.getCardinality();
            // different approach - give preference to shared variables
            joinCardinality = Long.MAX_VALUE-1;
        } else {
            if (unsharedVars == false) {
                // shared vars and no unshared vars - take the min
                joinCardinality = 
                    Math.min(d1.getCardinality(), d2.getCardinality());
            } else {
                // shared vars and unshared vars - take the max
                joinCardinality = 
                    Math.max(d1.getCardinality(), d2.getCardinality());
            }
        }
        return joinCardinality;
    }
    
    /**
     * Get the named variables for a given tail.  Is there a better way to do
     * this?
     * 
     * @param tail
     *          the tail
     * @return
     *          the named variables
     */
    protected Set<String> getVars(int tail) {
        final Set<String> vars = new HashSet<String>();
        IVariableOrConstant s = rule.getTail(tail).get(0);
        if (s.isVar()) {
            vars.add(s.getName());
        }
        IVariableOrConstant p = rule.getTail(tail).get(1);
        if (p.isVar()) {
            vars.add(p.getName());
        }
        IVariableOrConstant o = rule.getTail(tail).get(2);
        if (o.isVar()) {
            vars.add(o.getName());
        }
        return vars;
    }
    
    /**
     * Look for shared variables.
     * 
     * @param d1
     *          the first join dimension
     * @param d2
     *          the second join dimension
     * @return
     *          true if there are shared variables, false otherwise
     */
    protected boolean hasSharedVars(IJoinDimension d1, IJoinDimension d2) {
        for(String var : d1.getVars()) {
            if (d2.getVars().contains(var)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Look for unshared variables.
     * 
     * @param d1
     *          the first join dimension
     * @param d2
     *          the second join dimension
     * @return
     *          true if there are unshared variables, false otherwise
     */
    protected boolean hasUnsharedVars(IJoinDimension d1, IJoinDimension d2) {
        for(String var : d1.getVars()) {
            if (d2.getVars().contains(var) == false) {
                return true;
            }
        }
        for(String var : d2.getVars()) {
            if (d1.getVars().contains(var) == false) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * A join dimension can be either a tail, or a previous join.  Either way
     * we need to know its cardinality, its variables, and its tails.
     */
    private interface IJoinDimension {
        long getCardinality();
        Set<String> getVars();
        String toJoinString();
        boolean isOptional();
    }

    /**
     * A join implementation of a join dimension. The join can consist of two
     * tails, or one tail and another join.  Theoretically it could be two
     * joins as well, which might be a future optimization worth thinking about.
     */
    private class Join implements IJoinDimension {
        
        private final IJoinDimension d1, d2;
        private final long cardinality;
        private final Set<String> vars;
        
        public Join(IJoinDimension d1, IJoinDimension d2, 
                    long cardinality, Set<String> vars) {
            this.d1 = d1;
            this.d2 = d2;
            this.cardinality = cardinality;
            this.vars = vars;
        }

        public IJoinDimension getD1() {
            return d1;
        }

        public IJoinDimension getD2() {
            return d2;
        }

        public Set<String> getVars() {
            return vars;
        }
        
        public long getCardinality() {
            return cardinality;
        }
        
        public boolean isOptional() {
            return false;
        }
        
        public String toJoinString() {
            return d1.toJoinString() + " X " + d2.toJoinString();
        }
        
    }
    
    /**
     * A tail implementation of a join dimension. 
     */
    private class Tail implements IJoinDimension {

        private final int tail;
        private final long cardinality;
        private final Set<String> vars;
        
        public Tail(int tail, long cardinality, Set<String> vars) {
            this.tail = tail;
            this.cardinality = cardinality;
            this.vars = vars;
        }

        public int getTail() {
            return tail;
        }

        public long getCardinality() {
            return cardinality;
        }

        public Set<String> getVars() {
            return vars;
        }
        
        public boolean isOptional() {
            return rule.getTail(tail).isOptional();
        }
        
        public String toJoinString() {
            return String.valueOf(tail);
        }
        
    }
    
}
