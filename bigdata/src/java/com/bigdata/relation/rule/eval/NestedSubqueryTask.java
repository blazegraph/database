/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 29, 2007
 */

package com.bigdata.relation.rule.eval;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Evaluation of an {@link IRule} using nested subquery (one or more JOINs plus
 * any filters specified for the predicates in the tail or the rule itself).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NestedSubqueryTask implements IStepTask {

    protected static final Logger log = Logger.getLogger(NestedSubqueryTask.class);
    
//    /**
//     * When true, enables subquery elimination by mapping a set of outer
//     * {@link SPO}s that would result in the same subquery across the subquery.
//     * The effect is that the subquery is executed once, but the same set of
//     * bindings are generated as if you did not perform subquery elimination.
//     * 
//     * @todo subquery elimination is not entirely working yet. there is an
//     *       assumption that there will be shared variables which is not always
//     *       true (fast query 11 and 13 are counter examples). There is also
//     *       clearly some other problem since it causes too many inferences to
//     *       be drawn - perhaps related to variable bindings not be set or
//     *       cleared correctly when the subqueries are formulated?
//     */
//    final boolean subqueryElimination = false;

    /*
     * from the ctor.
     */
    private final IJoinNexus joinNexus;
    private final IBuffer<ISolution> buffer;
    private final IBindingSet bindingSet;
    private RuleState ruleState;
    private RuleStats ruleStats;
    
    public NestedSubqueryTask(IRule rule, IJoinNexus joinNexus, IBuffer<ISolution> buffer) {

        if (rule == null)
            throw new IllegalArgumentException();

        if( joinNexus == null)
             throw new IllegalArgumentException();
        
        if (buffer == null)
            throw new IllegalArgumentException();

        this.joinNexus = joinNexus;
        
        this.buffer = buffer;

        this.bindingSet = joinNexus.newBindingSet(rule);

        this.ruleState = new RuleState(rule, joinNexus);

        this.ruleStats = new RuleStats(ruleState);
        
    }
    
    /**
     * Recursively evaluate the subqueries.
     */
    final public RuleStats call() {

        if(log.isDebugEnabled()) {
            
            log.debug("begin: ruleState="+ruleState);
            
        }
        
        final long begin = System.currentTimeMillis();

//        if(subqueryElimination) {
//
//            apply2( 0, EMPTY_SET);
//            
//        } else {
            
            apply1( 0 );
            
//        }
        
        ruleStats.elapsed += System.currentTimeMillis() - begin;
        
        if(log.isDebugEnabled()) {
            
            log.debug("done: ruleState="+ruleState+", ruleStats="+ruleStats);
            
        }
        
        return ruleStats;
        
    }
    
    /**
     * Variant does not attempt subquery elimination.
     * 
     * @param tailIndex
     *            The current index in order[] that is being scanned.
     *            <p>
     *            Note: You MUST indirect through order, e.g., order[index], to
     *            obtain the index of the corresponding predicate in the
     *            evaluation order.
     */
    final private void apply1(final int tailIndex) {

        final IRule rule = ruleState.getRule();
        
        final int tailCount = rule.getTailCount();
        
        // note: evaluation order is fixed by now.
        final int order = ruleState.order[tailIndex];
        
        if (tailIndex < 0 || tailIndex >= tailCount)
            throw new IllegalArgumentException();
        
        /*
         * Subquery iterator.
         */
        final IAccessPath accessPath = ruleState.getAccessPath(order, bindingSet);
        
        final IChunkedOrderedIterator itr = accessPath.iterator();
        
        try {

            while (itr.hasNext()) {

                // next chunk of results from that access path.
                final Object[] chunk = itr.nextChunk();

                ruleStats.chunkCount[order]++;

                if (tailIndex + 1 < tailCount) {

                    // nexted subquery.

                    for (Object e : chunk) {

                        if (log.isDebugEnabled()) {
                            log.debug("Considering: " + e.toString()
                                    + ", tailIndex=" + tailIndex + ", rule="
                                    + rule.getName());
                        }

                        ruleStats.elementCount[order]++;

                        /*
                         * Then bind this statement, which propagates bindings
                         * to the next predicate (if the bindings are rejected
                         * then the solution would violate the constaints on the
                         * JOIN).
                         */

                        ruleState.clearDownstreamBindings(tailIndex + 1, bindingSet);
                        
                        if (ruleState.bind(order, e, bindingSet)) {

                            // run the subquery.
                            
                            ruleStats.subqueryCount[order]++;

                            apply1(tailIndex + 1);//, ruleState);
                            
                        }

                    }

                } else {

                    // bottomed out.

                    for (Object e : chunk) {

                        if (log.isDebugEnabled()) {
                            log.debug("Considering: " + e.toString()
                                    + ", tailIndex=" + tailIndex + ", rule="
                                    + rule.getName());
                        }

                        ruleStats.elementCount[order]++;

                        // bind variables from the current element.
                        if (ruleState.bind(order, e, bindingSet)) {

                            /*
                             * emit entailment
                             */

                            if (log.isDebugEnabled()) {
                                log.debug("solution: " + bindingSet);
                            }

                            final ISolution solution = joinNexus.newSolution(
                                    rule, bindingSet);
            
                            ruleStats.solutionCount++;
                            
                            buffer.add( solution );
                            
                        }

                    }

                }

            } // while

        } finally {

            itr.close();

        }

    }

//    /**
//     * Variant does not attempt subquery elimination but unrolls each chunk of
//     * the outer query. The chunk is unrolled by forming a range query whose
//     * <i>fromKey</i> is determined by the first tuple in the chunk and whose
//     * <i>toKey</i> is determined by the last key in the chunk. The chunk
//     * itself is wrapped up in an {@link IFilterConstructor} and the tuples in
//     * the chunk are used to advance through (skip to) the possible matches on
//     * the subquery using the {@link ITupleCursor} interface.
//     * 
//     * @todo this should be refactored so that each JOIN can execute on a
//     *       different host where it is reading from the right-hand side of the
//     *       index. The last JOIN should return a smart proxy iterator back to
//     *       the client.
//     * 
//     * @todo since the subquery iterator will visit elements belonging to each
//     *       element in the source chunk, we need to extend the logic that
//     *       processes the source chunk elements to notice when the next element
//     *       from the subquery iterator is not a match and then to advance the
//     *       source chunk until we have a match.
//     *       <p>
//     *       An alternative is to simply send the source chunk elements through
//     *       to the next JOIN, having the final JOIN write the solutions onto
//     *       the buffer.
//     * 
//     * @todo the chunk could be sent to the {@link IIndex} as a byte[][]
//     *       (perhaps even without deserializing from {@link ITuple}s) and the
//     *       {@link IIndex} could realize the JOIN itself.
//     *       <p>
//     *       This would let a key-range partitioned index split the chunk
//     *       according to the index partitions so that each index partition
//     *       would only receive an iterator whose advancer would visit tuples in
//     *       that index partition.
//     *       <p>
//     *       This could also help for hash-partitioned indices since the split
//     *       logic would be different.
//     * 
//     * @todo Unrolling loops will be important for the LTS also in order to
//     *       reduce synchronization for the iterators in
//     *       {@link UnisolatedReadWriteIndex}.
//     * 
//     * @param tailIndex
//     *            The current index in {@link RuleState#order} that is being
//     *            scanned.
//     *            <p>
//     *            Note: You MUST indirect through order, e.g.,
//     *            <code>order[index]</code>, to obtain the index of the
//     *            corresponding tail predicate in the evaluation order.
//     * 
//     * @todo not realized yet and it might be refactored into the {@link IIndex}
//     *       to make this efficient.
//     */
//    final private void applyWithChunkUnrolled(final int tailIndex) {
//
//        final IRule rule = ruleState.getRule();
//        
//        final int tailCount = rule.getTailCount();
//        
//        // note: evaluation order is fixed by now.
//        final int order = ruleState.order[tailIndex];
//        
//        if (tailIndex < 0 || tailIndex >= tailCount)
//            throw new IllegalArgumentException();
//        
//        /*
//         * Subquery iterator.
//         */
//        final IAccessPath accessPath = ruleState.getAccessPath(order, bindingSet);
//        
//        final IChunkedOrderedIterator itr = accessPath.iterator();
//        
//        try {
//
//            while (itr.hasNext()) {
//
//                // next chunk of results from that access path.
//                final Object[] chunk = itr.nextChunk();
//
//                ruleStats.chunkCount[order]++;
//
//                if (tailIndex + 1 < tailCount) {
//
//                    // nexted subquery.
//
//                    for (Object e : chunk) {
//
//                        if (log.isDebugEnabled()) {
//                            log.debug("Considering: " + e.toString()
//                                    + ", tailIndex=" + tailIndex + ", rule="
//                                    + rule.getName());
//                        }
//
//                        ruleStats.elementCount[order]++;
//
//                        /*
//                         * Then bind this statement, which propagates bindings
//                         * to the next predicate (if the bindings are rejected
//                         * then the solution would violate the constaints on the
//                         * JOIN).
//                         */
//
//                        ruleState.clearDownstreamBindings(tailIndex + 1, bindingSet);
//                        
//                        if (ruleState.bind(order, e, bindingSet)) {
//
//                            // run the subquery.
//                            
//                            ruleStats.subqueryCount[order]++;
//
//                            apply1(tailIndex + 1);
//                            
//                        }
//
//                    }
//
//                } else {
//
//                    // bottomed out.
//
//                    for (Object e : chunk) {
//
//                        if (log.isDebugEnabled()) {
//                            log.debug("Considering: " + e.toString()
//                                    + ", tailIndex=" + tailIndex + ", rule="
//                                    + rule.getName());
//                        }
//
//                        ruleStats.elementCount[order]++;
//
//                        // bind variables from the current element.
//                        if (ruleState.bind(order, e, bindingSet)) {
//
//                            /*
//                             * emit entailment
//                             */
//
//                            if (log.isDebugEnabled()) {
//                                log.debug("solution: " + bindingSet);
//                            }
//
//                            final ISolution solution = joinNexus.newSolution(
//                                    rule, bindingSet);
//            
//                            ruleStats.solutionCount++;
//                            
//                            buffer.add( solution );
//                            
//                        }
//
//                    }
//
//                }
//
//            } // while
//
//        } finally {
//
//            itr.close();
//
//        }
//
//    }

//    /**
//     * 
//     * @param index
//     *            The current index in order[] that is being scanned.
//     *            <p>
//     *            Note: You MUST indirect through order, e.g., order[index], to
//     *            obtain the index of the corresponding predicate in the
//     *            evaluation order.
//     */
//    final private void apply2(final int index, State state, List<SPO> outerSet) {
//
//        assert index >= 0;
//        assert index < body.length;
//        
//        // the sort order for chunks for this iterator (iff there is a subquery).
//        final KeyOrder keyOrder = (index + 1 == body.length //
//                ? null // no subquery
//                : getSortOrder(state, state.order[index], state.order[index + 1]) // subquery
//                );
//        
//        /*
//         * Subquery iterator.
//         * 
//         * Note: if there was an outer subquery then the natural order of the
//         * inner subquery MUST match the order into which we sort each chunk of
//         * statement delivered by the outer subquery in order for subquery
//         * elimination to work.
//         */
//        final ISPOIterator itr = state.iterator(state.order[index]);
//        
//        while(itr.hasNext()) {
//
//            // next chunk of statements : sorted iff there is a subquery.
//            SPO[] chunk = (keyOrder == null ? itr.nextChunk() : itr
//                    .nextChunk(keyOrder));
//
//            if( index+1 < body.length ) {
//                
//                // nexted subquery.
//
//                for(int i=0; i<chunk.length; /*inc below*/) {
//
//                    if(subqueryElimination && outerSet.isEmpty()) {
//                    
//                        /*
//                         * Subquery elimination logic:
//                         */
//                        
//                        /*
//                         * Collect a sequence of statements that can be mapped
//                         * across the same subquery.
//                         */
//                        List<SPO> innerSet = getStatementsBindingSameSubquery(
//                                state, index, chunk, 0);
//                        
//                        // the #of statement in that sequence.
//                        final int n = innerSet.size();
//                        
//                        i += n;
//                    
//                        state.stats.nstmts[state.order[index]] += n;
//                        
//                        /*
//                         * Apply bindings for the 1st stmt (all statements in this
//                         * set will cause the same values to be bound for the
//                         * subquery so it does not matter which one you use here).
//                         */
//    
//                        state.clearDownstreamBindings(index+1);
//
//                        if( state.bind(state.order[index],innerSet.get(0)) ) {
//                        
//                            // run the subquery.
//                        
//                            state.stats.nsubqueries[state.order[index]]++;
//    
//                            apply2(index+1,state,innerSet);
//                            
//                        }
//                        
//                    } else {
//                    
//                        /*
//                         * @todo The logic does not support recursive subquery
//                         * elimination. In order to do that the outerSet would
//                         * have to become a list of sets in case we were able to
//                         * do subquery elimination for two or more predicates in
//                         * the order[] of evaluation.
//                         */
//                        
//                        state.stats.nstmts[state.order[index]]++;
//                        
//                        // Apply bindings for the the current statement.
//                        
//                        if( state.bind(state.order[index],chunk[i]) ) {
//                        
//                            // run the subquery.
//                        
//                            state.stats.nsubqueries[state.order[index]]++;
//    
//                            apply2(index+1,state,EMPTY_SET);
//
//                        }
//                        
//                        i++; // next in this chunk.
//                        
//                    }
//                    
//                }
//                
//            } else {
//                
//                // bottomed out.
//                
//                for( SPO stmt : chunk ) {
//                    
//                    state.stats.nstmts[state.order[index]]++;
//
//                    // bind this statement.
//                    
//                    if( state.bind(state.order[index],stmt) ) {
//
//                        if(outerSet.isEmpty()) {
//    
//                            state.emit();
//    
//                        } else {
//    
//                            for (SPO ostmt : outerSet) {
//    
//                                // bind the statement from the _outer_ query.
//                                
//                                if( state.bind(state.order[index-1],ostmt) ) {
//                            
//                                    state.emit();
//                                    
//                                }
//                            
//                            }
//    
//                        }
//                        
//                    }
//                    
//                }
//                
//            }
//            
//        }
//
//    }
//
//    /**
//     * At each level of nexted subquery evaluation, the statements are sorted
//     * such that the shared variable(s) with the nested subquery are ordered.
//     * This allows us to eliminate subqueries with duplicate bindings by
//     * skipping tuples in the outer query that would realize identical bindings
//     * on the inner query.
//     * 
//     * @param outerIndex
//     *            The index of the predicate for the outer query.
//     * 
//     * @param innerIndex
//     *            The index of the predicate for the inner (aka nested) query.
//     * 
//     * @return The sort order to be applied to each chunk of statements that we
//     *         visit in the outer query.
//     * 
//     * @throws IndexOutOfBoundsException
//     *             if either index is out of bounds.
//     */
//    private KeyOrder getSortOrder(State state,int outerIndex, int innerIndex) {
//    
//        Set<Var> sharedVars = getSharedVars(outerIndex, innerIndex);
//        
//        if (sharedVars.size() == 0) {
//
//            throw new AssertionError("No shared variables: outer=" + outerIndex
//                    + ", inner=" + innerIndex + ", " + this);
//            
//        }
//
//        /*
//         * Form a triple pattern that will have the correct order for
//         * correlating the outer and inner subqueries so that we can eliminate
//         * redundent subqueries.
//         * 
//         * If the variable is shared between the two predicates then the
//         * corresponding position in the triple pattern will be non-NULL (it is
//         * set to a fake constant since it will be bound when we evaluate the
//         * subquery).
//         * 
//         * Otherwise the position in the triple pattern will be the currently
//         * bound value.  This covers three distinct cases:
//         * 
//         * 1) If the position is a constant, then this will be a constant.
//         * 
//         * 2) If the position is a variable AND the variable was already bound
//         * when evaluating a previous triple pattern, then the position will be
//         * a constant.
//         * 
//         * 3) Finally, if the position is a variable and it is unbound, then the
//         * subquery will be unbound in that position and the position will be
//         * NULL.
//         */
//        
//        // a fake constant.
//        final long c = -1; 
//        
//        long s = sharedVars.contains(body[innerIndex].s)?c:state.get(body[innerIndex].s);
//        long p = sharedVars.contains(body[innerIndex].p)?c:state.get(body[innerIndex].p);
//        long o = sharedVars.contains(body[innerIndex].o)?c:state.get(body[innerIndex].o);
//        
//        IAccessPath accessPath = state.database.getAccessPath(s, p, o);
//        
//        KeyOrder keyOrder = accessPath.getKeyOrder(); 
//        
//        return keyOrder;
//        
//    }
//
//    /**
//     * 
//     * @param index
//     *            The index into {@link Rule#order} corresponding to the
//     *            predicate for which the chunk of statements was materialized.
//     * @param chunk
//     *            A chunk of {@link SPO}s sorted according to the natural order
//     *            that will be used by the triple pattern for the subquery.
//     * @param i
//     *            The starting index in <i>chunk</i>
//     * 
//     * @return The set of sequentially occurring {@link SPO}s in <i>chunk</i>
//     *         that will impose the same bindings on the subquery.
//     */
//    private List<SPO> getStatementsBindingSameSubquery(State state, int index,
//            SPO[] chunk, int i) {
//
//        // variables shared between the current predicate and the subquery.
//        Set<Var> sharedVars = getSharedVars(state.order[index],
//                state.order[index + 1]);
//
//        if(sharedVars.isEmpty()) {
//            
//            /*
//             * if nothing is shared then all statements will cause the same
//             * subquery to be issued (this would be pretty unusual).
//             */
//            
//            return Arrays.asList(chunk);
//            
//        }
//
//        // consider the inner predicate.
//        Predicate inner = body[state.order[index+1]];
//
//        // set of statements that result in the same subquery.
//        List<SPO> ret = new LinkedList<SPO>();
//        
//        /*
//         * collect up a sequence of SPOs from the chunk[] in which the variables
//         * shared with the subquery are invariant across the sequence.
//         */
//        
//        while (i < chunk.length) {
//        
//            if (sharedVars.contains(inner.s)) {
//                
//                if (chunk[0].s != chunk[i].s)
//                    break;
//                
//            }
//
//            if (sharedVars.contains(inner.p)) {
//                
//                if (chunk[0].p != chunk[i].p)
//                    break;
//                
//            }
//
//            if (sharedVars.contains(inner.o)) {
//                
//                if (chunk[0].o != chunk[i].o)
//                    break;
//                
//            }
//            
//            ret.add(chunk[i++]);
//            
//        }
//
//        return ret;
//
//    }
//    
//    private static final List<SPO> EMPTY_SET = Collections
//            .unmodifiableList(new LinkedList<SPO>());

}
