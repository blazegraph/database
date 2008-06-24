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

package com.bigdata.join;

import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;

/**
 * Evaluation uses nested subquery and is optimized under the assumption that
 * the indices backing the {@link IAccessPath}s are all local (on the same
 * {@link AbstractJournal}). Under these assumptions we can submit a task to
 * the {@link ConcurrencyManager} that obtains all necessary locks and then runs
 * with the local B+Tree objects.
 * 
 * FIXME Write logic to extract the names of the indices that this task will
 * access from the rule. Note that the focusStore indices are typically on a
 * distinct temporary journal. Access to those indices needs to be handled quite
 * distinctly from access to the normal indices - there is no concurrency
 * control for those indices. (As an alternative, the temporary indices could be
 * created in temporary resources managed by a transaction for the federation.
 * This would impose concurrency control.) Regardless, the access should be to a
 * consistent historical checkpoint.
 * 
 * FIXME There needs to be an interface for choosing the JOIN operator impl. For
 * RDF with its perfect indices this is always going to use the same operator
 * for a given deployment (e.g., LDS vs Jini Federation).
 * 
 * Note that index maintenance is highly specialized for the RDF DB because of
 * its perfect indices. GOM is a more typical example where there may be a
 * primary (clustered) index and then zero or more secondary indices.
 * 
 * @todo JOINs all use the same eval strategy for the RDF DB but that is because
 *       there is one relation (the triples) and multiple indices over that
 *       relation (the access paths) (of course, in fact we only store the data
 *       in the indices and all data from the relation is replicated into each
 *       index so the relation is virtual - an abstraction only for RDF).
 * 
 * @todo make {@link Callable} and return {@link Iterator} if we are querying
 *       and otherwise <code>null</code> since the solutions were {inserted
 *       into, updated on, or removed from} the database?
 * 
 * @todo The {@link Program} can map the N passes in parallel (or N rules in
 *       parallel), each chunk[] from the first access path can be reordered for
 *       the next access path and the {@link ClientIndexView} can split the
 *       chunk[] and map N splits in parallel. This presumes that the
 *       {@link DataService} can function as a full {@link IBigdataClient} (the
 *       M/R architecture can also be layered on that assumption). Otherwise we
 *       bring all data back to the client from each JOIN before sending out the
 *       next JOIN. The different also results in JOIN at once vs solution at
 *       once processing.
 * 
 * @todo do an variant of this evaluation that runs as a procedure on a LDS and
 *       which assumes that all indices required by the various access paths are
 *       local. this evaluation strategy does not need to unroll anything since
 *       local access to the indices will be quite fast. the chunk[]s will help
 *       to maintain ordered reads on the indices which will futher improve
 *       performance.
 * 
 * @todo do a variant of this evaluation that assumes that the indices for the
 *       access paths are remote and partitioned. This evaluation strategy needs
 *       to take a chunk[] from the first access path and re-distribute, split
 *       that chunk according to the index partitions, and then send each split
 *       to the index partition for the 2nd access path where it will
 *       materialize a set of bindings that satisify the join and send them
 *       back. (A set at a time variant of the JOIN where the sets correspond to
 *       the index paritions and are bounded by the chunk size).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalNestedSubqueryEvaluator implements IRuleEvaluator {

    protected static final Logger log = Logger.getLogger(LocalNestedSubqueryEvaluator.class);
    
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
   
    private final RuleState state;
    private final IBuffer<ISolution> buffer;
    private final IBindingSet bindingSet;
    private final RuleStats ruleStats;
    
    public LocalNestedSubqueryEvaluator(RuleState ruleState, IBuffer<ISolution> buffer) {

        if (ruleState == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.state = ruleState;

        this.buffer = buffer;
        
        this.bindingSet = ruleState.newBindingSet();

        this.ruleStats = new RuleStats(ruleState);
        
    }
    
    /**
     * Recursively evaluate the subqueries
     */
    final public Object call() {
        
        final long begin = System.currentTimeMillis();

//        if(subqueryElimination) {
//
//            apply2( 0, state, EMPTY_SET);
//            
//        } else {
            
            apply1( 0, state );
            
//        }
        
        ruleStats.elapsed += System.currentTimeMillis() - begin;
        
        return null;
        
    }
    
    /**
     * Variant does not attempt subquery elimination.
     * 
     * @param index
     *            The current index in order[] that is being scanned.
     *            <p>
     *            Note: You MUST indirect through order, e.g., order[index], to
     *            obtain the index of the corresponding predicate in the
     *            evaluation order.
     * 
     * @todo it would be nicer if the order was encapsulated by an iterator over
     *       the predicates so that order[] was not directly used by the rule
     *       impls. however that might just be a bias in favor of set-at-a-time
     *       solution processing vs set a a time JOIN processing or some other
     *       JOIN strategy.
     */
    final private void apply1(final int index, RuleState state) {

        final IRule rule = state.getRule();
        
        final int tailCount = rule.getTailCount();
        
        if (index < 0 || index >= tailCount)
            throw new IllegalArgumentException();
        
        /*
         * Subquery iterator.
         */
        final IChunkedOrderedIterator itr = state.iterator(bindingSet,
                state.order[index]);
        
        try {

            while (itr.hasNext()) {

                // next chunk of results from that access path.
                final Object[] chunk = itr.nextChunk();

                if (index + 1 < tailCount) {

                    // nexted subquery.

                    for (Object e : chunk) {

                        if (log.isDebugEnabled()) {
                            log.debug("Considering: " + e.toString()
                                    + ", index=" + index + ", rule="
                                    + rule.getName());
                        }

                        ruleStats.nstmts[state.order[index]]++;

                        /*
                         * Then bind this statement, which propagates bindings
                         * to the next predicate (if the bindings are rejected
                         * then the solution would violate the constaints on the
                         * JOIN).
                         */

                        state.clearDownstreamBindings(bindingSet, index + 1);
                        
                        if (state.bind(bindingSet, state.order[index], e)) {

                            // run the subquery.
                            
                            ruleStats.nsubqueries[state.order[index]]++;

                            apply1(index + 1, state);
                            
                        }

                    }

                } else {

                    // bottomed out.

                    for (Object e : chunk) {

                        if (log.isDebugEnabled()) {
                            log.debug("Considering: " + e.toString()
                                    + ", index=" + index + ", rule="
                                    + rule.getName());
                        }

                        ruleStats.nstmts[state.order[index]]++;

                        // bind variables from the current element.
                        if (state.bind(bindingSet, state.order[index], e)) {

                            /*
                             * emit entailment
                             * 
                             * FIXME Compute the bindings on the head.
                             * 
                             * FIXME make bindingSet and rule optional. The rule
                             * reference could be safely attached but not
                             * serialized, but the bindingSet needs to be CLONED
                             * so keeping that is relatively costly.
                             */
                            buffer.add(null/*FIXME*/);//new Solution(null/*FIXME e*/,bindingSet,rule));
                            
                        }

                    }

                }

            } // while

        } finally {

            itr.close();

        }

    }

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
