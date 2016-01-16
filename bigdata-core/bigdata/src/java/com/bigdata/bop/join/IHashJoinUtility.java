/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General License for more details.

You should have received a copy of the GNU General License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Nov 8, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBuffer;
import com.ibm.icu.util.BytesTrie.Iterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Interface for hash index build and hash join operations.
 * 
 * <h2>Use cases</h2>
 * 
 * For a JOIN, there are two core steps, plus one additional step if the join is
 * optional. The hash join logically has a <em>Left Hand Side</em> (LHS) and a
 * Right Hand Side (RHS). The RHS is used to build up a hash index which is then
 * probed for each LHS solution. The LHS is generally an access path scan, which
 * is done once. A hash join therefore provides an alternative to a nested index
 * join in which we visit the access path once, probing the hash index for
 * solutions which join.
 * <dl>
 * <dt>Accept solutions</dt>
 * <dd>This step builds the hash index, also known as the RHS (Right Hand Side).
 * </dd>
 * <dt>hash join</dt>
 * <dd>The hash join considers each left solution in turn and outputs solutions
 * which join. If optionals are required, this step also builds an hash index
 * (the <i>joinSet</i>) over the right solutions which did join.</dd>
 * <dt>Output optionals</dt>
 * <dd>The RHS hash index is scanned and the <i>joinSet</i> is probed to
 * identify right solutions which did not join with any left solution. Those
 * solutions are output as "optionals".</dd>
 * </dl>
 * <p>
 * This class also supports DISTINCT SOLUTIONS filters. For this use case, the
 * caller uses {@link #filterSolutions(ICloseableIterator, BOpStats, IBuffer)}
 * method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHashJoinUtility {
    
    /**
     * Return the type safe enumeration indicating what kind of operation is to
     * be performed.
     */
    JoinTypeEnum getJoinType();
    
    /**
     * The variable bound based on whether or not a solution survives an
     * "EXISTS" graph pattern (optional).
     * 
     * @see HashJoinAnnotations#ASK_VAR
     */
    IVariable<?> getAskVar();
    
    /**
     * The join variables.
     * 
     * @see HashJoinAnnotations#JOIN_VARS
     */
    IVariable<?>[] getJoinVars();

    /**
     * The variables to be retained (optional, all variables are retained if
     * not specified).
     * 
     * @see JoinAnnotations#SELECT
     */
    IVariable<?>[] getSelectVars();

    /**
     * Returns true if the projection outputs the distinct join vars (in
     * that case, the variables delivered by {{@link #getSelectVars()} will
     * be ignored, might even be uninitialized). See
     * {@link HashJoinAnnotations#OUTPUT_DISTINCT_JVs}.
     */
    public boolean isOutputDistinctJoinVars();
    
    /**
     * The join constraints (optional).
     * 
     * @see JoinAnnotations#CONSTRAINTS
     */
    IConstraint[] getConstraints();
    
    /**
     * Return <code>true</code> iff there are no solutions in the hash index.
     */
    boolean isEmpty();

    /**
     * Return the #of solutions in the hash index.
     */
    long getRightSolutionCount();

    /**
     * Discard the hash index.
     */
    void release();

    /**
     * Buffer solutions on a hash index.
     * <p>
     * When <code>optional:=true</code>, solutions which do not have a binding
     * for one or more of the join variables will be inserted into the hash
     * index anyway using <code>hashCode:=1</code>. This allows the solutions to
     * be discovered when we scan the hash index and the set of solutions which
     * did join to identify the optional solutions.
     * 
     * @param itr
     *            The source from which the solutions will be drained.
     * @param stats
     *            The statistics to be updated as the solutions are buffered on
     *            the hash index.
     * 
     * @return The #of solutions that were buffered.
     */
    long acceptSolutions(ICloseableIterator<IBindingSet[]> itr,
            BOpStats stats);

    /**
     * Filter solutions, writing only the DISTINCT solutions onto the sink.
     * 
     * @param itr
     *            The source solutions.
     * @param stats
     *            The stats to be updated.
     * @param sink
     *            The sink.
     *            
     * @return The #of source solutions which pass the filter.
     */
    long filterSolutions(ICloseableIterator<IBindingSet[]> itr,
            BOpStats stats, IBuffer<IBindingSet> sink);

    /**
     * Do a hash join between a stream of source solutions (left) and a hash
     * index (right). For each left solution, the hash index (right) is probed
     * for possible matches (solutions whose as-bound values for the join
     * variables produce the same hash code). Possible matches are tested for
     * consistency and the constraints (if any) are applied. Solutions which
     * join are written on the caller's buffer.
     * <p>
     * Note: Some {@link JoinTypeEnum}s have side-effects on the join state. For
     * this joins, once method has been invoked for the final time, you must
     * then invoke either {@link #outputOptionals(IBuffer)} (Optional or
     * NotExists) or {@link #outputJoinSet(IBuffer)} (Exists).
     * 
     * @param leftItr
     *            A stream of chunks of solutions to be joined against the hash
     *            index (left).
     * @param stats
     *            The statistics to be updated as solutions are drained from the
     *            <i>leftItr</i> (optional). When <code>left</code> is the
     *            pipeline, {@link BOpStats#chunksIn} and
     *            {@link BOpStats#unitsIn} should be updated by passing in the
     *            {@link BOpStats} object. When <code>left</code> is a hash
     *            index (i.e., for a hash join against an access path), you
     *            should pass <code>null</code> since the chunksIn and unitsIn
     *            are updated as the {@link HashIndexOp} builds the hash index
     *            rather than when it executes the join against the access
     *            path).
     * @param outputBuffer
     *            Where to write the solutions which join.
     */
    void hashJoin(//
            ICloseableIterator<IBindingSet[]> leftItr,//
            BOpStats stats,//
            IBuffer<IBindingSet> outputBuffer//
    );

    /**
     * Variant hash join method allows the caller to impose different
     * constraints or additional constraints. This is used to impose join
     * constraints when a solution set is joined back into a query based on the
     * join filters in the join group in which the solution set is included.
     * <p>
     * Note: Some {@link JoinTypeEnum}s have side-effects on the join state. For
     * this joins, once method has been invoked for the final time, you must
     * then invoke either {@link #outputOptionals(IBuffer)} (Optional or
     * NotExists) or {@link #outputJoinSet(IBuffer)} (Exists).
     * 
     * @param leftItr
     *            A stream of chunks of solutions to be joined against the hash
     *            index (left).
     * @param stats
     *            The statistics to be updated as solutions are drained from the
     *            <i>leftItr</i>.
     * @param outputBuffer
     *            Where to write the solutions which join.
     * @param constraints
     *            Constraints attached to this join (optional). Any constraints
     *            specified here are combined with those specified in the
     *            constructor.
     */
    void hashJoin2(//
            ICloseableIterator<IBindingSet[]> leftItr,//
            BOpStats stats,//
            IBuffer<IBindingSet> outputBuffer,//
            IConstraint[] constraints//
    );

    /**
     * Perform an N-way merge join. For an OPTIONAL join, <i>this</i> instance
     * is understood to be the index having the "required" solutions.
     * <p>
     * The merge join takes a set of solution sets in the some order and having
     * the same join variables. It examines the next solution in order for each
     * solution set and compares them. For each solution set which reported a
     * solution having the same join variables as that earliest solution, it
     * outputs the cross product and advances the iterator on that solution set.
     * <p>
     * The iterators draining the source solution sets need to be synchronized
     * such that we consider only solutions having the same hash code in each
     * cycle of the MERGE JOIN. The synchronization step is different depending
     * on whether or not the MERGE JOIN is OPTIONAL.
     * <p>
     * If the MERGE JOIN is REQUIRED, then we want to synchronize the source
     * solution iterators on the next lowest key (aka hash code) which they all
     * have in common.
     * <p>
     * If the MERGE JOIN is OPTIONAL, then we want to synchronize the source
     * solution iterators on the next lowest key (aka hash code) which appears
     * for any source iterator. Solutions will not be drawn from iterators not
     * having that key in that pass.
     * <p>
     * Note that each hash code may be an alias for solutions having different
     * values for their join variables. Such solutions will not join. However,
     * only solutions having the same values for the hash code can join. Thus,
     * by proceeding with synchronized iterators and operating only on solutions
     * having the same hash code in each round, we will consider all solutions
     * which COULD join with one another in each round.
     * <p>
     * Note: If the solutions are not in a stable and mutually consistent order
     * by hash code in the hash indices then the solutions in each hash index
     * MUST be SORTED before proceeding. (The {@link HTree} maintains solutions
     * in such an order but the JVM collections do not.)
     * 
     * @param others
     *            The other solution sets to be joined. All instances must be of
     *            the same concrete type as <i>this</i>.
     * @param outputBuffer
     *            Where to write the solutions.
     * @param constraints
     *            The join constraints.
     * @param optional
     *            <code>true</code> iff the join is optional.
     */
    void mergeJoin(//
            IHashJoinUtility[] others,//
            IBuffer<IBindingSet> outputBuffer,//
            IConstraint[] constraints,//
            boolean optional//
    );
    
    /**
     * Checkpoint the generated hash index such that it becomes safe for
     * concurrent readers.
     */
    void saveSolutionSet();

    /**
     * Identify and output the optional solutions. This is used with OPTIONAL
     * and NOT EXISTS.
     * <p>
     * Optionals are identified using a <i>joinSet</i> containing each right
     * solution which joined with at least one left solution. The total set of
     * right solutions is then scanned once. For each right solution, we probe
     * the <i>joinSet</i>. If the right solution did not join, then it is output
     * now as an optional join.
     * 
     * @param outputBuffer
     *            Where to write the optional solutions.
     */
    void outputOptionals(IBuffer<IBindingSet> outputBuffer);

    /**
     * Output the solutions buffered in the hash index. This is used when an
     * operator is building a hash index for use by a downstream operator.
     * 
     * @param out
     *            Where to write the solutions.
     */
    void outputSolutions(IBuffer<IBindingSet> out);

    /**
     * Return an {@link Iterator} that visits all solutions in the index (index
     * scan). The visited solutions MAY contain variables that would not be
     * projected out of the hash join.
     * <p>
     * Note: This is very nearly the same as {@link #outputSolutions(IBuffer)}
     * except that the latter only outputs the projected variables and it writes
     * onto an {@link IBuffer} rather than returning an
     * {@link ICloseableIterator}.
     * 
     * @return The {@link Iterator}.
     */
    ICloseableIterator<IBindingSet> indexScan();
    
    /**
     * Output the solutions which joined. This is used with EXISTS.
     * 
     * @param out
     *            Where to write the solutions.
     */
    void outputJoinSet(IBuffer<IBindingSet> out);
    
}
