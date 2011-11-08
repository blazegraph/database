/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Nov 8, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.ICloseableIterator;

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
     * Return <code>true</code> iff this is a JOIN with OPTIONAL semantics.
     * 
     * @see IPredicate.Annotations#OPTIONAL
     */
    public boolean isOptional();
    
    /**
     * Return <code>true</code> iff this is a DISTINCT SOLUTIONS filter.
     */
    public boolean isFilter();
    
    /**
     * The join variables.
     * 
     * @see HashJoinAnnotations#JOIN_VARS
     */
    public IVariable<?>[] getJoinVars();

    /**
     * The variables to be retained (optional, all variables are retained if
     * not specified).
     * 
     * @see JoinAnnotations#SELECT
     */
    public IVariable<?>[] getSelectVars();

    /**
     * The join constraints (optional).
     * 
     * @see JoinAnnotations#CONSTRAINTS
     */
    public IConstraint[] getConstraints();
    
    /**
     * Return <code>true</code> iff there are no solutions in the hash index.
     */
    public boolean isEmpty();

    /**
     * Return the #of solutions in the hash index.
     */
    public long getRightSolutionCount();

    /**
     * Discard the hash index.
     */
    public void release();

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
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats);

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
    public long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink);

    /**
     * Do a hash join between a stream of source solutions (left) and a hash
     * index (right). For each left solution, the hash index (right) is probed
     * for possible matches (solutions whose as-bound values for the join
     * variables produce the same hash code). Possible matches are tested for
     * consistency and the constraints (if any) are applied. Solutions which
     * join are written on the caller's buffer.
     * 
     * @param leftItr
     *            A stream of solutions to be joined against the hash index
     *            (left).
     * @param outputBuffer
     *            Where to write the solutions which join.
     * @param leftIsPipeline
     *            <code>true</code> iff <i>left</i> is a solution from upstream
     *            in the query pipeline. Otherwise, <i>right</i> is the upstream
     *            solution.
     * 
     *            TODO Drop [leftIsPipeline]
     */
    public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final boolean leftIsPipeline//
    );

    /**
     * Variant hash join method allows the caller to impose different
     * constraints or additional constraints. This is used to impose join
     * constraints when a solution set is joined back into a query based on the
     * join filters in the join group in which the solution set is included.
     * 
     * @param leftItr
     * @param outputBuffer
     * @param leftIsPipeline
     * @param constraints
     * 
     *            TODO Drop [leftIsPipeline]
     */
    public void hashJoin2(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final boolean leftIsPipeline,//
            final IConstraint[] constraints//
    );

    /**
     * Identify and output the optional solutions. Optionals are identified
     * using a <i>joinSet</i> containing each right solution which joined with
     * at least one left solution. The total set of right solutions is then
     * scanned once. For each right solution, we probe the <i>joinSet</i>. If
     * the right solution did not join, then it is output now as an optional
     * join.
     * 
     * @param outputBuffer
     *            Where to write the optional solutions.
     */
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer);

    /**
     * Output the solutions buffered in the hash index. This is used when an
     * operator is building a hash index for use by a downstream operator.
     * 
     * @param out
     *            Where to write the solutions.
     */
    public void outputSolutions(final IBuffer<IBindingSet> out);

}