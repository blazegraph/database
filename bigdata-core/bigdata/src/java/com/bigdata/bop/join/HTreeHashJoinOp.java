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
 * Created on Aug 14, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.ISingleThreadedOp;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * A hash join against an {@link IAccessPath} based on the {@link HTree} and
 * suitable for very large intermediate result sets. Source solutions are
 * buffered on the {@link HTree} on each evaluation pass. When the memory demand
 * of the {@link HTree} is not bounded, the hash join will run a single pass
 * over the {@link IAccessPath} for the target {@link IPredicate}. For some
 * queries, this can be more efficient than probing as-bound instances of the
 * target {@link IPredicate} using a nested indexed join, such as
 * {@link PipelineJoin}. This can also be more efficient on a cluster where the
 * key range scan of the target {@link IPredicate} will be performed using
 * predominately sequential IO.
 * <p>
 * If the {@link PipelineOp.Annotations#MAX_MEMORY} annotation is specified then
 * an evaluation pass over the target {@link IAccessPath} will be triggered if,
 * after having buffered some chunk of solutions on the {@link HTree}, the
 * memory demand of the {@link HTree} exceeds the capacity specified by that
 * annotation. This "blocked" evaluation trades off multiple scans of the target
 * {@link IPredicate} against the memory demand of the intermediate result set.
 * <p>
 * The source solutions presented to a hash join MUST have bindings for the
 * {@link HashJoinAnnotations#JOIN_VARS} in order to join (they can still
 * succeed as optionals if the join variables are not bound).
 * 
 * <h2>Handling OPTIONAL</h2>
 * 
 * An optional join makes life significantly more complex. For each source
 * solution we need to know whether or not it joined at least once with the
 * access path. A join can only occur when the source solution and the access
 * path have the same as-bound values for the join variables. However, the same
 * as-bound values can appear multiple times when scanning an access path, even
 * if the access path does not allow duplicates. For example, an SPO index scan
 * can many tuples with the same O. This means that we can not simply remove
 * source solution when they join as they might join more than once.
 * <p>
 * While it is easy enough to associate a flag or counter with each source
 * solution when running on the JVM heap, updating that flag or counter when the
 * data are on a persistent index is more expensive. Another approach is to
 * build up a second hash index (a "join set") of the solutions which joined and
 * then do a scan over the original hash index, writing out any solution which
 * is not in the joinSet. This is also expensive since we could wind up double
 * buffering the source solutions. Both approaches also require us to scan the
 * total multiset of the source solutions in order to detect and write out any
 * optional solutions. I've gone with the joinSet approach here as it reduces
 * the complexity associated with update of a per-solution counter in the hash
 * index.
 * <p>
 * Finally, note that "blocked" evaluation is not possible with OPTIONAL because
 * we must have ALL solutions on hand in order to decide which solutions did not
 * join. Therefore {@link PipelineOp.Annotations#MAX_MEMORY} must be set to
 * {@link Long#MAX_VALUE} when the {@link IPredicate} is
 * {@link IPredicate.Annotations#OPTIONAL}.
 * 
 * @see HTreeHashJoinUtility
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeHashJoinOp<E> extends HashJoinOp<E> implements
        ISingleThreadedOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashJoinOp.Annotations,
            HTreeHashJoinAnnotations {
        
    }
    
    /**
     * @param op
     */
    public HTreeHashJoinOp(final HTreeHashJoinOp<E> op) {
    
        super(op);
        
    }
    
    public HTreeHashJoinOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * @param args
     * @param annotations
     */
    public HTreeHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        assertMaxParallelOne();

        // Note: This is no longer true. It is now shared via the IQueryAttributes.
//        // shared state is used to share the hash table.
//        if (!isSharedState()) {
//            throw new UnsupportedOperationException(Annotations.SHARED_STATE
//                    + "=" + isSharedState());
//        }

        if (!isLastPassRequested()) {
            /*
             * Last pass evaluation must be requested. This operator relies on
             * last pass evaluation semantics to produce its outputs.
             */
            throw new IllegalArgumentException(PipelineOp.Annotations.LAST_PASS
                    + "=" + isLastPassRequested());
        }

        // Must be positive.  May be max long for unbounded memory.
        if (getMaxMemory() <= 0L)
            throw new UnsupportedOperationException(Annotations.MAX_MEMORY
                    + "=" + getMaxMemory());

        if (isOptional() && getMaxMemory() != Long.MAX_VALUE) {

            /*
             * An optional join requires that we buffer all solutions so we can
             * identify those which do not join. This makes it impossible to do
             * multiple evaluation passes. Therefore, if the join is OPTIONAL it
             * is illegal to place a limit on MAX_MEMORY for this operator.
             */

            throw new UnsupportedOperationException("Optional join, but "
                    + PipelineOp.Annotations.MAX_MEMORY + " is constrained");
        
        }
        
    }

    @Override
    protected IHashJoinUtility newState(final BOpContext<IBindingSet> context,
            final INamedSolutionSetRef namedSetRef, final JoinTypeEnum joinType) {

        return new HTreeHashJoinUtility(context.getRunningQuery()
                .getMemoryManager(), this, joinType);
    
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@link HTreeHashJoinOp} runs the hash join either exactly once
     * (at-once evaluation) or once a target memory threshold has been exceeded
     * (blocked evaluation).
     */
    @Override
    protected boolean runHashJoin(final BOpContext<?> context,
            final IHashJoinUtility state) {

        final long maxMemory = getMaxMemory();

        final long usedMemory = ((HTreeHashJoinUtility) state).getStore()
                .size();

        if (context.isLastInvocation() || usedMemory >= maxMemory) {

            return true;

        }

        return false;

    }
    
}
