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
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.ISingleThreadedOp;
import com.bigdata.bop.NV;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * A hash join against an {@link IAccessPath} based on the Java collections
 * classes. Source solutions are buffered on the Java collection on each
 * evaluation pass. Once ALL source solutions have been buffered, the hash join
 * will run a single pass over the {@link IAccessPath} for the target
 * {@link IPredicate}. For some queries, this can be more efficient than probing
 * as-bound instances of the target {@link IPredicate} using a nested indexed
 * join, such as {@link PipelineJoin}. This can also be more efficient on a
 * cluster where the key range scan of the target {@link IPredicate} will be
 * performed using predominately sequential IO.
 * <p>
 * The source solutions presented to a hash join MUST have bindings for the
 * {@link HashJoinAnnotations#JOIN_VARS} in order to join (they can still
 * succeed as optionals if the join variables are not bound).
 * 
 * @see JVMHashJoinUtility
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JVMHashJoinOp<E> extends HashJoinOp<E> implements ISingleThreadedOp {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashJoinOp.Annotations,
            HashMapAnnotations {
        
    }
    
    /**
     * @param op
     */
    public JVMHashJoinOp(final JVMHashJoinOp<E> op) {
    
        super(op);
        
    }
    
    public JVMHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * @param args
     * @param annotations
     */
    public JVMHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        assertMaxParallelOne();

        assertAtOnceJavaHeapOp();

    }

    @Override
    protected IHashJoinUtility newState(final BOpContext<IBindingSet> context,
            final INamedSolutionSetRef namedSetRef, final JoinTypeEnum joinType) {

        return new JVMHashJoinUtility(this, joinType);
    
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@link JVMHashJoinOp} executes the hash join for each chunk of
     * intermediate solutions (it is not an "at-once" operator).
     * <p>
     * Note: Because this is an at-once operator, the solutions are all buffered
     * on the query engine and this operator is invoked exactly once.
     * <p>
     * Unlike the {@link HTreeHashJoinOp}, the concept of a LAST PASS evaluation
     * does not enter in to the evaluation of this operator. However, by
     * publishing the [state] on the query attribute we do gain visibility into
     * the dynamics of the hash join while it is executing against the B+Tree
     * access path.
     */
    @Override
    protected boolean runHashJoin(final BOpContext<?> context,
            final IHashJoinUtility state) {

        return true;
        
    }

}
