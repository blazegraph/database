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
 * Created on Aug 14, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IShardwisePipelineOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBindingSetAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.rwstore.sector.MemStore;

/**
 * A hash join based on the {@link HTree} and suitable for very large
 * intermediate result sets. Intermediate results are buffered on the
 * {@link HTree} on each evaluation pass. When the memory demand of the
 * {@link HTree} is not bounded, the hash join will run a single pass over the
 * {@link IAccessPath} for the target {@link IPredicate}. For some queries, this
 * can be more efficient than probing as-bound instances of the target
 * {@link IPredicate} using a nested indexed join, such as {@link PipelineOp}.
 * This can also be more efficient on a cluster where the key range scan of the
 * target {@link IPredicate} will be performed using predominately sequential
 * IO.
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HTreeHashJoinOp<E> extends PipelineOp implements
        IShardwisePipelineOp<E> {
    
    static private final transient Logger log = Logger
            .getLogger(HTreeHashJoinOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AccessPathJoinAnnotations,
            HTreeAnnotations, HashJoinAnnotations {
        
    }
    
    /**
     * @param op
     */
    public HTreeHashJoinOp(final HTreeHashJoinOp<E> op) {
    
        super(op);
        
    }
    
    public HTreeHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * @param args
     * @param annotations
     */
    public HTreeHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        switch (getEvaluationContext()) {
        case CONTROLLER:
        case SHARDED:
        case HASHED:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1)
            throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
                    + "=" + getMaxParallel());

        // Note: This is no longer true. It is now shared via the IQueryAttributes.
//        // shared state is used to share the hash table.
//        if (!isSharedState()) {
//            throw new UnsupportedOperationException(Annotations.SHARED_STATE
//                    + "=" + isSharedState());
//        }

        // Must be positive.  May be max long for unbounded memory.
        if (getMaxMemory() <= 0L)
            throw new UnsupportedOperationException(Annotations.MAX_MEMORY
                    + "=" + getMaxMemory());

        // Predicate for the access path must be specified.
        getPredicate();

        // Join variables must be specified.
        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

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

    /**
     * {@inheritDoc}
     * 
     * @see Annotations#PREDICATE
     */
    @SuppressWarnings("unchecked")
    public IPredicate<E> getPredicate() {

        return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);

    }
    
    /**
     * Return <code>true</code> iff the predicate associated with the join is
     * optional.
     * 
     * @see IPredicate.Annotations#OPTIONAL
     */
    private boolean isOptional() {
        
        return getPredicate().isOptional();
        
    }
    
    /**
     * 
     * @see Annotations#CONSTRAINTS
     */
    public IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }    

    /**
     * @see Annotations#ADDRESS_BITS
     */
    public int getAddressBits() {

        return getProperty(Annotations.ADDRESS_BITS,
                Annotations.DEFAULT_ADDRESS_BITS);

    }

    /**
     * @see Annotations#RAW_RECORDS
     */
    public boolean getRawRecords() {

        return getProperty(Annotations.RAW_RECORDS,
                Annotations.DEFAULT_RAW_RECORDS);

    }
    
    /**
     * @see Annotations#MAX_RECLEN
     */
    public int getMaxRecLen() {

        return getProperty(Annotations.MAX_RECLEN,
                Annotations.DEFAULT_MAX_RECLEN);

    }

    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<E>(context, this));
        
    }

    /**
     * Task executing on the node.
     */
    private static class ChunkTask<E> implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final HTreeHashJoinOp<E> op;

        private final IRelation<E> relation;
        
        private final IPredicate<E> pred;
        
        private final IVariable<E>[] joinVars;
        
        private final IConstraint[] constraints;

        private final IVariable<?>[] selectVars;
        
        private final boolean optional;
        
        private final BaseJoinStats stats;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        /**
         * A map whose keys are the bindings on the specified variables. The
         * values in the map are <code>null</code>s.
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         */
        private final HTree rightSolutions;

        /**
         * The set of distinct source solutions which joined. This set is
         * maintained iff the join is optional.
         */
        private final HTree joinSet;

        @SuppressWarnings("unchecked")
        public ChunkTask(final BOpContext<IBindingSet> context,
                final HTreeHashJoinOp<E> op) {

            this.context = context;

            this.stats = (BaseJoinStats) context.getStats();

            this.pred = op.getPredicate();

            this.relation = context.getRelation(pred);

            this.selectVars = (IVariable<?>[]) op
                    .getProperty(Annotations.SELECT);

            this.joinVars = (IVariable<E>[]) op
                    .getRequiredProperty(Annotations.JOIN_VARS);
            
            this.constraints = op.constraints();

            this.optional = op.isOptional();

            this.sink = context.getSink();

            this.sink2 = context.getSink2();

            this.op = op;

            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 */
                final IQueryAttributes attrs = context.getRunningQuery()
                        .getAttributes();

                final Object sourceSolutionsKey = op.getId()
                        + ".sourceSolutions";

                final Object joinSetKey = op.getId() + ".joinSet";

                HTree rightSolutions = (HTree) attrs.get(sourceSolutionsKey);

                HTree joinSet = (HTree) attrs.get(joinSetKey);

                if (rightSolutions == null) {

                    /*
                     * Create the map(s).
                     */
                    
                    final IndexMetadata metadata = new IndexMetadata(
                            UUID.randomUUID());

                    metadata.setAddressBits(op.getAddressBits());

                    metadata.setRawRecords(op.getRawRecords());

                    metadata.setMaxRecLen(op.getMaxRecLen());

                    metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code
                                                          // keys.

                    /*
                     * TODO This sets up a tuple serializer for a presumed case
                     * of 4 byte keys (the buffer will be resized if necessary)
                     * and explicitly chooses the SimpleRabaCoder as a
                     * workaround since the keys IRaba for the HTree does not
                     * report true for isKeys(). Once we work through an
                     * optimized bucket page design we can revisit this as the
                     * FrontCodedRabaCoder should be a good choice, but it
                     * currently requires isKeys() to return true.
                     */
                    final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                            new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                            new SimpleRabaCoder(),// keys : TODO Optimize for int32!
                            new SimpleRabaCoder() // vals
                    );

                    metadata.setTupleSerializer(tupleSer);

                    /*
                     * This wraps an efficient raw store interface around a
                     * child memory manager created from the IMemoryManager
                     * which is backing the query.
                     */
                    final IRawStore store = new MemStore(context
                            .getRunningQuery().getMemoryManager()
                            .createAllocationContext());

                    // Will support incremental eviction and persistence.
                    rightSolutions = HTree.create(store, metadata);

                    // Used to handle optionals.
                    joinSet = op.isOptional() ? HTree.create(store,
                            metadata.clone()) : null;

                    if (attrs.putIfAbsent(sourceSolutionsKey, rightSolutions) != null)
                        throw new AssertionError();

                    if (joinSet != null) {
                        if (attrs.putIfAbsent(joinSetKey, joinSet) != null)
                            throw new AssertionError();
                    }
                    
                }

                // The map is shared state across invocations of this operator
                // task.
                this.rightSolutions = rightSolutions;

                // defined iff the join is optional.
                this.joinSet = joinSet;

            }

        }

        /**
         * Discard the {@link HTree} data.
         */
        private void release() {

            if (joinSet != null) {

                joinSet.close();

//                joinSet = null;
                
            }

            if (rightSolutions != null) {

                final IRawStore store = rightSolutions.getStore();

                rightSolutions.close();
                
//                sourceSolutions = null;
                
                store.close();

            }

        }
        
        public Void call() throws Exception {

            try {

                acceptSolutions();

                final long maxMemory = op.getMaxMemory();

                final long usedMemory = rightSolutions.getStore().size();
                
                if (context.isLastInvocation() || usedMemory >= maxMemory) {

                    doHashJoin();
                    
                }

                // Done.
                return null;
                
            } finally {

                if (context.isLastInvocation()) {

                    release();

                }
                
                sink.close();

                if (sink2 != null)
                    sink2.close();
                
            }

        }

        /**
         * Buffer intermediate resources on the {@link HTree}.
         */
        private void acceptSolutions() {

            HashJoinUtility.acceptSolutions(context.getSource(), joinVars,
                    stats, rightSolutions, optional);

        }

        /**
         * Do a hash join of the buffered solutions with the access path.
         */
        private void doHashJoin() {

            if (rightSolutions.getEntryCount() == 0)
                return;
            
            final IAccessPath<?> accessPath = context.getAccessPath(relation,
                    pred);

            if (log.isDebugEnabled()) {
                log.debug("rightSolutions=" + rightSolutions.getEntryCount());
                log.debug("joinVars=" + Arrays.toString(joinVars));
                log.debug("accessPath=" + accessPath);
            }

            stats.accessPathCount.increment();

            stats.accessPathRangeCount.add(accessPath
                    .rangeCount(false/* exact */));

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            HashJoinUtility.hashJoin(
                    ((IBindingSetAccessPath<?>)accessPath).solutions(stats),// left
                    unsyncBuffer, joinVars, selectVars, constraints,
                    rightSolutions, joinSet, optional, false/*leftIsPipeline*/);

            if (optional) {

                // where to write the optional solutions.
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
                        : new UnsyncLocalOutputBuffer<IBindingSet>(
                                op.getChunkCapacity(), sink2);

                HashJoinUtility.outputOptionals(unsyncBuffer2, rightSolutions,
                        joinSet);

                unsyncBuffer2.flush();
                if (sink2 != null)
                    sink2.flush();

            }

            unsyncBuffer.flush();
            sink.flush();

        }
        
    } // class ChunkTask

}
