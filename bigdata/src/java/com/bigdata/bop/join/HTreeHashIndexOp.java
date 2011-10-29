/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.SubqueryJoinAnnotations;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.rwstore.sector.MemStore;

/**
 * Operator builds an {@link HTree} index from the source solutions. Once all
 * source solutions have been materialized on the {@link HTree}, the source
 * solutions are output on the default sink. The set of variables to be copied
 * to the sink may be restricted by an annotation.
 * <p>
 * There are two basic use cases for the {@link HTreeHashIndexOp}, both of which rely
 * on a {@link HTreeSolutionSetHashJoinOp} to re-integrate the results buffered on
 * the {@link HTree}.
 * <p>
 * The first use case is when we will run an OPTIONAL group. In this case, an
 * OPTIONAL hash join will be used and a buffered solution will be output if
 * there was no solution in the optional group for that buffered solution. All
 * known bound variables should be used as the join variables. All variables
 * should be selected.
 * <p>
 * The second use case is when we will run a sub-select. In this case, only the
 * variables which are projected by the subquery should be selected. Those will
 * also serve as the join variables. The hash join will integrate the solutions
 * from the subquery with the buffered solutions using those join variables. The
 * integrated solutions will be the net output of the hash join.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization. The operator MUST be run on the query controller.
 * 
 * @see HTreeSolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeHashIndexOp extends PipelineOp {

//    static private final transient Logger log = Logger
//            .getLogger(HashIndexOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HTreeAnnotations, HashJoinAnnotations {

        /**
         * The name of {@link IQueryAttributes} attribute under which the
         * subquery solution set is stored (a {@link HTree} reference). The
         * attribute name includes the query UUID. The query UUID must be
         * extracted and used to lookup the {@link IRunningQuery} to which the
         * solution set was attached.
         * 
         * @see NamedSolutionSetRef
         */
        final String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;

        /**
         * An optional {@link IVariable}[] identifying the variables to be
         * projected in the {@link IBindingSet}s written out by the operator.
         * All variables are retained unless this annotation is specified. This
         * is normally set to the <em>projection</em> of the subquery.
         * 
         * @see JoinAnnotations#SELECT
         */
        final String SELECT = JoinAnnotations.SELECT;
        
        /**
         * Boolean annotation is <code>true</code> iff the solutions will be
         * re-integrated into the query plan using an OPTIONAL join.
         */
        final String OPTIONAL = SubqueryJoinAnnotations.OPTIONAL;

        final boolean DEFAULT_OPTIONAL = SubqueryJoinAnnotations.DEFAULT_OPTIONAL;
        
    }

    /**
     * Deep copy constructor.
     */
    public HTreeHashIndexOp(final HTreeHashIndexOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HTreeHashIndexOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1) {
            /*
             * Parallel evaluation is not allowed. This operator writes on an
             * HTree and that object is not thread-safe for mutation.
             */
            throw new IllegalArgumentException(
                    PipelineOp.Annotations.MAX_PARALLEL + "="
                            + getMaxParallel());
        }

        if (!isLastPassRequested()) {
            /*
             * Last pass evaluation must be requested. This operator will not
             * produce any outputs until all source solutions have been
             * buffered.
             */
            throw new IllegalArgumentException(PipelineOp.Annotations.LAST_PASS
                    + "=" + isLastPassRequested());
        }

        getRequiredProperty(Annotations.NAMED_SET_REF);

        // Join variables must be specified.
        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

    }

    public HTreeHashIndexOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    /**
     * Return <code>true</code> iff the solutions on the hash index will be
     * re-integrated using an OPTIONAL join.
     * 
     * @see Annotations#OPTIONAL
     */
    public boolean isOptional() {
       
        return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);
        
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

    @Override
    public BOpStats newStats() {

        return new NamedSolutionSetStats();

    }

    /**
     * Adds reporting for the size of the named solution set.
     */
    private static class NamedSolutionSetStats extends BOpStats {
        
        private static final long serialVersionUID = 1L;
        
        final AtomicLong solutionSetSize = new AtomicLong();

        public void add(final BOpStats o) {

            super.add(o);

            if (o instanceof NamedSolutionSetStats) {

                final NamedSolutionSetStats t = (NamedSolutionSetStats) o;

                solutionSetSize.addAndGet(t.solutionSetSize.get());

            }

        }

        @Override
        protected void toString(final StringBuilder sb) {
            super.toString(sb);
            sb.append(",solutionSetSize=" + solutionSetSize.get());
        }

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }
    
	/**
	 * Evaluates the subquery for each source binding set. If the controller
	 * operator is interrupted, then the subqueries are cancelled. If a subquery
	 * fails, then all subqueries are cancelled.
	 */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final HTreeHashIndexOp op;
        
        private final NamedSolutionSetStats stats;
        
        /** Metadata to identify the named solution set. */
        private final NamedSolutionSetRef namedSetRef;

        /**
         * The {@link IQueryAttributes} for the {@link IRunningQuery} off which
         * we will hang the named solution set.
         */
        private final IQueryAttributes attrs;

        /**
         * The {@link IVariable}[]s to be projected.
         */
        @SuppressWarnings("rawtypes")
        private final IVariable[] selected; 
        
        /**
         * The join variables.
         */
        @SuppressWarnings("rawtypes")
        private final IVariable[] joinVars;
        
        /**
         * <code>true</code> iff this is the first time the task is being
         * invoked, in which case we allocate the {@link #solutions} map.
         */
        private final boolean first;
        
        /**
         * <code>true</code> iff the solutions will be reintegrated by an
         * OPTIONAL join. 
         */
        private final boolean optional;
        
        /**
         * The generated solution set (hash index using the specified join
         * variables).
         */
        private final HTree solutions;
        
        public ControllerTask(final HTreeHashIndexOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.op = op;
            
            this.stats = ((NamedSolutionSetStats) context.getStats());

            this.selected = (IVariable[]) op.getProperty(Annotations.SELECT);
            
            this.optional = op.isOptional();

            this.namedSetRef = (NamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            this.joinVars = (IVariable[]) op
                    .getRequiredProperty(Annotations.JOIN_VARS);
            
            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 */
                
                // Lookup the attributes for the query on which we will hang the
                // solution set.
                attrs = context.getQueryAttributes(namedSetRef.queryId);

                HTree solutions = (HTree) attrs.get(namedSetRef);

                if (solutions == null) {

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
                     * which will back the named solution set.
                     */
                    final IRawStore store = new MemStore(context
                            .getMemoryManager(namedSetRef.queryId)
                            .createAllocationContext());

                    // Will support incremental eviction and persistence.
                    solutions = HTree.create(store, metadata);

                    /*
                     * Note: This is done once the subquery has been run so we
                     * can checkpoint the HTree first and put the read-only
                     * reference on the attribute.
                     */ 
//                    if (attrs.putIfAbsent(namedSetRef, solutions) != null)
//                        throw new AssertionError();

                    this.first = true;
                 
                    if (attrs.putIfAbsent(namedSetRef, solutions) != null)
                        throw new AssertionError();

                    if (optional) {

                        /*
                         * The join set is used to handle optionals. It is saved
                         * under a named solution set reference based on the
                         * original named solution set reference, but with a
                         * slightly different named set name.
                         */
                        final NamedSolutionSetRef joinSetRef = new NamedSolutionSetRef(
                                namedSetRef.queryId, namedSetRef.namedSet
                                        + ".joinSet", joinVars);

                        HTree joinSet = HTree.create(store, metadata.clone());

                        if (attrs.putIfAbsent(joinSetRef, joinSet) != null)
                            throw new AssertionError();
                        
                    }
                    
                } else {
                    
                    this.first = false;
                    
                }

                this.solutions = solutions;

            }
            
        }

        /**
         * Evaluate.
         */
        public Void call() throws Exception {
            
            try {

                // Buffer all source solutions.
                acceptSolutions();

                if(context.isLastInvocation()) {

                    /*
                     * Note: The [HTree] object is already present on the
                     * IQueryAttributes. However, it is the mutable HTree
                     * object. We convert it to an immutable HTree object here
                     * by check pointing the HTree and updating the reference on
                     * the IQueryAttributes. That would allow the consumer of
                     * the HTree to be safe for concurrent readers.
                     * 
                     * Note: We must checkpoint the solution set before we
                     * output anything. Otherwise the chunks output by this
                     * operator could appear at the SolutionSetHashJoinOp before
                     * this operator is done and it would have the mutable view
                     * of the HTree rather than the concurrent read-only view of
                     * the HTree.
                     */
                    
                    // Checkpoint and save the solution set.
                    saveSolutionSet();
                    
                    // Output the buffered solutions.
                    outputSolutions();
                    
                }
                
                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();
                
//                if (context.getSink2() != null)
//                    context.getSink2().close();

            }
            
        }

        /**
         * Buffer intermediate resources on the {@link HTree}.
         */
        private void acceptSolutions() {

            HTreeHashJoinUtility.acceptSolutions(context.getSource(), joinVars,
                    stats, solutions, optional);

        }

        /**
         * Output the buffered solutions.
         */
        private void outputSolutions() {

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();
            
            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            // source.
            @SuppressWarnings("unchecked")
            final ITupleIterator<IBindingSet> solutionsIterator = solutions.rangeIterator();
            
            while(solutionsIterator.hasNext()) {

                final ITuple<IBindingSet> tuple = solutionsIterator.next();

                IBindingSet bset = tuple.getObject();

                if (selected != null) {

                    // Drop variables which are not projected.
                    bset = bset.copy(selected);

                }

                unsyncBuffer.add(bset);

            }

            unsyncBuffer.flush();

            sink.flush();

        }

        /**
         * Checkpoint the {@link HTree} containing the results of the subquery,
         * re-load the {@link HTree} in a read-only mode from that checkpoint
         * and then set the reference to the read-only view of the {@link HTree}
         * on the {@link IQueryAttributes}. This exposes a view of the
         * {@link HTree} which is safe for concurrent readers.
         */
        private void saveSolutionSet() {

            // Checkpoint the HTree.
            final Checkpoint checkpoint = solutions.writeCheckpoint2();

            // Get a read-only view of the HTree.
            final HTree readOnly = HTree.load(solutions.getStore(),
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            // final IQueryAttributes attrs = context.getRunningQuery()
            // .getAttributes();

            if (attrs.putIfAbsent(namedSetRef, readOnly) == null) {
                /*
                 * Note: We are expecting the attribute to already exist.
                 */
                throw new AssertionError();
            }

        }

    } // ControllerTask

}
