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

package com.bigdata.bop.controller;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.HashJoinUtility;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.sparql.ast.BigdataServiceCall;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.WrappedAsynchronousIterator;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.striterator.ChunkedWrappedIterator;

/**
 * Evaluation of a service call, producing a named result set. This operator passes
 * through any source binding sets without modification. The service is
 * evaluated exactly once, the first time this operator is invoked for a given
 * query plan. No bindings are pushed into the service. If some variables are
 * known to be bound, then they should be rewritten into constants or their
 * bindings should be inserted into the service using LET() operator.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization for the "run-once" contract of the service. The operator
 * MUST be run on the query controller.
 *
 * @see NamedSubqueryIncludeOp
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ServiceOp extends PipelineOp {

    static private final transient Logger log = Logger
            .getLogger(ServiceOp.class);

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends  HTreeAnnotations,
            HashJoinAnnotations {

        /**
         * The name of {@link IQueryAttributes} attribute under which the
         * subquery solution set is stored (a {@link HTree} reference). The
         * attribute name includes the query UUID. The query UUID must be
         * extracted and used to lookup the {@link IRunningQuery} to which the
         * solution set was attached.
         *
         * @see NamedSolutionSetRef
         */
        final String NAMED_SET_REF = "namedSetRef";

        final String SERVICE_CALL  = "serviceCall";

    }


    /**
     * Deep copy constructor.
     */
    public ServiceOp(final ServiceOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     *
     * @param args
     * @param annotations
     */
    public ServiceOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1) {
            throw new IllegalArgumentException(
                    PipelineOp.Annotations.MAX_PARALLEL + "="
                            + getMaxParallel());
        }

        getRequiredProperty(Annotations.SERVICE_CALL);

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

    public ServiceOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

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

        private final NamedSolutionSetStats stats;

        /** The subquery which is evaluated for each input binding set. */
        private final BigdataServiceCall serviceCall;

        /** Metadata to identify the named solution set. */
        private final NamedSolutionSetRef namedSetRef;

        /**
         * The join variables.
         */
        private final IVariable[] joinVars;

        /**
         * <code>true</code> iff this is the first time the task is being
         * invoked, in which case we will evaluate the subquery and save its
         * result set on {@link #solutions}.
         */
        private final boolean first;

        /**
         * The generated solution set (hash index using the specified join
         * variables).
         */
        private final HTree solutions;

        public ControllerTask(final ServiceOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.stats = ((NamedSolutionSetStats) context.getStats());

            this.serviceCall = (BigdataServiceCall) op
                    .getRequiredProperty(Annotations.SERVICE_CALL);

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
                final IQueryAttributes attrs = context.getRunningQuery()
                        .getAttributes();

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
                     * which is backing the query.
                     */
                    final IRawStore store = new MemStore(context
                            .getRunningQuery().getMemoryManager()
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

                if(first) {

                    // Generate the result set and write it on the HTree.
                    new ServiceTask(new ListBindingSet(), serviceCall, context)
                            .call();

                }

                // source.
                final IAsynchronousIterator<IBindingSet[]> source = context
                        .getSource();

                // default sink
                final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

                BOpUtility.copy(//
                        source, //
                        sink,//
                        null, // sink2
                        null, // mergeSolution (aka parent's source solution).
                        null, // selectVars (aka projection).
                        null, // constraints
                        context.getStats()//
                        );

                sink.flush();

                // Done.
                return null;

            } finally {

                context.getSource().close();

                context.getSink().close();

                if (context.getSink2() != null)
                    context.getSink2().close();

            }

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

            final IQueryAttributes attrs = context.getRunningQuery()
                    .getAttributes();

            if (attrs.putIfAbsent(namedSetRef, readOnly) != null)
                throw new AssertionError();

        }

        /**
         * Run a subquery.
         */
        private class ServiceTask implements Callable<Void> {

            /**
             * The evaluation context for the parent query.
             */
            private final BOpContext<IBindingSet> parentContext;

            /**
             * The source binding set.
             */
            private final IBindingSet bset;

            /**
             * The root operator for the subquery.
             */
            private final BigdataServiceCall serviceCall;

            public ServiceTask(final IBindingSet bset, final BigdataServiceCall serviceCall,
                    final BOpContext<IBindingSet> parentContext) {

                this.bset = bset;

                this.serviceCall = serviceCall;

                this.parentContext = parentContext;

            }

            public Void call() throws Exception {

            	// The iterator draining the subquery
                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    /*
                     * FIXME The BindingsClause needs to be on the QueryRoot. It
                     * should be translated to IVs when we parse the SPARQL
                     * query. If the service invocation is an external service
                     * then we need to materialize the IVs (or cache them up
                     * front) and send them along and, in addition, bulk resolve
                     * the visited BindingSets using the
                     * BigdataOpenRDFBindingSetsResolverator (there might be a
                     * BigdataServiceCall class which handles this and delegates
                     * through to an HTTP request against a remote Service URI).
                     * Otherwise we should pass in the IBindingSet[].
                     */
                    // Iterator visiting the subquery solutions.
                    subquerySolutionItr = new WrappedAsynchronousIterator<IBindingSet[], IBindingSet>(
                            new ChunkedWrappedIterator<IBindingSet>(
                                    serviceCall.call(null/* BindingsClause */)));

						// Buffer the solutions on the hash index.
                        final long ncopied = HashJoinUtility.acceptSolutions(
                                subquerySolutionItr, joinVars, stats,
                                solutions, false/* optional */);


                        // Report the #of solutions in the named solution set.
                        stats.solutionSetSize.addAndGet(ncopied);

                        // Publish the solution set on the query context.
                        saveSolutionSet();

                        if (log.isInfoEnabled())
                            log.info("Solution set " + namedSetRef + " has "
                                    + ncopied + " solutions.");


                } catch (Throwable t) {


                } finally {
						// ensure the subquery solution iterator is closed.
						if (subquerySolutionItr != null)
							subquerySolutionItr.close();
                }

                // Done.
                return null;

            }

        } // SubqueryTask

    } // ControllerTask

}
