package com.bigdata.bop.solutions;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.ISingleThreadedOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.HTreeHashJoinUtility;
import com.bigdata.bop.join.JoinTypeEnum;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A pipelined DISTINCT operator based on the persistence capable {@link HTree}
 * suitable for very large solution sets. Only the variables which are used to
 * determine the DISTINCT solutions are projected from the operator. The
 * operator is specific to the RDF data model (it relies on encoded {@link IV}
 * s).
 * <p>
 * Note: This implementation is a single-threaded pipelined operator which
 * inspects each chunk of solutions as they arrive and those solutions which are
 * distinct for each chunk passed on.
 * <p>
 * Note: {@link PipelineOp.Annotations#MAX_MEMORY} is currently ignored by this
 * operator. This value could be used to trigger the switch to an external
 * memory DISTINCT (on a backing store) or to fail a query which attempts to put
 * too much data into the native heap. Right now, it will just keep adding data
 * on the native heap and eventually the machine will begin to swap.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeDistinctBindingSetsOp extends PipelineOp implements
        ISingleThreadedOp {

//	private final static transient Logger log = Logger
//			.getLogger(DistinctBindingSetsWithHTreeOp.class);
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	public interface Annotations extends PipelineOp.Annotations,
			HTreeAnnotations, DistinctAnnotations, NamedSetAnnotations {

//        /**
//         * The name of {@link IQueryAttributes} attribute under which the
//         * {@link HTreeHashJoinState} for this operator is stored. The attribute
//         * name includes the query UUID. The query UUID must be extracted and
//         * used to lookup the {@link IRunningQuery} to which the solution set
//         * was attached.
//         * 
//         * @see NamedSolutionSetRef
//         */
//        final String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;

	}

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public HTreeDistinctBindingSetsOp(final HTreeDistinctBindingSetsOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public HTreeDistinctBindingSetsOp(final BOp[] args,
            final Map<String, Object> annotations) {

		super(args, annotations);

		switch (getEvaluationContext()) {
        case CONTROLLER:
        case HASHED:
            break;
		default:
			throw new UnsupportedOperationException(
					Annotations.EVALUATION_CONTEXT + "="
							+ getEvaluationContext());
		}

		assertMaxParallelOne();

//		// shared state is used to share the hash table.
//		if (!isSharedState()) {
//			throw new UnsupportedOperationException(Annotations.SHARED_STATE
//					+ "=" + isSharedState());
//		}

        final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) getRequiredProperty(Annotations.NAMED_SET_REF);

		final IVariable<?>[] vars = (IVariable[]) getProperty(Annotations.VARIABLES);

		if (vars == null || vars.length == 0)
			throw new IllegalArgumentException();

    }

    public HTreeDistinctBindingSetsOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new DistinctTask(this, context));
        
    }

    /**
     * Task executing on the node.
     */
    static private class DistinctTask implements Callable<Void> {

        private final HTreeDistinctBindingSetsOp op;
        
        private final BOpContext<IBindingSet> context;

        private final HTreeHashJoinUtility state;
        
        DistinctTask(final HTreeDistinctBindingSetsOp op,
                final BOpContext<IBindingSet> context) {
            
            this.op = op;
            
            this.context = context;

            /** Metadata to identify the named solution set. */
            final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            /*
             * First, see if the map already exists.
             * 
             * Note: Since the operator is not thread-safe, we do not need to
             * use a putIfAbsent pattern here.
             */

            /*
			 * Lookup the attributes for the query on which we will hang the
			 * solution set. See BLZG-1493 (if queryId is null, use the query
			 * attributes for this running query).
			 */
			final IQueryAttributes attrs = context.getQueryAttributes(namedSetRef.getQueryId());

            HTreeHashJoinUtility state = (HTreeHashJoinUtility) attrs
                    .get(namedSetRef);

            if (state == null) {

                state = new HTreeHashJoinUtility(
                        context.getMemoryManager(namedSetRef.getQueryId()), op,
                        JoinTypeEnum.Filter);

                if (attrs.putIfAbsent(namedSetRef, state) != null)
                    throw new AssertionError();

            }

            this.state = state;
            
        }
        @Override
        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final ICloseableIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                        op.getChunkCapacity(), sink);

                state.filterSolutions(itr, stats, unsyncBuffer);

                unsyncBuffer.flush();
                
                sink.flush();
                
                // done.
                return null;
                
            } finally {

                if (context.isLastInvocation()) {

                    state.release();

                }

                sink.close();

            }

        }

    } // class DistinctTask

}
