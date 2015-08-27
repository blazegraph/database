package com.bigdata.bop.bset;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A operator which may be used at the end of query pipelines when there is a
 * requirement to marshal solutions back to the query controller but no
 * requirement to {@link SliceOp slice} solutions. The primary use case for
 * {@link EndOp} is on a cluster, where it is evaluated on the query controller
 * so the results will be streamed back to the query controller from the nodes
 * of the cluster. You MUST specify {@link BOp.Annotations#EVALUATION_CONTEXT}
 * as {@link BOpEvaluationContext#CONTROLLER} when it is to be used for this
 * purpose.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/227
 */
public class EndOp extends PipelineOp {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public EndOp(EndOp op) {
		super(op);
	}

	public EndOp(BOp[] args, Map<String, Object> annotations) {

	    super(args, annotations);
        
	    switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

	}

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new OpTask(/*this, */context));
        
    }

    /**
     * Copy the source to the sink or the alternative sink depending on the
     * condition.
     */
    static private class OpTask implements Callable<Void> {

//        private final PipelineOp op;
        
        private final BOpContext<IBindingSet> context;
        
        OpTask(final BOpContext<IBindingSet> context) {

            this.context = context;
            
        }

        public Void call() throws Exception {

            final ICloseableIterator<IBindingSet[]> source = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                boolean didRun = false;

                while (source.hasNext()) {

                    final IBindingSet[] chunk = source.next();

                    sink.add(chunk);

                    didRun = true;

                }

                if (didRun)
                    sink.flush();

                return null;

            } finally {

                sink.close();
                
            }

        }

    }

}
