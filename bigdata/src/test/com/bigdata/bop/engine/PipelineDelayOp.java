package com.bigdata.bop.engine;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;

/**
 * This operator is used to feed the first join in the pipeline. The operator
 * should have no children but may be decorated with annotations as necessary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PipelineDelayOp extends AbstractPipelineOp<IBindingSet> implements
        BindingSetPipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AbstractPipelineOp.Annotations {

        /**
         * The delay imposed by the operator (milliseconds).
         */
        String DELAY = PipelineDelayOp.class.getName() + ".delay";

        long DEFAULT_DELAY = 2000;
        
    }
    
    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    public PipelineDelayOp(PipelineDelayOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public PipelineDelayOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Return the delay.
     */
    public long delayMillis() {
        final Long delay = (Long) annotations.get(Annotations.DELAY);
        if (delay == null)
            return Annotations.DEFAULT_DELAY;
        return delay.longValue();
    }

    /**
     * Blocks for the specified {@link Annotations#DELAY}. 
     */
    public Future<Void> eval(final BOpContext<IBindingSet> context) {

        final FutureTask<Void> ft = new FutureTask<Void>(new Callable<Void>() {
            public Void call() throws Exception {
                Thread.sleep(delayMillis());
                TestQueryEngine.fail();
                return null;
            }
        });
        
        context.getIndexManager().getExecutorService().execute(ft);
        
        return ft;

    }

}