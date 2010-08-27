package com.bigdata.bop.engine;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import junit.framework.AssertionFailedError;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;

/**
 * Operator block evaluation for the specified {@link Annotations#DELAY} and
 * then throws an {@link AssertionFailedError}.
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

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        final FutureTask<Void> ft = new FutureTask<Void>(new Callable<Void>() {
            public Void call() throws Exception {
                Thread.sleep(delayMillis());
                throw new AssertionFailedError();
            }
        });
        
        return ft;

    }

}
