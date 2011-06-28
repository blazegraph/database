package com.bigdata.bop.engine;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import junit.framework.AssertionFailedError;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.IBindingSet;

/**
 * Operator block evaluation for the specified {@link Annotations#DELAY} and
 * then throws an {@link PipelineDelayError}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PipelineDelayOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The delay imposed by the operator (milliseconds).
         */
        String DELAY = (PipelineDelayOp.class.getName() + ".delay").intern();

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

        return (Long) getRequiredProperty(Annotations.DELAY);

    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new DelayTask(this, context));

    }

    private static class DelayTask implements Callable<Void> {

        private final long delay;

        public DelayTask(final PipelineDelayOp op,
                final BOpContext<IBindingSet> context) {

            this.delay = op.delayMillis();

        }

        public Void call() throws Exception {

            Thread.sleep(delay);

            throw new PipelineDelayError();

        }

    }

    static class PipelineDelayError extends AssertionFailedError {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

    }

}
