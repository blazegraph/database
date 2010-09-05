package com.bigdata.bop.aggregation;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * An in-memory merge sort for binding sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 * 
 * @todo unit tests.
 * @todo do an external merge sort operator.
 */
public class MemorySortOp extends SortOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public MemorySortOp(final MemorySortOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public MemorySortOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new SortTask(this, context));
        
    }

    /**
     * Task executing on the node.
     */
    static private class SortTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The binding set comparator.
         */
        private final Comparator<IBindingSet> comparator;

        SortTask(final MemorySortOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.comparator = op.getComparator();

        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final IBindingSet[] all = BOpUtility.toArray(context
                        .getSource(), stats);

                // sort.
                Arrays.sort(all, comparator);

                // update counters.
                stats.unitsOut.add(all.length);
                stats.chunksOut.increment();

                // write output and flush.
                sink.add(all);
                sink.flush();

                // done.
                return null;

            } finally {

                sink.close();

            }

        }

    }

}
