package com.bigdata.bop.ndx;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IPredicate;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Sampling operator for a shard view.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public class SampleLocalShard<E> extends AbstractSampleIndex<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public SampleLocalShard(final IPredicate<E> pred, final int limit) {

        super(pred,limit);
        
    }

    /*
     * Note: This is done at evaluation time, local to the data. 
     */
    public Future<Void> eval(final BOpContext<E> context) {

        if (context.getPartitionId() == -1) {
            // Must be specific to a shard.
            throw new UnsupportedOperationException();
        }

        final FutureTask<Void> ft = new FutureTask<Void>(
                new LocalShardSampleTask(context));

        context.getIndexManager().getExecutorService().execute(ft);

        return ft;

    }

    /**
     * Sample an {@link AbstractBTree}.
     */
    private class LocalShardSampleTask implements Callable<Void> {

        private final BOpContext<E> context;
        private final IBlockingBuffer<E[]> sink;

        LocalShardSampleTask(final BOpContext<E> context) {

            this.context = context;
            
            this.sink = context.getSink();

        }

        public Void call() throws Exception {

            final IPredicate<E> pred = pred();
            
            final IRelation<E> view = context.getReadRelation(pred);

            final IAccessPath<E> accessPath = view.getAccessPath(pred);

            /*
             * FIXME Sample N tuples based on a uniform offset distribution,
             * discarding duplicates or tuples which are deleted in their
             * most recent revision.
             * 
             * Note: If there are only 100 leaves and we sample evenly, that
             * could result in reading all the leaves. However, when the
             * B+Tree is large we will only touch a few leaves even with
             * uniform sampling.
             */
            throw new UnsupportedOperationException();

        }

    } // class LocalShardSampleTask

}
