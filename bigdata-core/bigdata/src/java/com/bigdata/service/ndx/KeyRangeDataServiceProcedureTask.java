package com.bigdata.service.ndx;

import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.service.Split;

/**
 * Handles stale locators for {@link IKeyRangeIndexProcedure}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class KeyRangeDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

    final byte[] fromKey;
    final byte[] toKey;
  
    /**
     * Always reports ZERO (0) (these operations are generally range counts).
     */
    protected int getElementCount() {
        
        return 0;
        
    }
    
    /**
     * @param fromKey
     * @param toKey
     * @param split
     * @param proc
     * @param resultHandler
     */
    public KeyRangeDataServiceProcedureTask(final IScaleOutClientIndex ndx,
            final byte[] fromKey, final byte[] toKey, final long ts,
            final Split split, final IKeyRangeIndexProcedure proc,
            final IResultHandler resultHandler) {

        super(ndx, ts, split, proc, resultHandler);
        
        /*
         * Constrain the range to the index partition. This constraint will
         * be used if we discover that the locator data was stale in order
         * to discover the new locator(s).
         */
        
        this.fromKey = AbstractKeyRangeIndexProcedure.constrainFromKey(
                fromKey, split.pmd);

        this.toKey = AbstractKeyRangeIndexProcedure.constrainFromKey(toKey,
                split.pmd);
        
        synchronized(taskCountersByIndex) {
            taskCountersByIndex.keyRangeRequestCount++;
        }

    }

    /**
     * The {@link IKeyRangeIndexProcedure} is re-mapped for the constrained
     * key range of the stale locator using
     * {@link ClientIndexView#submit(byte[], byte[], IKeyRangeIndexProcedure, IResultHandler)}.
     */
    @Override
    protected void retry() throws Exception {

        synchronized(taskCountersByIndex) {
            taskCountersByIndex.redirectCount++;
        }

        /*
         * Note: recursive retries MUST run in the same thread in order to
         * avoid deadlock of the client's thread pool. The recursive depth
         * is used to enforce this constrain.
         */

        final AtomicInteger recursionDepth = ndx.getRecursionDepth();
        
        final int depth = recursionDepth.incrementAndGet();

        try {
        
            if (depth > ndx.getFederation().getClient().getMaxStaleLocatorRetries()) {

                throw new RuntimeException("Retry count exceeded: ntries="
                        + depth);
                
            }

            /*
             * Note: This MUST use the timestamp already assigned for this
             * operation but MUST compute new splits against the updated
             * locators.
             */
            ((ClientIndexView) ndx).submit(ts, fromKey, toKey,
                    (IKeyRangeIndexProcedure) proc, resultHandler);

        } finally {

            final int tmp = recursionDepth.decrementAndGet();

            assert tmp >= 0 : "depth=" + depth + ", tmp=" + tmp;

        }
        
    }
    
}
