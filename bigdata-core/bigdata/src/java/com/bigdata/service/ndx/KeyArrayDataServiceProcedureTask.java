package com.bigdata.service.ndx;

import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.service.Split;

/**
 * Handles stale locators for {@link IKeyArrayIndexProcedure}s. When
 * necessary the procedure will be re-split.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class KeyArrayDataServiceProcedureTask extends
        AbstractDataServiceProcedureTask {

    /**
     * The keys. Only the elements in this array indexed by
     * {@link AbstractDataServiceProcedureTask#split} will be operated on by
     * this instance of the procedure.
     * <p>
     * Note: On the client, {@link #keys} may be partitioned across many
     * {@link Split}s but is always dense on the server. This conversion to a
     * dense array is achieved by the {@link AbstractKeyArrayIndexProcedure}
     * when it serializes its keys.
     */
    protected final byte[][] keys;

    /**
     * The values and <code>null</code> if the operation does not take values
     * as an input. Only the elements in this array indexed by
     * {@link AbstractDataServiceProcedureTask#split} will be operated on by
     * this instance of the procedure.
     * <p>
     * Note: On the client, {@link #vals} may be partitioned across many
     * {@link Split}s but is always dense on the server. This conversion to a
     * dense array is achieved by the {@link AbstractKeyArrayIndexProcedure}
     * when it serializes its values.
     */
    protected final byte[][] vals;
    
    /**
     * The object that knows how to create an instance of the procedure to be
     * applied to the keys and values when they are de-serialized on the server.
     */
    protected final AbstractKeyArrayIndexProcedureConstructor ctor;
    
    /**
     * Reports the #of keys that are used by the {@link Split} tasked to this
     * instance of the procedure to be executed.
     */
    protected int getElementCount() {
        
        return split.ntuples;
//        return keys.length;
        
    }
    
    /**
     * Variant used for {@link IKeyArrayIndexProcedure}s.
     * 
     * @param keys
     *            The original keys[][].
     * @param vals
     *            The original vals[][].
     * @param split
     *            The split identifies the subset of keys and values to be
     *            applied by this procedure.
     * @param proc
     *            The procedure instance.
     * @param resultHandler
     *            The result aggregator.
     * @param ctor
     *            The object used to create instances of the <i>proc</i>.
     *            This is used to re-split the data if necessary in response
     *            to stale locator information.
     */
    public KeyArrayDataServiceProcedureTask(final IScaleOutClientIndex ndx,
            final byte[][] keys, final byte[][] vals, final long ts,
            final Split split, final IKeyArrayIndexProcedure proc,
            final IResultHandler resultHandler,
            final AbstractKeyArrayIndexProcedureConstructor ctor) {
        
        super( ndx, ts, split, proc, resultHandler );
        
        if (ctor == null)
            throw new IllegalArgumentException();

        this.ctor = ctor;
        
        this.keys = keys;
        
        this.vals = vals;

        synchronized(taskCountersByIndex) {
            taskCountersByIndex.keyArrayRequestCount++;
        }
        
    }

    /**
     * Submit using
     * {@link ClientIndexView#submit(int, int, byte[][], byte[][], AbstractKeyArrayIndexProcedureConstructor, IResultHandler)}.
     * This will recompute the split points and re-map the procedure across
     * the newly determined split points.
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
        
            if (depth > ndx.getFederation().getClient()
                    .getMaxStaleLocatorRetries()) {

                throw new RuntimeException("Retry count exceeded: ntries="
                        + depth);
                
            }
            
            /*
             * Note: This MUST use the timestamp already assigned for this
             * operation but MUST compute new splits against the updated
             * locators.
             */
            ((ClientIndexView)ndx).submit(ts, split.fromIndex, split.toIndex,
                    keys, vals, ctor, resultHandler);
            
        } finally {
            
            final int tmp = recursionDepth.decrementAndGet();
            
            assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
            
        }
        
    }
    
}