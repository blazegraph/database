package com.bigdata.service.ndx;

import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.IDataService;
import com.bigdata.service.Split;

/**
 * Class handles stale locators by finding the current locator for the
 * <i>key</i> and redirecting the request to execute the procedure on the
 * {@link IDataService} identified by that locator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class SimpleDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

    protected final byte[] key;
    
    private int ntries = 1;

    /**
     * Always reports ONE (1).
     */
    protected int getElementCount() {
        
        return 1;
        
    }
    
    /**
     * @param key
     * @param split
     * @param proc
     * @param resultHandler
     */
    public SimpleDataServiceProcedureTask(final IScaleOutClientIndex ndx,
            final byte[] key, long ts, final Split split,
            final ISimpleIndexProcedure proc,
            final IResultHandler resultHandler) {

        super(ndx, ts, split, proc, resultHandler);
        
        if (key == null)
            throw new IllegalArgumentException("name="+ndx.getName()+", proc="+proc);
    
        this.key = key;
        
        synchronized(taskCountersByIndex) {
            taskCountersByIndex.pointRequestCount++;
        }
        
    }

    /**
     * The locator is stale. We locate the index partition that spans the
     * {@link #key} and re-submit the request.
     */
    @Override
    protected void retry() throws Exception {
        
        synchronized(taskCountersByIndex) {
            taskCountersByIndex.redirectCount++;
        }

        if (ntries++ > ndx.getFederation().getClient().getMaxStaleLocatorRetries()) {

            throw new RuntimeException("Retry count exceeded: ntries="
                    + ntries);

        }

        /*
         * Note: uses the metadata index for the timestamp against which the
         * procedure is running.
         */
        final PartitionLocator locator = ndx.getFederation()
                .getMetadataIndex(ndx.getName(), ts).find(key);

        if (log.isInfoEnabled())
            log.info("Retrying: proc=" + proc.getClass().getName()
                    + ", locator=" + locator + ", ntries=" + ntries);

        /*
         * Note: In this case we do not recursively submit to the outer
         * interface on the client since all we need to do is fetch the
         * current locator for the key and re-submit the request to the data
         * service identified by that locator.
         */

        submit(locator);
        
    }
    
}
