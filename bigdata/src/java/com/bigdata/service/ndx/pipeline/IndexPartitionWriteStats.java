package com.bigdata.service.ndx.pipeline;

import com.bigdata.service.ndx.pipeline.IndexWriteTask.IndexPartitionWriteTask;

/**
 * Statistics reported for each index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IndexPartitionWriteTask
 */
public class IndexPartitionWriteStats {

    /**
     * The #of elements in the output chunks (not including any eliminated
     * duplicates).
     */
    public long elementsOut = 0L;

    /**
     * The #of chunks written onto the index partition using RMI.
     */
    public long chunksOut = 0L;

    /**
     * Elapsed nanoseconds for RMI requests.
     */
    public long elapsedNanos = 0L;

    public IndexPartitionWriteStats() {

    }

    public String toString() {

        return getClass().getName() + "{chunksOut=" + chunksOut
                + ", elementsOut=" + elementsOut + ", elapsedNanos="
                + elapsedNanos + ", averageNanos/write="
                + (elapsedNanos == 0L ? 0 : chunksOut / elapsedNanos) + "}";

    }

}
