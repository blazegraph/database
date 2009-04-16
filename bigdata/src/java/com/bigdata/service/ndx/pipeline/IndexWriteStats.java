package com.bigdata.service.ndx.pipeline;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.resources.StaleLocatorException;

/**
 * Statistics for the consumer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexWriteStats<L, HS extends IndexPartitionWriteStats> extends
        AbstractMasterStats<L, HS> {

    /**
     * The #of duplicates which were filtered out.
     */
    public long duplicateCount = 0L;
    
    /**
     * Return a {@link CounterSet} which may be used to report the statistics on
     * the index write operation. The {@link CounterSet} is NOT placed into any
     * namespace.
     */
    @Override
    public CounterSet getCounterSet() {
        
        final CounterSet t = super.getCounterSet();
        
        t.addCounter("duplicateCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(duplicateCount);
            }
        });

        return t;

    }

    @Override
    public String toString() {

        return super.toString() + "{duplicateCount=" + duplicateCount + "}";

    }

    @SuppressWarnings("unchecked")
    @Override
    protected HS newSubtaskStats(final L locator) {

        return (HS) new IndexPartitionWriteStats();
        
    }

}
