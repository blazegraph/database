package com.bigdata.service.mapReduce;

import java.util.UUID;

/**
 * Metadata for the reduce operation of a map/reduce job.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReduceJobMetadata implements IJobMetadata {

    /**
     * 
     */
    private static final long serialVersionUID = 529948641278190744L;
    private UUID job;

    public ReduceJobMetadata(UUID job) {
        
        this.job = job;
        
    }
    
    public UUID getUUID() {

        return job;
        
    }
    
}