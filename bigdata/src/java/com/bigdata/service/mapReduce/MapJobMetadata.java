package com.bigdata.service.mapReduce;

import java.util.UUID;

/**
 * Metadata for the map operation of a map/reduce job.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MapJobMetadata implements IJobMetadata {
    
    /**
     * 
     */
    private static final long serialVersionUID = -2830596078975222225L;
    
    /**
     * The job identifier.
     */
    private UUID job;
    /**
     * The task identifier for each reduce task.
     */
    private UUID[] reduceTasks;
    /**
     * The data service identifier for each reduce partition (one per reduce
     * task).
     */
    private UUID[] dataServices;
    
    public MapJobMetadata(UUID job, UUID[] reduceTasks, UUID[] dataServices) {
        
        this.job = job;
        
        this.reduceTasks = reduceTasks;
        
        this.dataServices = dataServices;
        
    }

    public UUID getUUID() {

        return job;
        
    }
    
    /**
     * The task identifier for each reduce task.
     */
    public UUID[] getReduceTasks() {
        
        return reduceTasks;
        
    }

    /**
     * The data service identifier for each reduce partition (one per reduce
     * task).
     */
    public UUID[] getDataServices() {
        
        return dataServices;
        
    }
    
}