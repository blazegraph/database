package com.bigdata.service.mapReduce;

import java.util.UUID;


/**
 * Abstract base class for reduce tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractReduceTask implements IReduceTask {

    /**
     * The task identifier.
     */
    final public UUID uuid;
    
    /**
     * The data service identifier. The task will read its data from the
     * index on the data service that is named by the task identifier.
     */
    final private UUID dataService;
    
    /**
     * 
     * @param uuid
     *            The task identifier.
     * @param dataService
     *            The data service identifier. The task will read its data
     *            from the index on the data service that is named by the
     *            task identifier.
     */
    protected AbstractReduceTask(UUID uuid, UUID dataService) {
        
        if(uuid==null) throw new IllegalArgumentException();
        
        if(dataService==null) throw new IllegalArgumentException();
        
        this.uuid = uuid;
        
        this.dataService = dataService;
        
    }
    
    public UUID getUUID() {
        
        return uuid;
        
    }

    public UUID getDataService() {
        
        return dataService;
        
    }
    
}