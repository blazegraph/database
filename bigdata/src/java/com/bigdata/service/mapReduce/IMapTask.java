package com.bigdata.service.mapReduce;

import java.util.UUID;

/**
 * Interface for a map task to be executed on a map/reduce client.
 * <p>
 * Each map task will be presented with a series of records drawn from the
 * input sources for the task. When the source is an index, the key-value
 * pairs will be presented in key order. The map operator is responsible for
 * writting zero or more key value pairs on the output sink. Those key value
 * pairs will be assigned to N different reduce tasks by applying the
 * user-defined hash function to the output key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMapTask {
    
    /**
     * The unique identifier for the map task. If a map task is retried then
     * the new instance of that task will have the <em>same</em>
     * identifier.
     */
    public UUID getUUID();
    
}
