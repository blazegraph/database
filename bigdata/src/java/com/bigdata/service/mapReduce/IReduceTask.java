package com.bigdata.service.mapReduce;

import java.util.Iterator;
import java.util.UUID;

import com.bigdata.service.IDataService;

/**
 * Interface for a reduce task to be executed on a map/reduce client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IReduceTask {

    /**
     * The unique identifier for this reduce task. If a reduce task is
     * retried then the new instance of the task will have the <em>same</em>
     * identifier.
     */
    public UUID getUUID();
    
    /**
     * The {@link IDataService}  from which the reduce task will read its
     * input.
     */
    public UUID getDataService();
    
    /**
     * Each reduce task will be presented with a series of key-value pairs
     * in key order. However, the keys will be distributed across the N
     * reduce tasks by the used defined hash function, so this is NOT a
     * total ordering over the intermediate keys.
     * <p>
     * Note: This method is never invoked for a key for which there are no
     * values.
     * 
     * @param key
     *            A key.
     * @param vals
     *            An iterator that will visit the set of values for that
     *            key.
     */
    public void reduce(byte[] key, Iterator<byte[]> vals) throws Exception;
    
}