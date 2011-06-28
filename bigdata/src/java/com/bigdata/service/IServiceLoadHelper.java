package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Interface for decision making about the load imposed on services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IServiceLoadHelper {

    /**
     * Return an array of under-utilized {@link IDataService} {@link UUID}s.
     * 
     * @param minCount
     * @param maxCount
     * @param exclude
     * 
     * @return
     * 
     * @throws InterruptedException
     * @throws TimeoutException
     * 
     * @see ILoadBalancerService#getUnderUtilizedDataServices(int, int,
     *      UUID)
     */
    public UUID[] getUnderUtilizedDataServices(int minCount, int maxCount,
            UUID exclude) throws InterruptedException, TimeoutException;
    
}