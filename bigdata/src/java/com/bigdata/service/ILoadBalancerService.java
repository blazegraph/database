/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Mar 2, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Interface for collecting, reporting, and decision-making based on node and
 * service utilization statistics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILoadBalancerService extends IEventReportingService {
   
    /**
     * Sent an event.
     * 
     * @param e
     *            The event.
     */
    public void notifyEvent(Event e) throws IOException;
    
    /**
     * Send performance counters. Clients SHOULD invoke this method no less than
     * once every 60 seconds.
     * 
     * @param serviceUUID
     *            The service {@link UUID} that is self-reporting.
     * @param data
     *            The serialized performance counter data.
     * 
     * @throws IOException
     */
    public void notify(UUID serviceUUID, byte[] data) throws IOException;

    /**
     * A warning issued by a client when it is in danger of depleting its
     * resources.
     * 
     * @param msg
     *            A message.
     * @param serviceUUID
     *            The service {@link UUID} that is self-reporting.
     * 
     * @throws IOException
     */
    public void warn(String msg,UUID serviceUUID) throws IOException;

    /**
     * An urgent warning issued the caller is in immediate danger of depleting
     * its resources with a consequence of immediate service and/or host
     * failure(s).
     * 
     * @param msg
     *            A message.
     * @param serviceUUID
     *            The service {@link UUID} that is self-reporting.
     * 
     * @throws IOException
     */
    public void urgent(String msg,UUID serviceUUID) throws IOException;

    /**
     * Return the {@link UUID} of an under-utilized data service. If there is no
     * under-utilized service, then return the {@link UUID} of the service with
     * the least load.
     * 
     * @throws TimeoutException
     *             if there are no data services and a timeout occurs while
     *             awaiting a service join.
     * 
     * @throws InterruptedException
     *             if the request is interrupted.
     */
    public UUID getUnderUtilizedDataService() throws IOException, TimeoutException, InterruptedException;

    /**
     * Return up to <i>limit</i> {@link IDataService} {@link UUID}s that are
     * currently under-utilized.
     * <p>
     * When <i>minCount</i> is positive, this method will always return at
     * least <i>minCount</i> service {@link UUID}s, however the {@link UUID}s
     * returned MAY contain duplicates if the {@link LoadBalancerService} has a
     * strong preference for allocating load to some services (or for NOT
     * allocating load to other services). Further, the
     * {@link LoadBalancerService} MAY choose (or be forced to choose) to return
     * {@link UUID}s for services that are within a nominal utilization range,
     * or even {@link UUID}s for services that are highly-utilized if it could
     * otherwise not satisify the request.
     * 
     * @param minCount
     *            The minimum #of services {@link UUID}s to return -or- zero
     *            (0) if there is no minimum limit.
     * @param maxCount
     *            The maximum #of services {@link UUID}s to return -or- zero
     *            (0) if there is no maximum limit.
     * @param exclude
     *            The optional {@link UUID} of a data service to be excluded
     *            from the returned set.
     * 
     * @return Up to <i>maxCount</i> under-utilized services -or-
     *         <code>null</code> IFF no services are recommended at this time
     *         as needing additional load.
     * 
     * @throws TimeoutException
     *             if there are no data services, or if there is only a single
     *             data service and it is excluded by the request, and a timeout
     *             occurs while awaiting a service join.
     * 
     * @throws InterruptedException
     *             if the request is interrupted.
     * 
     * @todo generalize to also accept the class or interface of the service so
     *       that it can be used with services other than data services, e.g.,
     *       metadata services, map/reduce services, {@link IBigdataClient}s,
     *       etc.
     * 
     * @todo probably should use {@link Integer#MAX_VALUE} rather than ZERO for
     *       the "no limit" signifier for [maxCount].
     */
    public UUID[] getUnderUtilizedDataServices(int minCount, int maxCount,
            UUID exclude) throws IOException, TimeoutException, InterruptedException;
    
    /**
     * Return <code>true</code> if the service is considered to be "highly
     * utilized".
     * <p>
     * Note: This is used mainly to decide when a service should attempt to shed
     * index partitions. This implementation SHOULD reflect the relative rank of
     * the service among all services as well as its absolute load.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @return <code>true</code> if the service is considered to be "highly
     *         utilized".
     * 
     * @throws IOException
     */
    public boolean isHighlyUtilizedDataService(UUID serviceUUID) throws IOException;
    
    /**
     * Return <code>true</code> if the service is considered to be
     * "under-utilized".
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @return <code>true</code> if the service is considered to be "under-utilized".
     * 
     * @throws IOException
     */
    public boolean isUnderUtilizedDataService(UUID serviceUUID) throws IOException;

//    /**
//     * Return the identifier(s) of under-utilized service(s).
//     * 
//     * @param minCount
//     *            The minimum #of services {@link UUID}s to return -or- zero
//     *            (0) if there is no minimum limit.
//     * @param maxCount
//     *            The maximum #of services {@link UUID}s to return -or- zero
//     *            (0) if there is no maximum limit.
//     * @param exclude
//     *            The optional {@link UUID} of a service to be excluded from the
//     *            returned set.
//     * @param iface
//     *            A class or interface that the service must implement.
//     * 
//     * @return Up to <i>limit</i> under-utilized services -or-
//     *         <code>null</code> IFF no services are recommended at this time
//     *         as needing additional load.
//     * 
//     * @todo Since {@link IMetadataService} extends {@link IDataService} we
//     *       really need a filter here that can detect the difference.
//     */
//    public UUID[] getUnderUtilizedService(int minCount, int maxCount, UUID exclude,Class iface)
//            throws IOException;
        
}
