/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;

/**
 * A metadata service for a named index.
 * <p>
 * The metadata service maintains locator information for the data service
 * instances responsible for each partition in the named index. Partitions are
 * automatically split when they overflow (~200M) and joined when they underflow
 * (~50M).
 * <p>
 * Note: methods on this interface MUST throw {@link IOException} in order to be
 * compatible with RMI.
 * 
 * @todo consider adding the timestamp of commit record for the data that was
 *       read such that a client can effect consistent read-only view simply by
 *       providing that timestamp to the next method call. This is especially
 *       relevant for the {@link IMetadataService} interface since clients need
 *       to make multiple requests for rangeCount and rangeIterator and the use
 *       of the same read-timestamp will make those requests consistent.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataService extends IDataService, Remote {
        
    /*
     * methods that require access to the metadata server for their
     * implementations.
     * 
     * @todo the tx identifier will have to be pass in for clients that want to
     * use transactional isolation to achieve a consistent and stable view of
     * the metadata index as of the start time of their transaction.
     */
    
    /**
     * Return the identifier of an under utilized data service.
     */
    public UUID getUnderUtilizedDataService() throws IOException;

    /**
     * Return the proxy for a {@link IDataService} from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link DataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link DataService}.
     * 
     * @throws IOException
     */
    public IDataService getDataServiceByUUID(UUID serviceUUID)
            throws IOException;

    /**
     * Register and statically partition a scale-out index.
     * 
     * @param metadata
     *            The metadata template describing the scale-out index,
     *            including the name to be assigned to that index.
     * @param separatorKeys
     *            The array of separator keys. Each separator key is interpreted
     *            as an <em>unsigned byte[]</em>. The first entry MUST be an
     *            empty byte[]. The entries MUST be in sorted order.
     * @param dataServiceUUIDs
     *            The array of data services onto which each partition defined
     *            by a separator key will be mapped (optional). The #of entries
     *            in this array MUST agree with the #of entries in the
     *            <i>separatorKeys</i> array. When <code>null</code>, the
     *            index paritions will be auto-assigned to data services.
     * 
     * @return The UUID of the scale-out index.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public UUID registerScaleOutIndex(IndexMetadata metadata,
            byte[][] separatorKeys, UUID[] dataServices) throws IOException,
            InterruptedException, ExecutionException;
    
    /**
     * Drop the named scale-out index.
     * 
     * @param name
     *            The name of the scale-out index.
     */
    public void dropScaleOutIndex(String name) throws IOException,
            InterruptedException, ExecutionException;
    
}
