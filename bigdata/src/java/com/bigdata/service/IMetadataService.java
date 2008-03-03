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
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataService extends IDataService, Remote {
        
    /*
     * methods that require access to the metadata server for their
     * implementations.
     */
    
    /**
     * Return the identifier of an under utilized data service.
     * 
     * @todo probably will be moved to {@link IStatisticsService} 
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
    public IDataService getDataService(UUID serviceUUID)
            throws IOException;

    /**
     * Return the next unique partition identifier to be assigned to the named
     * scale-out index.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The next partition identifier.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public int nextPartitionId(String name) throws IOException,
            InterruptedException, ExecutionException;

    /**
     * Updates the {@link MetadataIndex} for the named scale-out index to
     * reflect the split of an index partition into N new index partitions. The
     * old index partition locator is removed from the {@link MetadataIndex} and
     * the new index partition locators are inserted in a single atomic
     * operation.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param oldLocator
     *            The partition locator that is being split.
     * @param newLocators
     *            The locator information for the new index partitions that were
     *            created by the split of the old index partition.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void splitIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator[] newLocators) throws IOException,
            InterruptedException, ExecutionException;
    /**
     * Updates the {@link MetadataIndex} for the named scale-out index to
     * reflect the join of N index partitions (which must be siblings) into a
     * single index partition. The old index partition locators are removed from
     * the {@link MetadataIndex} and the new index partition locator is inserted
     * in a single atomic operation.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param oldLocators
     *            The partition locators for the index partitions that were
     *            joined.
     * @param newLocator
     *            The locator for the new index partition created by that join.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void joinIndexPartition(String name, PartitionLocator[] oldLocators,
            PartitionLocator newLocator) throws IOException,
            InterruptedException, ExecutionException;

    /**
     * Updates the {@link MetadataIndex} for the named scale-out index to
     * reflect the move of an index partition from one data service to another.
     * The old index partition locator is removed from the {@link MetadataIndex}
     * and the new index partition locator is inserted in a single atomic
     * operation.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param oldLocator
     *            The partition locator for the source index partition.
     * @param newLocator
     *            The locator for the target index partition.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void moveIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator newLocator) throws IOException,
            InterruptedException, ExecutionException;
    
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
    
    /**
     * The partition with that separator key or <code>null</code> (exact match
     * on the separator key).
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The partition with that separator key or <code>null</code>.
     */
    public PartitionLocator get(String name, long timestamp, byte[] key)
            throws InterruptedException, ExecutionException, IOException;

    /**
     * Find and return the partition spanning the given key.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     * @param key
     *            A key of interest for the scale-out index.
     * 
     * @return The partition spanning the given key or <code>null</code> if
     *         there are no partitions defined.
     */
    public PartitionLocator find(String name, long timestamp, byte[] key)
            throws InterruptedException, ExecutionException, IOException;

}
