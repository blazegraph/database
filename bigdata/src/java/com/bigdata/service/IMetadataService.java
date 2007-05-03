/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.MetadataIndex;

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
public interface IMetadataService extends IDataService {
        
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
     * @param dataService
     *            The {@link UUID} for the {@link DataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link DataService}.
     * 
     * @throws IOException
     */
    public IDataService getDataServiceByUUID(UUID dataService)
            throws IOException;

    /*
     * methods that do not require direct access to the metadata server for
     * their implementation.
     */

    /**
     * Register and statically partition a scale-out index.
     * 
     * Create and statically partition a scale-out index.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param separatorKeys
     *            The array of separator keys. Each separator key is interpreted
     *            as an <em>unsigned byte[]</em>. The first entry MUST be an
     *            empty byte[]. The entries MUST be in sorted order.
     * @param dataServiceUUIDs
     *            The array of data services onto which each partition defined
     *            by a separator key will be mapped. The #of entries in this
     *            array MUST agree with the #of entries in the <i>separatorKeys</i>
     *            array.
     * @return The UUID of the scale-out index.
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     * 
     * @todo Add a method to let a client cache the partitions of a scale-out
     *       index - its use will be limited to a statically partitioned index
     *       at this time and the #of expected partitions will be small enough
     *       that it makes sense for clients to pre-fetch and cache the entire
     *       set of partition definitions for a scale-out index.
     */
    public UUID registerManagedIndex(String name, byte[][] separatorKeys,
            UUID[] dataServices) throws IOException, InterruptedException,
            ExecutionException;
    
    /**
     * Register a scale-out index with single initial partition. As the index
     * grows, the initial partition will be split and the various partitions may
     * be re-distributed among the available {@link DataService}s.
     * 
     * @param name
     *            The index name.
     * 
     * @param dataService
     *            The identifier of the {@link DataService} that will be
     *            assigned to the initial index partition (optional). When
     *            <code>null</code>, the {@link MetadataService} will choose
     *            the initial {@link DataService} automatically.
     * 
     * @return The UUID for that index.
     */
    public UUID registerManagedIndex(String name, UUID dataService)
            throws IOException, InterruptedException, ExecutionException;
    
    /**
     * Return the unique identifier for the managed index.
     * 
     * @param name
     *            The name of the managed index.
     * 
     * @return The managed index UUID -or- <code>null</code> if there is no
     *         managed scale-out index with that name.
     *         
     * @throws IOException
     */
    public UUID getManagedIndexUUID(String name) throws IOException;

    /**
     * Find the index of the partition spanning the given key.
     * 
     * @return The index of the partition spanning the given key or
     *         <code>-1</code> iff there are no partitions defined.
     * 
     * @exception IllegalStateException
     *                if there are partitions defined but no partition spans the
     *                key. In this case the {@link MetadataIndex} lacks an entry
     *                for the key <code>new byte[]{}</code>.
     */
    public int findIndexOfPartition(String name,byte[] key) throws IOException;
    
    /**
     * Return the metadata for the index partition (and the left- and
     * right-separator keys for that index partition) in which the specified key
     * would be found.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param key
     *            A key for that scale-out index (the key may or may not exist
     *            in the index).
     * 
     * @return An byte[3][] array containing
     *         <dl>
     *         <dt>byte[0][]</dt>
     *         <dd>The left separator key for the index partition. This is the
     *         first key that would enter the index partition. The left most
     *         separator key for a partitioned index is always an empty byte[]
     *         since that is the smallest key that may be defined for any index.</dd>
     *         <dt>byte[1][]</dt>
     *         <dd>The byte[] containing the serialized
     *         {@link IPartitionMetadata} for the index partition that spans the
     *         given key.</dd>
     *         <dt>byte[2][]</dt>
     *         <dd>The right separator key for the index partition -or-
     *         <code>null</code> if there is no right sibling for the index
     *         partition (a null has the semantics of no upper bound for the
     *         index partition).</dd>
     *         </dl>
     *         If the metadata index is empty (no partitions are defined) then
     *         this method will return <code>null</code>. Note that this is
     *         NOT a normal circumstance. The metadata index should always have
     *         at least one partition once it has been registered.
     */
    public byte[][] getPartition(String name, byte[] key)
            throws IOException;
    
    /**
     * The partition at that index together with its left- and right-separator
     * keys.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param index
     *            The entry index in the metadata index.
     * 
     * @return An byte[3][] array containing
     *         <dl>
     *         <dt>byte[0][]</dt>
     *         <dd>The left separator key for the index partition. This is the
     *         first key that would enter the index partition. The left most
     *         separator key for a partitioned index is always an empty byte[]
     *         since that is the smallest key that may be defined for any index.</dd>
     *         <dt>byte[1][]</dt>
     *         <dd>The byte[] containing the serialized
     *         {@link IPartitionMetadata} for the index partition that spans the
     *         given key.</dd>
     *         <dt>byte[2][]</dt>
     *         <dd>The right separator key for the index partition -or-
     *         <code>null</code> if there is no right sibling for the index
     *         partition (a null has the semantics of no upper bound for the
     *         index partition).</dd>
     *         </dl>
     *         If the metadata index is empty (no partitions are defined) then
     *         this method will return <code>null</code>. Note that this is
     *         NOT a normal circumstance. The metadata index should always have
     *         at least one partition once it has been registered.
     * 
     * @throws IOException
     */
    public byte[][] getPartitionAtIndex(String name, int index ) throws IOException;
    
    /**
     * Create a new partition for a scale-out index.
     * 
     * @param name
     *            The index name.
     * @param key
     *            The separator key for the new partition.
     * @param dataServiceUUID
     *            The data service to which the new partition will be mapped.
     * 
     * @return The partition metadata for the new partition.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     * 
     * @todo change return signature to byte[][] or byte[] to consistent with
     *       the other methods in this interface and send back a serialized
     *       {@link PartitionMetadataWithSeparatorKeys}
     */
    public IPartitionMetadata createPartition(String name, byte[] key,
            UUID dataServiceUUID) throws IOException, InterruptedException,
            ExecutionException;
    
}
