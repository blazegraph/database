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

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataService extends IDataService {
    
//    /**
//     * Return the partition metadata for the index partition that includes the
//     * specified key.
//     * 
//     * @param key
//     *            The key.
//     * 
//     * @return The metadata index partition in which that key is or would be
//     *         located.
//     * 
//     * @todo return lease for the index partition that would contain the key.
//     * 
//     * @todo abstract away from Jini so that we can support other fabrics
//     *       (OSGi/SCA).
//     * 
//     * @todo Either the client or the metadata service should support
//     *       pre-caching of some number of index partitions surrounding that
//     *       partition.
//     * 
//     * @todo do a variant that supports a key range - this should really just be
//     *       the same as
//     *       {@link IDataService#rangeQuery(long, String, byte[], byte[], int, int)}
//     *       with the client addressing the metadata index rather than the data
//     *       index (likewise for this method as well).
//     * 
//     * @todo update the {@link PartitionMetadata} data model to reflect a single
//     *       point of responsibility with a media replication chain for
//     *       failover. Either this method or a variant method needs to return
//     *       the partition metadata itself so that {@link DataService}s can
//     *       configure their downstream media replication pipelines.
//     */
//    public PartitionMetadata getPartition(String name, byte[] key) throws IOException;
//
    
    /*
     * methods that require access to the metadata server for their
     * implementations.
     */
    
    /**
     * Return the UUID of an under utilized data service.
     */
    public ServiceID getUnderUtilizedDataService() throws IOException;

    /**
     * Return the proxy for a {@link IDataService} from the local cache.
     * 
     * @param serviceID
     *            The {@link ServiceID} for the {@link DataService}.
     *            
     * @return The proxy or <code>null</code> if the {@link ServiceID} is not
     *         mapped to a {@link ServiceItem} for a known {@link DataService}
     *         by the local cache.
     *         
     * @throws IOException
     */
    public IDataService getDataServiceByID(ServiceID serviceID)
            throws IOException;

    /*
     * methods that do not require direct access to the metadata server for
     * their implementation.
     */

    /**
     * Register a scale-out index. The index will automatically be assigned to a
     * {@link DataService} for its initial partition. As the index grows, the
     * initial partition will be split and the various partitions may be
     * re-distributed among the available {@link DataService}s.
     * 
     * @param name
     *            The index name.
     * 
     * @return The UUID for that index.
     */
    public UUID registerIndex(String name) throws IOException,
            InterruptedException, ExecutionException;
    
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
     * Return the metadata for the index partition in which the key would be
     * found.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param key
     *            The key.
     * @return The metadata for the index partition in which that key would be
     *         found.
     *         
     * @throws IOException
     * 
     * FIXME offer a variant that reports the index partitions spanned by a key
     * range and write tests for that. Note that the remote API for that method
     * should use a result-set data model to efficiently communicate the data
     * when there are a large #of spanned partitions.
     * 
     * @see MetadataIndex#find(byte[])
     */
    public IPartitionMetadata getPartition(String name, byte[] key)
            throws IOException;
    
}
