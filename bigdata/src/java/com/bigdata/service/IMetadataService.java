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
     * 
     * @todo convert to return a UUID to kind Jini isolated from the core impl.
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
     * 
     * @todo convert serviceID to a UUID to keep Jini encapsulated.
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
     * 
     * @return The serialized {@link IPartitionMetadata} spanning the given key
     *         or <code>null</code> if there are no partitions defined.
     * 
     * @throws IOException
     * 
     * @see MetadataIndex#find(byte[])
     */
    public byte[] getPartition(String name, byte[] key)
            throws IOException;
    
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
     * The partition at that index.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param index
     *            The entry index in the metadata index.
     * 
     * @return The serialized {@link IPartitionMetadata} for that the entry with
     *         that index.
     * 
     * @throws IOException
     * 
     * @todo this is subject to concurrent modification of the metadata index
     *       would can cause the index to identify a different partition. client
     *       requests that use {@link #findIndexOfPartition(String, byte[])} and
     *       {@link #getPartitionAtIndex(String, int)} really need to refer to
     *       the same historical version of the metadata index (this effects
     *       range count and range iterator requests and to some extent batch
     *       operations that span multiple index partitions).
     */
    public byte[] getPartitionAtIndex(String name, int index ) throws IOException;
    
}
