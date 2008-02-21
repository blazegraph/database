/*

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
package com.bigdata.service;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex;

/**
 * The client-facing interface to a bigdata federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo reconcile with {@link IIndexManager} and {@link IIndexStore}.
 */
public interface IBigdataFederation {

    public static final Logger log = Logger.getLogger(IBigdataFederation.class);

    /**
     * Return the metadata service (or a proxy for the metadata service).
     */
    public IMetadataService getMetadataService();
    
    /**
     * Return a read-only view of the index partitions for the named scale-out
     * index.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The {@link IMetadataIndex} for the named scale-out index -or-
     *         <code>null</code> iff there is no such scale-out index.
     */
    public IMetadataIndex getMetadataIndex(String name);
    
    /**
     * Register a scale-out index.
     * 
     * @param metadata
     *            The metadata template used to create component indices for
     *            {@link BTree}s this scale-out index (this also specifies the
     *            name of the scale-out index).
     * 
     * @return The UUID for the scale-out index.
     * 
     * @todo Since the index UUID is declared by the provided metadata object it
     *       does not need to be returned here. However I am not sure what would
     *       be a better return value.  We can't return an {@link IIndex} since
     *       we do not have the timestamp to fully qualify the view.
     */
    public UUID registerIndex(IndexMetadata metadata);
    
    /**
     * Register a scale-out index and assign the initial index partition to the
     * specified data service.
     * 
     * @param metadata
     *            The metadata template used to create component indices for
     *            {@link BTree}s this scale-out index (this also specifies the
     *            name of the scale-out index).
     * @param dataServiceUUID
     *            The data service identifier (optional). When <code>null</code>,
     *            a data service will be selected automatically.
     * 
     * @return The UUID of the registered index.
     */
    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID);
    
    /**
     * Register and statically partition a scale-out index.
     * 
     * @param metadata
     *            The metadata template used to create component indices for
     *            {@link BTree}s this scale-out index (this also specifies the
     *            name of the scale-out index).
     * @param separatorKeys
     *            The array of separator keys. Each separator key is interpreted
     *            as an <em>unsigned byte[]</em>. The first entry MUST be an
     *            empty byte[]. The entries MUST be in sorted order.
     * @param dataServiceUUIDs
     *            The array of data services onto which each partition defined
     *            by a separator key will be mapped. The #of entries in this
     *            array MUST agree with the #of entries in the <i>separatorKeys</i>
     *            array.
     * 
     * @return The UUID of the scale-out index.
     */
    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs);

    /**
     * Drop a scale-out index.
     * 
     * @param name
     *            The index name.
     */
    public void dropIndex(String name);
    
    /**
     * Obtain a view on a partitioned index.
     * 
     * @param tx
     *            The transaction identifier or zero(0L) iff the index will
     *            be unisolated.
     * 
     * @param name
     *            The index name.
     * 
     * @return The index or <code>null</code> if the index is not
     *         registered with the {@link MetadataService}.
     */
    public IIndex getIndex(long tx, String name);
    
    /**
     * Return the client object that was used to connect to the federation.
     */
    public IBigdataClient getClient();

    /**
     * Disconnect from the federation.
     */
    public void disconnect();
            
}
