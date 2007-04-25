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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.PartitionedIndexView;
import com.bigdata.service.BigdataClient.BigdataFederation;
import com.bigdata.service.BigdataClient.IBigdataFederation;

/**
 * A client-side view of an index.
 * 
 * @todo cache leased information about index partitions of interest to the
 *       client. The cache will be a little tricky since we need to know when
 *       the client does not possess a partition definition. Index partitions
 *       are defined by the separator key - the first key that lies beyond that
 *       partition. the danger then is that a client will presume that any key
 *       before the first leased partition is part of that first partition. To
 *       guard against that the client needs to know both the separator key that
 *       represents the upper and lower bounds of each partition. If a lookup in
 *       the cache falls outside of any known partitions upper and lower bounds
 *       then it is a cache miss and we have to ask the metadata service for a
 *       lease on the partition. the cache itself is just a btree data structure
 *       with the proviso that some cache entries represent missing partition
 *       definitions (aka the lower bounds for known partitions where the left
 *       sibling partition is not known to the client).
 * 
 * @todo support partitioned indices by resolving client operations against each
 *       index partition as necessary and maintaining leases with the metadata
 *       service - the necessary logic is in the {@link PartitionedIndexView}
 *       and can be refactored for this purpose.
 * 
 * @todo offer alternatives for the batch insert and remove methods that do not
 *       return the old values to the client so that the client may opt to
 *       minimize network traffic for data that it does not need.
 * 
 * @todo develop and offer policies for handling index partitions that are
 *       unavailable at the time of the request (continued operation during
 *       partial failure).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexView implements IIndex {

    private final BigdataFederation fed;
    private final long tx;
    private final String name;

    /**
     * The unique index identifier (initally null and cached once fetched).
     */
    private UUID indexUUID;
    
    /**
     * Obtain the proxy for a metadata service. if this instance fails, then we
     * can always ask for a new instance for the same federation (failover).
     */
    protected IMetadataService getMetadataService() {
        
        return fed.getMetadataService();
        
    }
    
    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param tx
     *            The transaction identifier or zero(0L) iff the index view is
     *            not isolated by a transaction.
     * @param name
     *            The index name.
     */
    public ClientIndexView(BigdataFederation fed, long tx, String name) {
        
        if(fed ==null) throw new IllegalArgumentException();
        
        if(name==null) throw new IllegalArgumentException();
    
        if(tx != IBigdataFederation.UNISOLATED) {
            
            throw new UnsupportedOperationException(
                    "Only unisolated views are supported at this time");
            
        }
        
        this.fed = fed;

        this.tx = tx;
        
        this.name = name;
       
    }
    
    public UUID getIndexUUID() {
        
        if(indexUUID==null) {

            /*
             * obtain the UUID for the managed scale-out index.
             */
            
            try {

                indexUUID = getMetadataService().getManagedIndexUUID(name);
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

        }
        
        return indexUUID;
        
    }

    public boolean contains(byte[] key) {
        
        IPartitionMetadata pmd = fed.getPartition(name, key);

        IDataService dataService = fed.getDataService(pmd);

        final boolean[] ret;
        
        try {

            ret = dataService.batchContains(tx, name, 1, new byte[][]{key});
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return ret[0];
        
    }

    public Object insert(Object key, Object value) {

        IPartitionMetadata pmd = fed.getPartition(name, (byte[])key);
        
        IDataService dataService = fed.getDataService(pmd);

        final boolean returnOldValues = true;
        
        final byte[][] ret;
        
        try {
            
            ret = dataService.batchInsert(tx, name, 1,
                    new byte[][] { (byte[]) key },
                    new byte[][] { (byte[]) value }, returnOldValues);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return ret[0];

    }

    public Object lookup(Object key) {

        IPartitionMetadata pmd = fed.getPartition(name, (byte[])key);

        IDataService dataService = fed.getDataService(pmd);
        
        final byte[][] ret;
        
        try {
            
            ret = dataService.batchLookup(tx, name, 1,
                    new byte[][] { (byte[]) key });

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return ret[0];

    }

    public Object remove(Object key) {

        IPartitionMetadata pmd = fed.getPartition(name, (byte[])key);

        IDataService dataService = fed.getDataService(pmd);
        
        final byte[][] ret;
        
        final boolean returnOldValues = true;
        
        try {
            
            ret = dataService.batchRemove(tx, name, 1,
                    new byte[][] { (byte[]) key }, returnOldValues );

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return ret[0];

    }

    /**
     * Returns the sum of the range count for each index partition spanned by
     * the key range.
     * 
     * FIXME support range count that spans more than one partition.
     * 
     * FIXME Handle fromKey == null, toKey == null.
     * 
     * @todo note that it is possible (though unlikely) for an index partition
     *       split or join to occur during this operation. Figure out how I want
     *       to handle that, and how I want to handle that with transactional
     *       isolation (presumably a read-only historical view of the metadata
     *       index would be used - in which case we need to pass the tx into
     *       the getPartition() method).
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {

        IPartitionMetadata pmd1 = fed.getPartition(name, (byte[])fromKey);
        
        IPartitionMetadata pmd2 = fed.getPartition(name, (byte[])toKey);
        
        if(pmd2.getPartitionId()!=pmd1.getPartitionId()) {
            
            throw new UnsupportedOperationException(
                    "Can not span partitions at this time");
            
        }
        
        IDataService dataService = fed.getDataService(pmd1);

        int rangeCount = 0;
        
        try {

            rangeCount += dataService.rangeCount(IDataService.UNISOLATED, name,
                    fromKey, toKey);
        
        } catch(Exception ex) {

            throw new RuntimeException(ex);
            
        }
        
        return rangeCount;
        
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return null;
    }

    public void contains(BatchContains op) {
        // TODO Auto-generated method stub
        
    }

    public void insert(BatchInsert op) {
        // TODO Auto-generated method stub
        
    }

    public void lookup(BatchLookup op) {
        // TODO Auto-generated method stub
        
    }

    public void remove(BatchRemove op) {
        // TODO Auto-generated method stub
        
    }
    
}
