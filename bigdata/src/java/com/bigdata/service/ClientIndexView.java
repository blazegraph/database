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

import java.util.UUID;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.scaleup.PartitionedIndexView;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexView implements IIndex {

    protected final AbstractClient client;
    protected final String name;
    
    protected IMetadataService mdproxy;
    protected UUID indexUUID; // @todo final.
    
    public ClientIndexView(AbstractClient client, String name) {
        
        if(client==null) throw new IllegalArgumentException();
        
        if(name==null) throw new IllegalArgumentException();
        
        this.client = client;

        this.name = name;
        
        /*
         * obtain the proxy for a metadata service. if this instance fails, then
         * we can always ask for a new instance for the same distributed
         * database (failover).
         */
//        mdproxy = client.getMetadataService();
        
        /*
         * obtain the indexUUID.  this is a means of verifying that the index
         * exists.
         */
//        indexUUID = mdproxy.getIndexUUID(name);
        
    }
    
    public UUID getIndexUUID() {
        
        return indexUUID;
        
    }

    public boolean contains(byte[] key) {
        // TODO Auto-generated method stub
        return false;
    }

    public Object insert(Object key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object lookup(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object remove(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    public int rangeCount(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return 0;
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
