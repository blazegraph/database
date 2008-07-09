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
 * Created on Feb 21, 2007
 */

package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;

/**
 * A temporary store that supports named indices.
 * 
 * @todo Consider a re-write a wrapper for creating a
 *       {@link BufferMode#Temporary} {@link Journal}.
 *       <p>
 *       This has the advantage of full concurrency support, group commit, and
 *       low-latency startup (since the file is not created until the store
 *       attempts to write through to the disk).
 *       <p>
 *       However, the current approach is lighter weight precisely because it
 *       does not provide the thread-pool for concurrency control, so maybe it
 *       is useful to keep both approaches.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStore extends TemporaryRawStore implements IBTreeManager {

    /**
     * The size of the live index cache for the {@link Name2Addr} instance.
     * 
     * @todo this should be a configuration property once the temporary store
     *       accepts a {@link Properties} object in its ctor.
     * 
     * @see Options#DEFAULT_LIVE_INDEX_CACHE_CAPACITY
     */
    private final int liveIndexCacheCapacity = 20;
    
    /**
     * 
     */
    public TemporaryStore() {

        this(WormAddressManager.SCALE_UP_OFFSET_BITS);
        
    }

    /**
     * @param offsetBits
     *            This determines the capacity of the store file and the maximum
     *            length of a record.  The value is passed through to
     *            {@link WormAddressManager#WormAddressManager(int)}.
     */
    public TemporaryStore(int offsetBits) {

        super(0L/* maximumExtent */, offsetBits, getTempFile());

        setupName2AddrBTree();

//        relationLocator = new DefaultRelationLocator(executorService,this);
        
    }
    
    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known address of the named btree.
     * <p>
     * Note: This is a mutable {@link BTree} so it is NOT thread-safe. We always
     * synchronize on this object before accessing it.
     */
    private Name2Addr name2Addr;

    /**
     * Setup the btree that resolved named btrees.
     */
    private void setupName2AddrBTree() {

        assert name2Addr == null;
        
        name2Addr = Name2Addr.create(this);

        name2Addr.setupCache(liveIndexCacheCapacity);
        
    }

    public void registerIndex(IndexMetadata metadata) {
        
        registerIndex(metadata.getName(), metadata);
        
    }
    
    public BTree registerIndex(String name, IndexMetadata metadata) {
    
        BTree btree = BTree.create(this, metadata);

        return registerIndex(name, btree);
        
    }
    
    public BTree registerIndex(String name, BTree btree) {

        synchronized (name2Addr) {

            // add to the persistent name map.
            name2Addr.registerIndex(name, btree);

            return btree;

        }
        
    }
    
    public void dropIndex(String name) {
        
        synchronized(name2Addr) {

            // drop from the persistent name map.
            name2Addr.dropIndex(name);
            
        }
        
    }

    /**
     * Return the named index (unisolated). Writes on the returned index will be
     * made restart-safe with the next {@link #commit()} regardless of the
     * success or failure of a transaction. Transactional writes must use the
     * same named method on the {@link Tx} in order to obtain an isolated
     * version of the named btree.
     */
    public BTree getIndex(String name) {

        synchronized(name2Addr) {

            return name2Addr.getIndex(name);
            
        }

    }
    
    /**
     * Historical reads and transactions are not supported.
     * 
     * @throws UnsupportedOperationException
     *             unless the timestamp is either {@link ITx#READ_COMMITTED} or
     *             {@link ITx#UNISOLATED}.
     */
    public BTree getIndex(String name,long timestamp) {
        
        if(timestamp == ITx.READ_COMMITTED) {
            
            final long checkpointAddr;
            
            synchronized(name2Addr) {
                
                final Entry entry = name2Addr.getEntry(name);
                
                if (entry == null) {

                    log.warn("No such index: name=" + name + ", timestamp="
                            + TimestampUtility.toString(timestamp));
                    
                    return null;
                    
                }

                checkpointAddr = entry.checkpointAddr;
                
            }
            
            return BTree.load(this, checkpointAddr);
            
        }
        
        if(timestamp == ITx.UNISOLATED) {
            
            return getIndex(name);
            
        }

        throw new UnsupportedOperationException(
                "Not supported: timestamp="
                + TimestampUtility.toString(timestamp));
        
    }

    public SparseRowStore getGlobalRowStore() {

        return globalRowStoreHelper.getGlobalRowStore();
        
    }
    private GlobalRowStoreHelper globalRowStoreHelper = new GlobalRowStoreHelper(this); 

//    public IRelationLocator getRelationLocator() {
//        
//        return relationLocator;
//    
//    }
//    private final IRelationLocator relationLocator;
    
}
