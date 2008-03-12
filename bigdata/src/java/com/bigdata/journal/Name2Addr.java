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
package com.bigdata.journal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;

/**
 * BTree mapping index names to the last metadata record committed for the named
 * index. The keys are Unicode strings using the default {@link Locale}. The
 * values are {@link Entry} objects recording the name of the index and the last
 * known address of the {@link IndexMetadata} record for the named index.
 * <p>
 * Note: The {@link Journal} maintains an instance of this class that evolves
 * with each {@link Journal#commit()}. However, the journal also makes use of
 * historical states for the {@link Name2Addr} index in order to resolve the
 * historical state of a named index. Of necessity, the {@link Name2Addr}
 * objects used for this latter purpose MUST be distinct from the evolving
 * instance otherwise the current version of the named index would be resolved.
 * Note further that the historical {@link Name2Addr} states are accessed using
 * a canonicalizing mapping but that current evolving {@link Name2Addr} instance
 * is NOT part of that mapping.
 */
public class Name2Addr extends BTree {

    protected static final Logger log = Logger.getLogger(Name2Addr.class);

    /**
     * Cache of added/retrieved btrees by _name_. This cache is ONLY used by the
     * "live" {@link Name2Addr} instance.
     * <p>
     * Map from the name of an index to a weak reference for the corresponding
     * "live" version of the named index. Entries will be cleared from this map
     * if they are weakly reachable. In order to prevent dirty indices from
     * being cleared, we register an {@link IDirtyListener}. When it is
     * informed that an index is dirty it places a hard reference to that index
     * into the {@link #commitList}.
     * <p>
     * Note: The capacity of the backing hard reference LRU effects how many
     * _clean_ indices can be held in the cache. Dirty indices remain strongly
     * reachable owing to their existence in the {@link #commitList}.
     */
    private final WeakValueCache<String, BTree> indexCache;

    /**
     * The capacity of the inner {@link LRUCache} for the {@link WeakValueCache}.
     * 
     * @todo make the capacity of the backing LRU a configuration option for the
     *       journal. It indirectly effects how many writable indices the
     *       journal will hold open (the effect is indirect owning to the
     *       semantics of weak references and the control of the JVM over when
     *       they are cleared). The capacity will be most important as a tuning
     *       parameter for a data service on which several hot indices are
     *       multiplexed absorbing writes. In this scenario a low capacity could
     *       starve the data service forcing frequent reloading of indices from
     *       the store rather than reuse of the existing mutable index.
     *       <p>
     *       Note: This is complicated since we re-load the {@link Name2Addr}
     *       index from the store using a fixed API unless we persist the size
     *       of the cache in the index metadata, but really you want to be able
     *       to change it each time you start the journal.
     */
    protected final int LRU_CAPACITY = 10;
    
    /**
     * Holds hard references for the dirty indices along with the index name.
     * This collection prevents dirty indices from being cleared from the
     * {@link #indexCache}, which would result in lost updates.
     * <p>
     * Note: Operations on unisolated indices always occur on the "current"
     * state of that index. The "current" state is either unchanged (following a
     * successful commit) or rolled back to the last saved state (by an abort
     * following an unsuccessful commit). Therefore all unisolated index write
     * operations MUST complete before a commit and new unisolated operations
     * MUST NOT begin until the commit has either succeeded or been rolled back.
     * Failure to observe this constraint can result in new unisolated
     * operations writing on indices that should have been rolled back if the
     * commit is not successfull.
     */
    private ConcurrentHashMap<String, DirtyListener> commitList = new ConcurrentHashMap<String, DirtyListener>();
    
    /**
     * An instance of this {@link DirtyListener} is registered with each named
     * index that we administer to listen for events indicating that the index
     * is dirty. When we get that event we stick the {@link DirtyListener} on
     * the {@link #commitList}. This makes the commit protocol simpler since
     * the {@link DirtyListener} has both the name of the index and the
     * reference to the index and we need both on hand to do the commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class DirtyListener implements IDirtyListener {
        
        final String name;
        final IIndex ndx;
        
        DirtyListener(String name,IIndex ndx) {
            
            assert name!=null;
            
            assert ndx!=null;
            
            this.name = name;
            
            this.ndx = ndx;
            
        }

        /**
         * Add <i>this</i> to the {@link Name2Addr#commitList}.
         *  
         * @param btree
         */
        public void dirtyEvent(BTree btree) {

            assert btree == this.ndx;
            
            {
                
                IIndex cached = indexCache.get(name);

                if (cached == null) {

                    /*
                     * There is no index in the cache for this name. This can
                     * occur if someone is holding a reference to a mutable
                     * BTree and they write on it after a commit or abort.
                     */
                    
                    throw new RuntimeException("No index in cache: name="+name);

                }

                if (cached != btree) {

                    /*
                     * There is a different index in the cache for this name.
                     * This can occur if someone is holding a reference to a
                     * mutable BTree and they write on it after a commit or
                     * abort but the named index has already been re-loaded into
                     * the cache.
                     */

                    throw new RuntimeException("Different index in cache: "+name);

                }
                
            }

            log.info("Adding dirty index to commit list: ndx="+name);
            
            commitList.putIfAbsent(name,this);
            
        }

    }

    /**
     * Create a new instance.
     * 
     * @param store
     *            The backing store.
     * 
     * @return The new instance.
     */
    static public Name2Addr create(IRawStore store) {
    
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setClassName(Name2Addr.class.getName());
        
        return (Name2Addr) BTree.create(store, metadata);
        
    }
    
//    public Name2Addr(IRawStore store) {
//
//        super(store, DEFAULT_BRANCHING_FACTOR, UUID.randomUUID());
//
//        // Note: code shared by both constructors.
//        indexCache = new WeakValueCache<String, IIndex>(new LRUCache<String, IIndex>(LRU_CAPACITY));
//
//    }

    /**
     * Load from the store (de-serialization constructor).
     * 
     * @param store
     *            The backing store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadataId
     *            The metadata record for the index.
     */
    public Name2Addr(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {

        super(store, checkpoint, metadata);
        
        indexCache = new WeakValueCache<String, BTree>(new LRUCache<String, BTree>(LRU_CAPACITY));

    }

    /**
     * True iff the index is on the commit list.
     * 
     * @param btree
     * 
     * @return
     */
    boolean willCommit(String name) {
    
        return commitList.containsKey(name);
        
    }
    
    /**
     * Extends the default behavior to cause each named btree to flush itself to
     * the store, updates the address from which that btree may be reloaded
     * within its internal mapping, and finally flushes itself and returns the
     * address from which this btree may be reloaded.
     */
    public long handleCommit() {

        // visit the indices on the commit list.
        Iterator<DirtyListener> itr = commitList.values().iterator();
        
        while(itr.hasNext()) {
            
            DirtyListener l = itr.next();
            
            String name = l.name;
            
            IIndex btree = l.ndx;
            
            log.info("Will commit: "+name);
            
            // request commit.
            final long addr = ((ICommitter)btree).handleCommit();
            
            // encode the index name as a key.
            final byte[] key = getKey(name);
            
            // lookup the current entry (if any) for that index.
            final byte[] val = lookup(key);

            // de-serialize iff entry was found.
            final Entry oldValue = (val == null ? null
                    : EntrySerializer.INSTANCE.deserialize(new DataInputBuffer(
                            val)));
            
            // if there is no existing entry or if the addr has changed.
            if (oldValue == null || oldValue.addr != addr) {

                // then update persistent mapping.
                insert(key, EntrySerializer.INSTANCE.serialize(new Entry(name, addr)));
                
            }
            
//            // place into the object cache on the journal.
//            journal.touch(addr, btree);
            
        }

        // Clear the commit list.
        commitList.clear();
        
        // and flushes out this btree as well.
        return super.handleCommit();
        
    }
    
    /**
     * Encodes a unicode string into a key.
     * 
     * @param name
     *            The name of the btree.
     *            
     * @return The corresponding key.
     */
    protected byte[] getKey(String name) {

        return KeyBuilder.asSortKey(name);

    }

    /**
     * Return the named index - this method tests a cache of the named btrees
     * and will return the same instance if the index is found in the cache.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> iff there is no index with
     *         that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     */
    public BTree get(String name) {

        if (name == null)
            throw new IllegalArgumentException();
        
        BTree btree = indexCache.get(name);
        
        if (btree != null)
            return btree;

        final byte[] val = super.lookup(getKey(name));

        if (val == null) {

            return null;
            
        }
        
        // deserialize entry.
//        final Entry entry = EntrySerializer.INSTANCE.deserialize(new DataInputBuffer(val));
        final Entry entry = EntrySerializer.INSTANCE.deserialize(new DataInputStream(new ByteArrayInputStream(val)));

//        /*
//         * Reload the index from the store using the object cache to ensure a
//         * canonicalizing mapping.
//         */
//        btree = journal.getIndex(entry.addr);
        
        // re-load btree from the store.
        btree = BTree.load(this.store, entry.addr);
        
        // save name -> btree mapping in transient cache.
//        indexCache.put(name,btree);
        indexCache.put(name, btree, false/*dirty*/);

        // listen for dirty events so that we know when to add this to the commit list.
        ((BTree)btree).setDirtyListener(new DirtyListener(name,btree));
        
        // report event (loaded btree).
        ResourceManager.openUnisolatedBTree(name);

        // return btree.
        return btree;

    }
    
    /**
     * Return the address from which the historical state of the named index may
     * be loaded.
     * <p>
     * Note: This is a lower-level access mechanism that is used by
     * {@link Journal#getIndex(String, ICommitRecord)} when accessing historical
     * named indices from an {@link ICommitRecord}.
     * 
     * @param name
     *            The index name.
     * 
     * @return The address or <code>0L</code> if the named index was not
     *         registered.
     */
    protected long getAddr(String name) {

        /*
         * Note: This uses a private cache to reduce the Unicode -> key
         * translation burden. We can not use the normal cache since that maps
         * the name to the index and we have to return the address not the index
         * in order to support a canonicalizing mapping in the Journal.
         */
        synchronized (addrCache) {

            Long addr = addrCache.get(name);

            if (addr == null) {

                final byte[] val = super.lookup(getKey(name));

                if (val == null) {

                    addr = 0L;
                    
                } else {

                    // deserialize entry.
                    final Entry entry = EntrySerializer.INSTANCE.deserialize(new DataInputBuffer(val));
                    
                    addr = entry.addr;
                    
                }

                addrCache.put(name, addr);
                
            }

            return addr;

        }

    }
    /**
     * A private cache used only by {@link #getAddr(String)}.
     */
    private HashMap<String/* name */, Long/* Addr */> addrCache = new HashMap<String, Long>();

    /**
     * Add an entry for the named index.
     * 
     * @param name
     *            The index name.
     * 
     * @param btree
     *            The index.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if <i>btree</i> is <code>null</code>.
     * @exception IndexExistsException
     *                if there is already an index registered under that name.
     */
    public void registerIndex(String name,BTree btree) {
        
        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();
        
        if( ! (btree instanceof ICommitter) ) {
            
            throw new IllegalArgumentException("Index does not implement: "
                    + ICommitter.class);
            
        }

        final byte[] key = getKey(name);
        
        if(super.contains(key)) {
            
            throw new IndexExistsException(name);
            
        }
        
        // flush btree to the store to get the checkpoint record address.
        final long addr = ((ICommitter)btree).handleCommit();
        
        // add a serialized entry to the persistent index.
        super.insert(key,EntrySerializer.INSTANCE.serialize(new Entry(name,addr)));
        
//        // touch the btree in the journal's object cache.
//        journal.touch(addr, btree);
        
        // add name -> btree mapping to the transient cache.
        indexCache.put(name, btree, true/*dirty*/);
        
        DirtyListener l = new DirtyListener(name,btree);
        
        // add to the commit list.
        commitList.put( name, l );

        // set listener on the btree as well.
        ((BTree)btree).setDirtyListener( l );
        
    }

    /**
     * Removes the entry for the named index. The named index will no longer
     * participate in commits.
     * 
     * @param name
     *            The index name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception NoSuchIndexException
     *                if the index does not exist.
     */
    public void dropIndex(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        final byte[] key = getKey(name);
        
        if(!super.contains(key)) {
            
            throw new NoSuchIndexException("Not registered: "+name);
            
        }
        
        // remove the name -> btree mapping from the transient cache.
        IIndex btree = indexCache.remove(name);
        
        if (btree != null) {

            /*
             * Make sure that the index is not on the commit list.
             * 
             * Note: If the index is not in the index cache then it WILL NOT be
             * in the commit list.
             */
            
            commitList.remove(name);
            
            // clear our listener.
            ((BTree)btree).setDirtyListener(null);

        }

        /*
         * Remove the entry from the persistent index. After a commit you will
         * no longer be able to find the metadata record for this index from the
         * current commit record (it will still exist of course in historical
         * commit records).
         */
        super.remove(key);

    }
    
    /**
     * An entry in the persistent index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Entry {
       
        /**
         * The name of the index.
         */
        public final String name;
        
        /**
         * The address of the last known {@link IndexMetadata} record for the
         * index with that name.
         */
        public final long addr;
        
        public Entry(String name,long addr) {
            
            this.name = name;
            
            this.addr = addr;
            
        }
        
    }

    /**
     * The values are {@link Entry}s.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class EntrySerializer {

        public static transient final EntrySerializer INSTANCE = new EntrySerializer();

        private EntrySerializer() {

        }

        public byte[] serialize(Entry entry) {

            try {

                // estimate capacity
                final int capacity = Bytes.SIZEOF_LONG + entry.name.length() * 2;
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream(capacity);
                
                DataOutput os = new DataOutputStream(baos);
                
                os.writeLong(entry.addr);

                os.writeUTF(entry.name);
                
                return baos.toByteArray();

            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }

        }

        public Entry deserialize(DataInput in) {

            try {

                final long addr = in.readLong();

                final String name = in.readUTF();

                return new Entry(name, addr);

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

    }

}
