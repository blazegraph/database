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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.cache.ICacheEntry;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;

/**
 * A {@link BTree} mapping index names to an {@link Entry} containing the last
 * {@link Checkpoint} record committed for the named index and the timestamp of
 * that commit. The keys are Unicode strings using the default {@link Locale}.
 * <p>
 * Note: The {@link AbstractJournal} maintains an instance of this class that
 * evolves with each {@link AbstractJournal#commit()}. However, the journal
 * also makes use of historical states for the {@link Name2Addr} index in order
 * to resolve the historical state of a named index. Of necessity, the
 * {@link Name2Addr} objects used for this latter purpose MUST be distinct from
 * the evolving instance otherwise the current version of the named index would
 * be resolved. Note further that the historical {@link Name2Addr} states are
 * accessed using a canonicalizing mapping but that current evolving
 * {@link Name2Addr} instance is NOT part of that mapping.
 * 
 * FIXME Writes on {@link Name2Addr} are not isolated - they are immediately
 * visible to any concurrent {@link AbstractTask}. When a task completes
 * normally this is not a problem since {@link AbstractTask}s use exclusive
 * locks to avoid concurrent access to the unisolated indices. However, if a
 * task registers or drops an index and then fails its intention is not isolated
 * on {@link Name2Addr} and WILL be visible to subsequent tasks unless the
 * entire commit group is discarded.
 * <p>
 * One way to address this problem is to use a per-instance {@link ThreadLocal}
 * to isolate the writes on {@link Name2Addr} for an {@link AbstractTask}. The
 * task would then propagate those changes atomically onto the {@link Name2Addr}
 * instance after it checkpoints its indices. At this point the task is done
 * doing its work and any subsequent errors in the {@link Thread} running the
 * task would be inside of the group commit protocol.
 * <p>
 * This per-instance {@link ThreadLocal} would be interposed between the task
 * and the {@link #indexCache}.
 * <p>
 * The trick is how this might effect non-concurrent users of {@link Name2Addr} -
 * those directly using the {@link IJournal} API rather than submitting tasks
 * for concurrent execution.
 * <p>
 * AH HA!
 * <p>
 * The underlying problem has been in fact the visibility of changes to the
 * commit list. This has been showing up primarily as lost notifications of
 * dirty btrees. What happens is that a btree sends {@link Name2Addr} notice
 * that it is dirty. If that notice is CONCURRENT with
 * {@link #handleCommit(long)} then the notice is lost when the commit list is
 * cleared. I've jiggled the code a bit and that specific problem no longer
 * arises, but a notice can still arrive before a commit causing the named index
 * to be cleared from the commit list before the task has actually finished
 * execution - another we have another lost update! This is "patched" in
 * AbstractTask which is explicitly jambing things onto the commit list, but the
 * real change is to the visibility.
 * <p>
 * 
 * A tasks intentions towards the {@link Name2Addr} index (adds, drops, and
 * "hey, i'm dirty" MUST be buffered until the task completes and then
 * atomically migrated down onto the {@link Name2Addr} object where it will be
 * made restart safe with the next commit.
 * 
 * <pre>
 * ERROR: 27498 pool-1-thread-40  9730ead2-3fd8-4b8b-8a5f-b716e8466fd1 commitCounter=9 com.bigdata.rdf.store.Term2IdWriteProc [test_term2id] 0 running  com.bigdata.journal.AbstractTask$InnerWriteServiceCallable.checkpointIndices(AbstractTask.java:1216): index not on commit list: name=test_term2id, checkpoint(before)=Checkpoint{height=3,nnodes=100,nleaves=1605,nentries=25695,counter=25695,addrRoot=72660444774767,addrMetadata=183072981501,addrCheckpoint=72685073727568}
 * ERROR: 27498 pool-1-thread-40  9730ead2-3fd8-4b8b-8a5f-b716e8466fd1 commitCounter=9 com.bigdata.rdf.store.Term2IdWriteProc [test_term2id] 0 running  com.bigdata.journal.AbstractTask$InnerWriteServiceCallable.checkpointIndices(AbstractTask.java:1223): index not on commit list: name=test_term2id, checkpoint(after )=Checkpoint{height=3,nnodes=156,nleaves=2688,nentries=49649,counter=49649,addrRoot=288123451867591,addrMetadata=183072981501,addrCheckpoint=288153986400336}
 * </pre>
 * 
 * Said another way:
 * <p>
 * The problem is concurrency in the data structure that keeps track of add/drop
 * for named indices and when those indices become dirty. It was written back in
 * the old single-threaded world and fails to account for concurrent tasks
 * running during a commit. What I need to do is make the add/drop/dirty notices
 * an atomic state change when the task completes and synchronization to prevent
 * that happening concurrently with commit processing.
 * </p>
 * 
 * Note: there needs to be additional synchronization when reading through to
 * the underlying BTree since access to that object is no longer single threaded
 * (commits concurrent with everything else). I've synchronized
 * {@link #handleCommit(long)} for the moment which might do the trick, but this
 * needs to be re-examined.
 * 
 */
public class Name2Addr extends BTree {

    protected static final Logger log = Logger.getLogger(Name2Addr.class);

    /**
     * Cache of added/retrieved btrees by _name_. This cache is ONLY used by the
     * "live" {@link Name2Addr} instance.
     * <p>
     * Map from the name of an index to a weak reference for the corresponding
     * "live" version of the named index. Entries will be cleared from this map
     * after they have become only weakly reachable. In order to prevent dirty
     * indices from being cleared, we register an {@link IDirtyListener}. When
     * it is informed that an index is dirty it places a hard reference to that
     * index into the {@link #commitList}.
     * <p>
     * Note: The capacity of the backing hard reference LRU effects how many
     * _clean_ indices can be held in the cache. Dirty indices remain strongly
     * reachable owing to their existence in the {@link #commitList}.
     */
    private WeakValueCache<String, BTree> indexCache = null;

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
        final BTree ndx;
        
        DirtyListener(String name, BTree ndx) {
            
            assert name!=null;
            
            assert ndx!=null;
            
            this.name = name;
            
            this.ndx = ndx;
            
        }
        
        /**
         * Return the {@link Name2Addr} instance to which this listener is
         * reporting.
         */
        private Name2Addr getName2Addr() {
            
            return Name2Addr.this;
            
        }

        /**
         * Add <i>this</i> to the {@link Name2Addr#commitList}.
         *  
         * @param btree
         */
        public void dirtyEvent(BTree btree) {

            assert btree == this.ndx;
            
            {
                
                final BTree cached = indexCache.get(name);

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

            if(INFO)
            log.info("Adding dirty index to commit list: ndx=" + name);

            /*
             * Note: This MUST be synchronized on the commitList to prevent loss
             * of dirty notifications that arrive while a concurrent commit is
             * in progress.
             */
            
            synchronized(commitList) {

                commitList.putIfAbsent(name, this);
                
            }
            
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
    
    /**
     * Load from the store (de-serialization constructor).
     * 
     * @param store
     *            The backing store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadata
     *            The metadata record for the index.
     */
    public Name2Addr(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {

        super(store, checkpoint, metadata);
        
    }

    /**
     * Setup the {@link #indexCache}.
     * <p>
     * Note: This cache is <code>null</code> unless initialized and is ONLY
     * used by the "live" version of the {@link Name2Addr} index. The only
     * method that creates or loads the "live" {@link Name2Addr} index is
     * {@link AbstractJournal#setupName2AddrBTree()}.
     * 
     * @param cacheCapacity
     *            The capacity of the inner {@link LRUCache} for the
     *            {@link WeakValueCache}.
     * 
     * @see Options#LIVE_INDEX_CACHE_CAPACITY
     */
    synchronized protected void setupCache(int cacheCapacity) {
        
        if (indexCache != null) {

            // Cache was already configured.
            
            throw new IllegalStateException();
            
        }
        
        indexCache = new WeakValueCache<String, BTree>(
                new LRUCache<String, BTree>(cacheCapacity));
        
    }
    
    /**
     * True iff the named index is on the commit list.
     * 
     * @param name
     *            The index name.
     */
    boolean willCommit(String name) {
    
        /*
         * Note: does not need to be synchronized since the commitList is
         * thread-safe.
         */
        
        return commitList.containsKey(name);
        
    }
    
    /**
     * Extends the default behavior to cause each named btree to flush itself to
     * the store, updates the address from which that btree may be reloaded
     * within its internal mapping, and finally flushes itself and returns the
     * address from which this btree may be reloaded.
     */
    synchronized
    public long handleCommit(final long commitTime) {

        /*
         * Note: the commit list MUST be protected against concurrent
         * modification during the commit (concurrent tasks could be reporting
         * dirty btrees while we are doing a commit and those notices would be
         * lost).
         * 
         * Indices get onto the commitList via the DirtyListener, so it is also
         * synchronized on the commitList.
         */
        final DirtyListener[] a;
        synchronized(commitList) {
            
            // snapshot the commit list to avoid concurrent mods.
            a = commitList.values().toArray(new DirtyListener[]{});
            
            // clear the commit list.
            commitList.clear();
            
        }

        // visit the indices on the commit list. @todo just use for() loop.
//        final Iterator<DirtyListener> itr = commitList.values().iterator();
        final Iterator<DirtyListener> itr = Arrays.asList(a).iterator();
        
        while(itr.hasNext()) {
            
            final DirtyListener l = itr.next();
            
            final String name = l.name;
            
            final BTree btree = l.ndx;
            
            log.info("Will commit: "+name);
            
            // request commit.
            final long checkpointAddr;
            try {

                checkpointAddr = btree.handleCommit(commitTime);
                
            } catch(Throwable t) {

                // adds the name to the stack trace.
                throw new RuntimeException("Could not commit index: name="
                        + name, t);
                
            }
            
            // set commitTime on the btree: @todo could be done by BTree#handleCommit() as easily.
            btree.setLastCommitTime(commitTime);
            
            // encode the index name as a key.
            final byte[] key = getKey(name);

            // lookup the current entry (if any) for that index.
            final byte[] val = lookup(key);

            // de-serialize iff entry was found.
            final Entry oldEntry = (val == null ? null
                    : EntrySerializer.INSTANCE.deserialize(new DataInputBuffer(
                            val)));

            /*
             * Update if there is no existing entry or if the checkpointAddr has
             * changed or if there was no commit time on the old entry.
             */
            if (oldEntry == null || oldEntry.checkpointAddr != checkpointAddr
                    || oldEntry.commitTime == 0L) {

                final Entry entry = new Entry(name, checkpointAddr, commitTime);
                
                // update persistent mapping.
                insert(key, EntrySerializer.INSTANCE.serialize( entry ));

                // update the transient cache.
                if (addrCache != null) {
                    
                    synchronized (addrCache) {
                    
                        addrCache.put(name, entry);
                        
                    }
                    
                }
                
            }
            
//            // place into the object cache on the journal.
//            journal.touch(addr, btree);
            
        }

//        // Clear the commit list.
//        commitList.clear();
        
        // and flushes out this btree as well.
        return super.handleCommit(commitTime);
        
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

        if (btree != null) {

            if (btree.getDirtyListener() == null) {

                /*
                 * Note: We can't return an unisolated view of a BTree to the
                 * caller without having a dirty listener set on it that will
                 * report any changes back to this name2addr instance.  An
                 * exception thrown here indicates that the BTree was able to
                 * remain in (or enter into) the indexCache without having its
                 * dirty listener set.
                 */

                throw new AssertionError();

            }

            /*
             * Further verify that the dirty listener is reporting to this
             * name2addr instance.
             */
            assert ((DirtyListener)btree.getDirtyListener()).getName2Addr() == this;

            return btree;

        }

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
        btree = BTree.load(this.store, entry.checkpointAddr);
        
        // set the lastCommitTime on the index.
        btree.setLastCommitTime(entry.commitTime);
        
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
     * Return the {@link Entry} for the named index.
     * <p>
     * Note: This is a lower-level access mechanism that is used by
     * {@link Journal#getIndex(String, ICommitRecord)} when accessing historical
     * named indices from an {@link ICommitRecord}.
     * 
     * @param name
     *            The index name.
     * 
     * @return The {@link Entry} for the named index.
     */
    public Entry getEntry(String name) {

        if (addrCache != null) {

            /*
             * Note: This uses a private cache to reduce the Unicode -> key
             * translation burden. We can not use the normal cache since that
             * maps the name to the index and we have to return the address not
             * the index in order to support a canonicalizing mapping in the
             * Journal.
             */
            synchronized (addrCache) {

                // check our pricate cache.
                Entry entry = addrCache.get(name);

                if (entry == null) {

                    // lookup in the index.
                    final byte[] val = super.lookup(getKey(name));

                    if (val != null) {

                        // deserialize entry.
                        entry = EntrySerializer.INSTANCE
                                .deserialize(new DataInputBuffer(val));

                        // update cache.
                        addrCache.put(name, entry);

                    }

                }

                return entry;

            }
            
        } else {

            // lookup in the index.
            final byte[] val = super.lookup(getKey(name));

            Entry entry = null;

            if (val != null) {

                // deserialize entry.
                entry = EntrySerializer.INSTANCE
                        .deserialize(new DataInputBuffer(val));

            }

            return entry;

        }

    }
    /**
     * A private cache used only by {@link #getEntry(String)}.
     */
    private HashMap<String/* name */, Entry> addrCache = new HashMap<String, Entry>();

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
    public void registerIndex(String name, BTree btree) {

        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();

        if (!(btree instanceof ICommitter)) {

            throw new IllegalArgumentException("Index does not implement: "
                    + ICommitter.class);

        }

        final byte[] key = getKey(name);

        if (super.contains(key)) {

            throw new IndexExistsException(name);

        }

        // flush btree to the store to get the checkpoint record address.
        final long checkpointAddr = btree.writeCheckpoint();

        /*
         * Add a serialized entry to the persistent index.
         * 
         * Note: The commit time here is a placeholder. It will be replaced with
         * the actual commit time by the next commit since the newly created
         * B+Tree is on our commit list. If there is an abort, then the entry is
         * simply discarded along with the rest of the Name2Addr state.
         */
        
        final Entry entry = new Entry(name, checkpointAddr, 0L/* commitTime */);
        
        super.insert(key, EntrySerializer.INSTANCE.serialize( entry ));
        
//        // touch the btree in the journal's object cache.
//        journal.touch(addr, btree);
        
        // add name -> btree mapping to the transient cache.
        indexCache.put(name, btree, true/*dirty*/);
        
        final DirtyListener l = new DirtyListener(name,btree);
        
        /*
         * Add to the commit list.
         * 
         * Note: MUST be synchronized since the change could otherwise be lost
         * with a concurrent commit.
         */
        synchronized(commitList) {
         
            commitList.put( name, l );
            
        }

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
        final BTree btree = indexCache.remove(name);
        
        if (btree != null) {

            /*
             * Make sure that the index is not on the commit list.
             * 
             * Note: If the index is not in the index cache then it WILL NOT be
             * in the commit list.
             */
            commitList.remove(name);
            
            // clear our listener.
            ((BTree) btree).setDirtyListener(null);

            if (addrCache != null) {
                
                synchronized (addrCache) {

                    addrCache.remove(name);

                }
                
            }
            
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
     * Rollback the named index to the specified {@link Checkpoint} address.
     * <p>
     * The change will be immediately visible to uses of {@link Name2Addr} but
     * will not be restart safe until the next commit.
     * 
     * @param entry
     *            A historical {@link Entry} for a named index.
     */
    public void rollback(Entry entry) {
        
        if (entry == null)
            throw new IllegalArgumentException();
        
        log.warn("entry="+entry);
        
        final byte[] key = getKey(entry.name);

        // @todo if the entry is equals then don't bother to update anything (rollback is NOP)?

        // update persistent mapping.
        insert(key, EntrySerializer.INSTANCE.serialize(entry));

        // update the transient cache.
        if (addrCache != null) {

            synchronized (addrCache) {

                addrCache.put(entry.name, entry);

            }

        }

        /*
         * Remove from the cache.
         */
        final BTree btree = indexCache.remove(entry.name);

        if (btree != null) {

            // clear the dirty listener on the btree.
            btree.setDirtyListener(null);

        }
        
        /*
         * Remove from the commit list.
         * 
         * Note: The index does not need to perform a commit after a rollback
         * and the change is already buffered on name2addr's backing btree.
         */
        commitList.remove(entry.name);

    }

    /**
     * Return a counter set reflecting the named indices that are currently open
     * (more accurately, those open named indices whose references are in
     * {@link Name2Addr}s internal {@link #indexCache}).
     * 
     * @return A new {@link CounterSet} reflecting the named indices that were
     *         open as of the time that this method was invoked.
     */
    public CounterSet getNamedIndexCounters() {

        final CounterSet tmp = new CounterSet();
        
        final Iterator<ICacheEntry<String,BTree>> itr = indexCache.entryIterator();
        
        while (itr.hasNext()) {

            final ICacheEntry<String, BTree> entry = itr.next();

            tmp.makePath(entry.getKey()).attach(entry.getObject().getCounters());

        }
        
        return tmp;

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
         * The address of the last known {@link Checkpoint} record for the
         * index with that name.
         */
        public final long checkpointAddr;

        /**
         * The commit time associated with the last commit point for the named
         * index.
         */
        public final long commitTime;

        public Entry(String name, long checkpointAddr, long commitTime) {
            
            this.name = name;
            
            this.checkpointAddr = checkpointAddr;
            
            this.commitTime = commitTime;
            
        }
        
        public String toString() {
            
            return "Entry{name=" + name + ",checkpointAddr=" + checkpointAddr + ",commitTime=" + commitTime + "}";
            
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

                os.writeLong(entry.commitTime);

                os.writeLong(entry.checkpointAddr);

                os.writeUTF(entry.name);
                
                return baos.toByteArray();

            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }

        }

        public Entry deserialize(DataInput in) {

            try {

                final long commitTime = in.readLong();
                
                final long checkpointAddr = in.readLong();

                final String name = in.readUTF();

                return new Entry(name, checkpointAddr, commitTime);

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

    }

}
