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
import java.util.Iterator;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
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
import com.bigdata.counters.ICounterSet;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;

/**
 * <p>
 * {@link Name2Addr} is a {@link BTree} mapping index names to an {@link Entry}
 * containing the last {@link Checkpoint} record committed for the named index
 * and the timestamp of that commit. The keys are Unicode strings using the
 * default {@link Locale}. The {@link Entry}s in {@link Name2Addr} are the set
 * of registered named indices for an {@link AbstractJournal}.
 * </p>
 * <p>
 * The {@link AbstractJournal} maintains an instance of this class that evolves
 * with each {@link AbstractJournal#commit()} and tracks the {@link Checkpoint}
 * records of the registered {@link ITx#UNISOLATED} indices. However, the
 * journal also makes use of historical states for the {@link Name2Addr} index
 * in order to resolve the historical state of a named index. Of necessity, the
 * {@link Name2Addr} objects used for this latter purpose MUST be distinct from
 * the evolving instance otherwise the current version of the named index would
 * be resolved. Note further that the historical {@link Name2Addr} states are
 * accessed using a canonicalizing mapping but that current evolving
 * {@link Name2Addr} instance is NOT part of that mapping.
 * </p>
 * <p>
 * Concurrent reads are permitted against the historical {@link Name2Addr}
 * objects since the {@link BTree} is thread-safe for read-only operations.
 * Likewise, writes are only allowed on the {@link ITx#UNISOLATED}
 * {@link Name2Addr} instance. <em>Write</em> access to the underlying
 * {@link BTree} MUST be synchronized on the {@link ITx#UNISOLATED}
 * {@link Name2Addr} instance since the {@link BTree} is NOT safe for concurrent
 * writers. Further, <em>read</em> access to {@link ITx#UNISOLATED}
 * {@link Name2Addr} object MUST be synchronized so as to not conflict with
 * writes on that {@link BTree}. Therefore all <em>write</em> methods on this
 * class are declared as <code>synchronized</em> but the caller MUST synchronize
 * on {@link Name2Addr} if they are performing reads on the {@link ITx#UNISOLATED}
 * {@link Name2Addr} instance.  This allows readers on historical {@link Name2Addr} 
 * instances to have full concurrency.
 * </p>
 * <p>
 * Note: {@link Name2Addr} by itself is NOT sufficient to handle commits with
 * concurrent task execution, such as arises with the group commit protocol. The
 * problem is concurrency in the data structure that keeps track of add/drop for
 * named indices and also tracks which named indices are dirty. In order to account for
 * tasks running concurent with commit processing, {@link AbstractTask} isolates
 * {@link Name2Addr} and makes the set of changes {registering indices, dropping
 * indices, and updating the {@link Entry} in {@link Name2Addr} to reflect the
 * current {@link Checkpoint} record for an index) an atomic state change that
 * is performed IFF the task completes successfully and is synchronized on
 * {@link Name2Addr} to prevent that happening concurrent with commit
 * processing.
 * </p>
 */
public class Name2Addr extends BTree {

    protected static final Logger log = Logger.getLogger(Name2Addr.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

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
    private class DirtyListener implements IDirtyListener, Comparable<DirtyListener> {
        
        final String name;
        final BTree btree;
        boolean needsCheckpoint;
        long checkpointAddr = 0L;
        
        public String toString() {
            
            return "DirtyListener{name="
                    + name
                    + ","
                    + (needsCheckpoint ? "needsCheckpoint" : "checkpointAddr="
                            + checkpointAddr) + "}";
            
        }
        
        private DirtyListener(String name, BTree btree, boolean needsCheckpoint) {
            
            assert name!=null;
            
            assert btree!=null;
            
            this.name = name;
            
            this.btree = btree;
            
            this.needsCheckpoint = needsCheckpoint;
            
            if(!needsCheckpoint) {
                
                /*
                 * Grab the checkpointAddr from the BTree.
                 */
                
                try {

                    checkpointAddr = btree.getCheckpoint().getCheckpointAddr();
                    
                } catch(IllegalStateException ex) {
                    
                    throw new RuntimeException(
                            "Checkpoint record not written: " + name);
                    
                }

            }
            
        }
        
        /**
         * Return the {@link Name2Addr} instance to which this listener is
         * reporting.
         */
        private Name2Addr getName2Addr() {
            
            return Name2Addr.this;
            
        }

        /**
         * Add <i>this</i> to the {@link Name2Addr#commitList} and set
         * {@link #needsCheckpoint} to <code>true</code>.
         * 
         * @param btree
         */
        public void dirtyEvent(BTree btree) {

            assert btree == this.btree;

            synchronized(Name2Addr.this) {
                
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

                /*
                 * Note: This MUST be synchronized to prevent loss of dirty
                 * notifications that arrive while a concurrent commit is in
                 * progress.
                 */

                commitList.putIfAbsent(name, this);

                needsCheckpoint = true;
                
                checkpointAddr = 0L;
                    
            } // synchronized.
            
            if(INFO)
                log.info("Added index to commit list: name=" + name);

        }

        /**
         * Puts instances into order by their {@link #name}
         */
        public int compareTo(DirtyListener arg0) {

            return name.compareTo(arg0.name);
            
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
     * Many methods on this class will throw an {@link IllegalStateException}
     * unless they are invoked on the {@link ITx#UNISOLATED} {@link Name2Addr}
     * instance. This method is used to test that assertion for those methods.
     * 
     * @throws IllegalStateException
     *             unless this is the {@link ITx#UNISOLATED} {@link Name2Addr}
     *             instance.
     */
    private void assertUnisolatedInstance() {
        
        if(indexCache == null) {
            
            throw new IllegalStateException();
            
        }
        
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
    protected void setupCache(int cacheCapacity) {
        
        if (indexCache != null) {

            // Cache was already configured.
            
            throw new IllegalStateException();
            
        }
        
        indexCache = new WeakValueCache<String, BTree>(
                new LRUCache<String, BTree>(cacheCapacity));
        
    }
    
    /**
     * Return <code>true</code> iff the named index is on the commit list.
     * <p>
     * Note: This is synchronized even through the commitList is thread-safe in
     * order to make the test atomic with respect to {@link #handleCommit(long)}.
     * <p>
     * Note: This will always return <code>false</code> unless you are using
     * the {@link ITx#UNISOLATED} {@link Name2Addr} object since the
     * {@link #commitList} will always be empty.
     * 
     * @param name
     *            The index name.
     */
    synchronized public boolean willCommit(String name) {

        assertUnisolatedInstance();

        return commitList.containsKey(name);
        
    }
    
    /**
     * Commit processing for named indices.
     * <p>
     * This method applies the {@link #commitList} and then flushes the backing
     * {@link BTree} to the store. The {@link #commitList} consists of
     * {@link DirtyListener}s. If the listener has its
     * {@link DirtyListener#needsCheckpoint} flag set, then the {@link BTree} to
     * which that listener is attached will have its
     * {@link BTree#writeCheckpoint() checkpoint written}. Otherwise the
     * {@link BTree} current {@link Checkpoint} address is recovered. Either
     * way, the {@link Entry} in {@link Name2Addr}s backing {@link BTree} is
     * updated to reflect the <i>commitTime</i> and {@link Checkpoint} address
     * for the index.
     * <p>
     * Finally {@link Name2Addr} {@link Checkpoint}s itself using
     * {@link BTree#handleCommit(long)} and returns the address from which
     * {@link Name2Addr} may be reloaded.
     * <p>
     * Note: The {@link #commitList} MUST be protected against concurrent
     * modification during the commit otherwiser concurrent tasks could be
     * reporting dirty {@link BTree}s while we are doing a commit and those
     * notices would be lost. {@link BTree}s get onto the {@link #commitList}
     * via the {@link DirtyListener}, so it is also synchronized.
     * <p>
     * Note: {@link Name2Addr} DOES NOT obtain a resource lock on the
     * {@link BTree}. Therefore it MUST NOT checkpoint an index on which an
     * {@link AbstractTask} has obtained a resource lock. Otherwise we have
     * concurrent writers on the {@link BTree} and the {@link BTree} is not
     * thread-safe for concurrent writers. Instead, the {@link AbstractTask}
     * checkpoints the {@link BTree} itself while it is holding the resource
     * lock and then sets {@link DirtyListener#needsCheckpoint} to
     * <code>false</code> using
     * {@link #putOnCommitList(String, BTree, boolean)} as an indication to
     * {@link Name2Addr} that it MUST persist the current checkpointAddr for the
     * {@link BTree} on its next commit (and MUST NOT write on the index when it
     * does that commit).
     */
    synchronized
    public long handleCommit(final long commitTime) {

        assertUnisolatedInstance();

        // snapshot the commit list
        final DirtyListener[] a = commitList.values().toArray(new DirtyListener[] {});
        
        // clear the commit list.
        commitList.clear();

        /*
         * Place into sorted order as an aid to debugging when examining the
         * commit list.
         * 
         * Note: This also approximates the order of the generated keys for the
         * indices which makes the operations on the underlying BTree somewhat
         * more efficient as they are more or less in key order. (The order is
         * only approximate since a Unicode collator determines the real order
         * for the sort keys generated from the index names).
         */
        Arrays.sort(a);
        
        if (INFO) {
            
            log.info("There are " + a.length + " dirty indices : "
                    + Arrays.toString(a));
            
        }

        // for each entry in the snapshot of the commit list.
        for(int i=0; i<a.length; i++) {
            
            final DirtyListener l = a[i];
            
            if(INFO) log.info("Will commit: "+l.name);
            
            final long checkpointAddr;
            if (l.needsCheckpoint) {

                /*
                 * Note: AbstractTask flags [needsCheckpoint := false] on the
                 * DirtyListener and handles the BTree checkpoint itself in
                 * order to avoid the possibility of a concurrent modification
                 * by this code during commit processing.
                 */
                
                try {

                    // checkpoint the index.
                    checkpointAddr = l.btree.handleCommit(commitTime);

                    // we just did the checkpoint.
                    l.needsCheckpoint = false;

                } catch (Throwable t) {

                    // adds the name to the stack trace.
                    throw new RuntimeException("Could not commit index: name="
                            + l.name + ", commitList=" + Arrays.toString(a), t);

                }
                
            } else {

                /*
                 * Note: AbstractTask avoids concurrent modification of the
                 * BTree checkpoint record during a commit by synchronizing on
                 * Name2Addr.
                 * 
                 * Note: The DirtyListener grabs the current checkpointAddr from
                 * the BTree when [needsCheckpoint := false]. This allows us to
                 * have asynchronous checkpoints of the BTree (by concurrent
                 * tasks) without causing the checkpoint address on the
                 * commitList to be advanced until the next atomic transfer of
                 * state from a completed AbstractTask down to Name2Addr.
                 * (Without this, the checkpointAddr on the BTree winds up
                 * updated concurrently but we only notice the problem when it
                 * happens to be 0L because the BTree is in the midst of
                 * updating its checkpoint!)
                 */
                
                // use last recorded checkpoint.
                checkpointAddr = l.checkpointAddr;

                if (checkpointAddr == 0L) {

                    throw new RuntimeException(
                            "Checkpoint address not written: name=" + l.name);
                    
                }
                                
            }
            
            // set commitTime on the btree (transient field).
            l.btree.setLastCommitTime(commitTime);
            
            // encode the index name as a key.
            final byte[] key = getKey(l.name);

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

                final Entry entry = new Entry(l.name, checkpointAddr, commitTime);
                
                // update persistent mapping.
                insert(key, EntrySerializer.INSTANCE.serialize( entry ));

            }
            
        }
        
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
     * Return the named {@link ITx#UNISOLATED} index - this method tests a cache
     * of the named {@link BTree}s and will return the same instance if the
     * index is found in the cache.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> iff there is no index with
     *         that name.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     * @throws IllegalStateException
     *             if this is not the {@link ITx#UNISOLATED} {@link Name2Addr}
     *             instance.
     */
    public BTree getIndex(String name) {

        assertUnisolatedInstance();

        if (name == null) {

            throw new IllegalArgumentException();
            
        }

        BTree btree;
        synchronized(this) {
        
            /*
             * Note: Synchronized since some operations (remove+add) are not
             * otherwise atomic.
             */
            
            btree = indexCache.get(name);
            
        }

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
//        indexCache.put(name, btree, false/*dirty*/);
        putIndexCache(name, btree, false/*replace*/);
        
        // listen for dirty events so that we know when to add this to the commit list.
        
        final DirtyListener l = new DirtyListener(name, btree, false/* needsCheckpoint */);
        
        btree.setDirtyListener( l );
        
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
     * @return The {@link Entry} for the named index -or- <code>null</code> if
     *         there is no entry for that <i>name</i>.
     */
    public Entry getEntry(String name) {

        // lookup in the index.
        final byte[] val = super.lookup(getKey(name));

        Entry entry = null;

        if (val != null) {

            // deserialize entry.
            entry = EntrySerializer.INSTANCE.deserialize(new DataInputBuffer(
                    val));

        }

        return entry;

    }

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
    synchronized public void registerIndex(String name, BTree btree) {

        assertUnisolatedInstance();

        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();
        
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
        
        putOnCommitList(name, btree, false/* needsCheckpoint */);
        
        // report event (the application has access to the named index).
        ResourceManager.openUnisolatedBTree(name);
        
    }

    /**
     * Adds the named index to the commit list and sets a {@link DirtyListener}
     * on the {@link BTree} so that this {@link Name2Addr} object will be
     * informed if the {@link BTree} becomes dirty.
     * 
     * @param name
     *            The index name.
     * @param btree
     *            The {@link BTree}.
     * @param needsCheckpoint
     *            Specify <code>true</code> if {@link Name2Addr} should
     *            {@link BTree#writeCheckpoint()} the index rather than just
     *            updating the {@link Entry} from {@link BTree#getCheckpoint()}
     */
    synchronized protected void putOnCommitList(String name, BTree btree,
            boolean needsCheckpoint) {

        assertUnisolatedInstance();

        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();
        
        // setup a dirty listener.
        final DirtyListener l = new DirtyListener(name, btree, needsCheckpoint);
        
        // and set it on the btree.
        btree.setDirtyListener(l);

        putIndexCache(name, btree, true/*replace*/);
        
        // add to the commit list.
        commitList.put(name, l);
        
    }

    /**
     * Adds the named index to the {@link ITx#UNISOLATED} index cache.
     * 
     * @param name
     *            The index name.
     * @param btree
     *            The {@link ITx#UNISOLATED} {@link BTree}.
     * @param replace
     *            If an existing entry for that name may be replaced.
     */
    synchronized protected void putIndexCache(String name, BTree btree,
            boolean replace) {

        assertUnisolatedInstance();
        
        /*
         * Note: the WeakValueCache does not let you replace an existing entry
         * so we first remove the old entry under the key if there is one.
         */
        if (replace) {

            indexCache.remove(name);

        }
        
        // add name -> btree mapping to the transient cache.
        indexCache.put(name, btree, true/*dirty*/);

    }
    
    /**
     * Return the current entry, if any, for the named {@link ITx#UNISOLATED}
     * index in the {@link #indexCache}.
     * <p>
     * Note: This method is more direct than {@link #getIndex(String)}.
     * {@link AbstractTask} uses this method together with
     * {@link #putIndexCache(String, BTree, boolean)} to allow different tasks
     * access to the same pool of {@link ITx#UNISOLATED} indices.
     * 
     * @param name
     *            The index name.
     *            
     * @return The index iff it was found in the cache.
     */
    synchronized protected BTree getIndexCache(String name) {
        
        assertUnisolatedInstance();
        
        return indexCache.get(name);
        
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
    synchronized public void dropIndex(String name) {

        assertUnisolatedInstance();

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

        }

        /*
         * Remove the entry from the persistent index. After a commit you will
         * no longer be able to find the metadata record for this index from the
         * current commit record (it will still exist of course in historical
         * commit records).
         */
        super.remove(key);

        // report event.
        ResourceManager.dropUnisolatedBTree(name);

    }
    
    /**
     * Return a counter set reflecting the named indices that are currently open
     * (more accurately, those open named indices whose references are in
     * {@link Name2Addr}s internal {@link #indexCache}). When index partitions
     * are in use their {@link CounterSet}s are reported under a path formed
     * from name of the scale-out index and partition identifier. Otherwise the
     * {@link CounterSet}s are reported directly under the index name.
     * 
     * @return A new {@link CounterSet} reflecting the named indices that were
     *         open as of the time that this method was invoked.
     */
    public CounterSet getNamedIndexCounters() {

        assertUnisolatedInstance();

        final CounterSet tmp = new CounterSet();
        
        final Iterator<ICacheEntry<String, BTree>> itr = indexCache
                .entryIterator();

        while (itr.hasNext()) {

            final ICacheEntry<String, BTree> entry = itr.next();

            final String name = entry.getKey();

            final BTree btree = entry.getObject();

            final IndexMetadata md = btree.getIndexMetadata();

            LocalPartitionMetadata pmd = md.getPartitionMetadata();

            final String path;
            if (pmd != null) {

                // Note: [name] already includes the partition identifier.
                path = md.getName() + ICounterSet.pathSeparator + name;

            } else {

                path = name;

            }

            tmp.makePath(path).attach(btree.getCounters());

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
