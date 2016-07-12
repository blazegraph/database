/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.util.Bytes;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.concurrent.ExecutionExceptions;

import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

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
 * tasks running concurrent with commit processing, {@link AbstractTask} isolates
 * {@link Name2Addr} and makes the set of changes {registering indices, dropping
 * indices, and updating the {@link Entry} in {@link Name2Addr} to reflect the
 * current {@link Checkpoint} record for an index) an atomic state change that
 * is performed IFF the task completes successfully and is synchronized on
 * {@link Name2Addr} to prevent that happening concurrent with commit
 * processing.
 * </p>
 */
public class Name2Addr extends BTree {

    private static final Logger log = Logger.getLogger(Name2Addr.class);

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
    private ConcurrentWeakValueCache<String, ICheckpointProtocol> indexCache = null;

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
     */
    private class DirtyListener implements IDirtyListener, Comparable<DirtyListener> {
        
        final String name;
        final ICheckpointProtocol btree;
        boolean needsCheckpoint;
        long checkpointAddr = 0L;
        
        @Override
        public String toString() {
            
            return "DirtyListener{name="
                    + name
                    + ","
                    + (needsCheckpoint ? "needsCheckpoint" : "checkpointAddr="
                            + checkpointAddr) + "}";
            
        }
        
        private DirtyListener(final String name,
                final ICheckpointProtocol btree, final boolean needsCheckpoint) {
            
            assert name!=null;
            
            assert btree!=null;
            
            this.name = name;
            
            this.btree = btree;
            
            this.needsCheckpoint = needsCheckpoint;
            
            if(!needsCheckpoint) {
                
                /*
                 * Grab the checkpointAddr for the object.
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
        @Override
        public void dirtyEvent(final ICheckpointProtocol btree) {

            assert btree == this.btree;

            @SuppressWarnings("unused")
            final boolean added;
            
            synchronized(Name2Addr.this) {
                
                final ICheckpointProtocol cached = indexCache.get(name);

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

                added = commitList.putIfAbsent(name, this) != null;

                needsCheckpoint = true;
                
                checkpointAddr = 0L;
                    
                if(log.isInfoEnabled()) {

                    /*
                     * Note: The size of the commit list can appear to increment
                     * by more than one if there are concurrent writes on
                     * different indices (e.g., if this log message is written
                     * outside of the synchronized block).
                     */
                    
                    log.info("name=" + name + ", commitListSize="
                            + commitList.size() + ", file="
                            + getStore().getFile());
                    
                }

            } // synchronized.
            
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
    static public Name2Addr create(final IRawStore store) {
    
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBTreeClassName(Name2Addr.class.getName());

        /*
         * TODO configure unicode sort key behavior explicitly?
         * 
         * Note: This only applies to new Name2Addr objects. However, historical
         * Name2Addr objects were created using the default collator for the
         * platform. When the ICU library is available, that is the default.
         */

        final Properties p = new Properties();

//        p.setProperty(KeyBuilder.Options.COLLATOR, CollatorEnum.ASCII.name());

        p.setProperty(KeyBuilder.Options.STRENGTH,
                StrengthEnum.Identical.name());

        metadata.setTupleSerializer(new Name2AddrTupleSerializer(
                new DefaultKeyBuilderFactory(p)));

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
    public Name2Addr(final IRawStore store, final Checkpoint checkpoint,
            final IndexMetadata metadata, final boolean readOnly) {

        super(store, checkpoint, metadata, readOnly);
        
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
    final protected void assertUnisolatedInstance() {
        
        if(indexCache == null) {
            
            throw new IllegalStateException();
            
        }
        
    }
    
    /**
     * Return <code>true</code> iff this is the {@link ITx#UNISOLATED}
     * {@link Name2Addr} instance.
     */
    final protected boolean isUnisolatedInstance() {

        return indexCache != null;
        
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
     * @param cacheTimeout
     *            The timeout in milliseconds for stale entries in the cache.
     * 
     * @see Options#LIVE_INDEX_CACHE_CAPACITY
     * @see Options#LIVE_INDEX_CACHE_TIMEOUT
     */
    protected void setupCache(final int cacheCapacity, final long cacheTimeout) {

        if (indexCache != null) {

            // Cache was already configured.
            
            throw new IllegalStateException();

        }

        // indexCache = new WeakValueCache<String, BTree>(
        // new LRUCache<String, BTree>(cacheCapacity));

        indexCache = new ConcurrentWeakValueCacheWithTimeout<String, ICheckpointProtocol>(
                cacheCapacity, TimeUnit.MILLISECONDS.toNanos(cacheTimeout));

    }
    
    /**
     * An iterator that visits the entries in the internal {@link #indexCache}.
     * You must test the weak reference for each entry in order to determine
     * whether its value has been cleared as of the moment that you request that
     * value.
     * 
     * @throws IllegalStateException
     *             unless this is the {@link ITx#UNISOLATED} instance.
     */
    private Iterator<java.util.Map.Entry<String, WeakReference<ICheckpointProtocol>>> indexCacheEntryIterator() {

        assertUnisolatedInstance();
        
        return indexCache.entryIterator();
    
    }
    
    /**
     * Return the approximate number of indices in the live index cache.
     * 
     * @throws IllegalStateException
     *             unless this is the {@link ITx#UNISOLATED} instance.
     */
    public int getIndexCacheSize() {
    
        assertUnisolatedInstance();
        
        return indexCache.size();
    
    }
    
    /**
     * Return <code>true</code> iff the named index is on the commit list.
     * <p>
     * Note: This is synchronized even through the commitList is thread-safe in
     * order to make the test atomic with respect to {@link #handleCommit(long)}.
     * 
     * @param name
     *            The index name.
     * 
     * @throws IllegalStateException
     *             unless this is the {@link ITx#UNISOLATED} instance.
     */
    synchronized public boolean willCommit(final String name) {

        assertUnisolatedInstance();

        return commitList.containsKey(name);
        
    }
    
    /**
     * Flush a dirty index to the disk. This is used to flush each dirty index
     * in parallel, providing increased IO throughput and reduced latency during
     * the commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/675"
     *      >Flush indices in parallel during checkpoint to reduce IO
     *      latency</a>
     */
    private static class CommitIndexTask implements Callable<CommitIndexTask> {
     
        private final DirtyListener l;
        
        private final long commitTime;
        
        private final AtomicLong checkpointAddr = new AtomicLong(0L);

        /**
         * Return the address of the {@link Checkpoint} record.
         */
        public long getCheckpointAddr() {
        
            return checkpointAddr.get();
            
        }

        /**
         * 
         * @param l
         *            The {@link DirtyListener}.
         * @param commitTime
         *            The commitTime associated with the commitPoint.
         */
        public CommitIndexTask(final DirtyListener l, final long commitTime) {

            if (l == null)
                throw new IllegalArgumentException();
            
            this.l = l;
            
            this.commitTime = commitTime;
            
        }

        /**
         * @return <i>self</i>
         */
        @Override
        public CommitIndexTask call() throws Exception {

            if (log.isInfoEnabled())
                log.info("Will commit: " + l.name);

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
                            + l.name, t);

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
            
            // publish the checkpoint address.
            this.checkpointAddr.set(checkpointAddr);
            
            // Done.
            return this;
            
        }
        
    } // CommitIndexTask
    
    /**
     * Commit processing for named indices.
     * <p>
     * This method applies the {@link #commitList} and then flushes the backing
     * {@link ICheckpointProtocol} object to the store. The {@link #commitList}
     * consists of {@link DirtyListener}s. If the listener has its
     * {@link DirtyListener#needsCheckpoint} flag set, then the
     * {@link ICheckpointProtocol} implementation to which that listener is
     * attached will have its {@link BTree#writeCheckpoint() checkpoint written}
     * . Otherwise the current {@link Checkpoint} address is recovered. Either
     * way, the {@link Entry} in {@link Name2Addr}s backing {@link BTree} is
     * updated to reflect the <i>commitTime</i> and {@link Checkpoint} address
     * for the index.
     * <p>
     * Finally {@link Name2Addr} {@link Checkpoint}s itself using
     * {@link ICommitter#handleCommit(long)} and returns the address from which
     * {@link Name2Addr} may be reloaded.
     * <p>
     * Note: The {@link #commitList} MUST be protected against concurrent
     * modification during the commit otherwise concurrent tasks could be
     * reporting dirty objects while we are doing a commit and those notices
     * would be lost. Persistence capable objects ({@link ICheckpointProtocol}
     * implementations) get onto the {@link #commitList} via the
     * {@link DirtyListener}, so it is also synchronized.
     * <p>
     * Note: {@link Name2Addr} DOES NOT obtain a resource lock on the
     * {@link ICheckpointProtocol} implementation. Therefore it MUST NOT
     * checkpoint an index on which an {@link AbstractTask} has obtained a
     * resource lock. Otherwise we have concurrent writers on the {@link BTree}
     * and the {@link BTree} is not thread-safe for concurrent writers. Instead,
     * the {@link AbstractTask} checkpoints the {@link BTree} itself while it is
     * holding the resource lock and then sets
     * {@link DirtyListener#needsCheckpoint} to <code>false</code> using
     * {@link #putOnCommitList(String, ICheckpointProtocol, boolean)} as an
     * indication to {@link Name2Addr} that it MUST persist the current
     * checkpointAddr for the {@link BTree} on its next commit (and MUST NOT
     * write on the index when it does that commit).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/675"
     *      >Flush indices in parallel during checkpoint to reduce IO
     *      latency</a>
     */
    @Override
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
        
        if (log.isInfoEnabled()) {
            
            log.info("Store file="+getStore().getFile());
            
            log.info("There are " + a.length + " dirty indices : "
                    + Arrays.toString(a));
            
        }

        // for each entry in the snapshot of the commit list.
        final List<CommitIndexTask> tasks = new ArrayList<CommitIndexTask>(a.length);
        for (int i = 0; i < a.length; i++) {
            
            final DirtyListener l = a[i];
            
            if (log.isInfoEnabled())
                log.info("Will commit: " + l.name);
            
            tasks.add(new CommitIndexTask(l, commitTime));

        }

        /*
         * Submit checkpoint tasks in parallel.
         * 
         * Note: This relies on getStore() providing access to the IIndexManager
         * interface.
         */
        final List<Future<CommitIndexTask>> futures;
        try {

            final ExecutorService executorService = ((IIndexManager) getStore())
                    .getExecutorService();
            
            /*
             * Invoke tasks.
             * 
             * Note: Blocks until all tasks are done. Hence we do NOT have to
             * cancel these Futures. If we obtain them, then they are already
             * done.
             */
            futures = executorService.invokeAll(tasks);
            
        } catch (InterruptedException e) {
            
            // Interrupted while awaiting checkpoint(s).
            throw new RuntimeException(e);
            
        }
        
        // for each entry in the snapshot of the commit list.
        final List<Throwable> causes = new LinkedList<Throwable>();
        for (Future<CommitIndexTask> f : futures) {
            
            try {
                
                final CommitIndexTask task = f.get();
                
                final DirtyListener l = task.l;
                
                final long checkpointAddr = task.getCheckpointAddr();
                
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
                
            } catch (InterruptedException e) {

                log.error("l.name: " + e, e);
                causes.add(e);
                
            } catch (ExecutionException e) {
                
                log.error("l.name: " + e, e);
                causes.add(e);
                
            }
            
//            if (l.needsCheckpoint) {
//
//                /*
//                 * Note: AbstractTask flags [needsCheckpoint := false] on the
//                 * DirtyListener and handles the BTree checkpoint itself in
//                 * order to avoid the possibility of a concurrent modification
//                 * by this code during commit processing.
//                 */
//                
//                try {
//
//                    // checkpoint the index.
//                    checkpointAddr = l.btree.handleCommit(commitTime);
//
//                    // we just did the checkpoint.
//                    l.needsCheckpoint = false;
//
//                } catch (Throwable t) {
//
//                    // adds the name to the stack trace.
//                    throw new RuntimeException("Could not commit index: name="
//                            + l.name + ", commitList=" + Arrays.toString(a), t);
//
//                }
//                
//            } else {
//
//                /*
//                 * Note: AbstractTask avoids concurrent modification of the
//                 * BTree checkpoint record during a commit by synchronizing on
//                 * Name2Addr.
//                 * 
//                 * Note: The DirtyListener grabs the current checkpointAddr from
//                 * the BTree when [needsCheckpoint := false]. This allows us to
//                 * have asynchronous checkpoints of the BTree (by concurrent
//                 * tasks) without causing the checkpoint address on the
//                 * commitList to be advanced until the next atomic transfer of
//                 * state from a completed AbstractTask down to Name2Addr.
//                 * (Without this, the checkpointAddr on the BTree winds up
//                 * updated concurrently but we only notice the problem when it
//                 * happens to be 0L because the BTree is in the midst of
//                 * updating its checkpoint!)
//                 */
//                
//                // use last recorded checkpoint.
//                checkpointAddr = l.checkpointAddr;
//
//                if (checkpointAddr == 0L) {
//
//                    throw new RuntimeException(
//                            "Checkpoint address not written: name=" + l.name);
//                    
//                }
//                                
//            }
//            
//            // set commitTime on the btree (transient field).
//            l.btree.setLastCommitTime(commitTime);
            
        } // next Future.
        
        /*
         * If there were any errors, then throw an exception listing them.
         */
        if (!causes.isEmpty()) {
            // Throw exception back to the leader.
            if (causes.size() == 1)
                throw new RuntimeException(causes.get(0));
            throw new RuntimeException("nerrors=" + causes.size(),
                    new ExecutionExceptions(causes));
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
    private byte[] getKey(final String name) {

        final byte[] a = metadata.getTupleSerializer().serializeKey(name);

//        log.error("name=" + name + ", key=" + BytesUtil.toString(a));

        return a;

//        return KeyBuilder.asSortKey(name);

    }

    /**
     * Return the {@link ITx#UNISOLATED} view of the named persistence capable
     * data structure - this method tests a cache of the named persistence
     * capable data structures and will return the existing instance if the
     * index is found in the cache and will otherwise load the
     * {@link ITx#UNISOLATED} view of the data structure from the backing store.
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
    public ICheckpointProtocol getIndex(final String name) {

        assertUnisolatedInstance();

        if (name == null) {

            throw new IllegalArgumentException();
            
        }

        ICheckpointProtocol ndx;
        synchronized(this) {
        
            /*
             * Note: Synchronized since some operations (remove+add) are not
             * otherwise atomic.
             */

            ndx = indexCache.get(name);

        }

        if (ndx != null) {

            if (ndx.getDirtyListener() == null) {

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
            assert ((DirtyListener)ndx.getDirtyListener()).getName2Addr() == this;

            return ndx;

        }

        final byte[] val = super.lookup(getKey(name));

        if (val == null) {

            return null;
            
        }
        
        // deserialize entry.
//        final Entry entry = EntrySerializer.INSTANCE.deserialize(new DataInputBuffer(val));
        final Entry entry = EntrySerializer.INSTANCE
                .deserialize(new DataInputStream(new ByteArrayInputStream(val)));

        // Load from the backing store.
        ndx = Checkpoint.loadFromCheckpoint(store, entry.checkpointAddr,
                false/* readOnly */);

        // Set the lastCommitTime on the index.
        ndx.setLastCommitTime(entry.commitTime);
        
        // Save name -> btree mapping in transient cache.
        putIndexCache(name, ndx, false/*replace*/);
        
        // listen for dirty events so that we know when to add this to the commit list.
        final DirtyListener l = new DirtyListener(name, ndx, false/* needsCheckpoint */);
        
        ndx.setDirtyListener( l );
        
        // report event (loaded btree).
        ResourceManager.openUnisolatedIndex(name);

        // return btree.
        return ndx;

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
    public Entry getEntry(final String name) {

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
    synchronized public void registerIndex(final String name,
            final ICheckpointProtocol btree) {

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
        ResourceManager.openUnisolatedIndex(name);
        
    }

    /**
     * Adds the named index to the commit list and sets a {@link DirtyListener}
     * on the {@link ICheckpointProtocol} so that this {@link Name2Addr} object
     * will be informed if the associated persistent data structure becomes
     * dirty.
     * 
     * @param name
     *            The index name.
     * @param btree
     *            The persistence capable data structure.
     * @param needsCheckpoint
     *            Specify <code>true</code> if {@link Name2Addr} should invoke
     *            {@link ICheckpointProtocol#writeCheckpoint()} rather than just
     *            updating the {@link Entry} for the persistenc capable data
     *            structure using {@link ICheckpointProtocol#getCheckpoint()}
     */
    synchronized protected void putOnCommitList(final String name,
            final ICheckpointProtocol btree, final boolean needsCheckpoint) {

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
     
        if(log.isInfoEnabled()) {
            
            log.info("name=" + name + ", commitListSize=" + commitList.size()
                    + ", needsCheckpoint=" + needsCheckpoint + ", file="
                    + getStore().getFile());
            
        }
        
    }

    /**
     * Adds the named index to the {@link ITx#UNISOLATED} index cache.
     * 
     * @param name
     *            The index name.
     * @param btree
     *            The {@link ITx#UNISOLATED} view of the persistence capable
     *            data structure.
     * @param replace
     *            If an existing entry for that name may be replaced.
     */
    synchronized protected void putIndexCache(final String name,
            final ICheckpointProtocol btree, final boolean replace) {

        assertUnisolatedInstance();
        
//        /*
//         * Note: the WeakValueCache does not let you replace an existing entry
//         * so we first remove the old entry under the key if there is one.
//         */
//        if (replace) {
//
//            indexCache.remove(name);
//
//        }
//        
//        // add name -> btree mapping to the transient cache.
//        indexCache.put(name, btree, true/*dirty*/);

        if (replace) {

            indexCache.put(name, btree);
            
        } else {
            
            indexCache.putIfAbsent(name, btree);
            
        }

    }
    
    /**
     * Return the current entry, if any, for the named {@link ITx#UNISOLATED}
     * index in the {@link #indexCache}.
     * <p>
     * Note: This method is more direct than {@link #getIndex(String)}.
     * {@link AbstractTask} uses this method together with
     * {@link #putIndexCache(String, ICheckpointProtocol, boolean)} to allow
     * different tasks access to the same pool of {@link ITx#UNISOLATED}
     * indices.
     * 
     * @param name
     *            The index name.
     * 
     * @return The index iff it was found in the cache.
     */
    synchronized protected ICheckpointProtocol getIndexCache(final String name) {

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
    synchronized public void dropIndex(final String name) {

        assertUnisolatedInstance();

        if (name == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("name=" + name);
        
        final byte[] key = getKey(name);
        
        if(!super.contains(key)) {
            
            throw new NoSuchIndexException("Not registered: "+name);
            
        }
        
        // remove the name -> btree mapping from the transient cache.
        final ICommitter btree = indexCache.remove(name);
        
        if (btree != null) {

            /*
             * Make sure that the index is not on the commit list.
             * 
             * Note: If the index is not in the index cache then it WILL NOT be
             * in the commit list.
             */
            commitList.remove(name);
            
            // clear our listener.
            ((ICheckpointProtocol) btree).setDirtyListener(null);

        }

        /*
         * Remove the entry from the persistent index. After a commit you will
         * no longer be able to find the metadata record for this index from the
         * current commit record (it will still exist of course in historical
         * commit records).
         */
        super.remove(key);

        // report event.
        ResourceManager.dropUnisolatedIndex(name);

    }
    
    @Override
    public void invalidate(final Throwable t) {

        if (t == null)
            throw new IllegalArgumentException();

        final Iterator<java.util.Map.Entry<String, WeakReference<ICheckpointProtocol>>> itr = indexCacheEntryIterator();

        while (itr.hasNext()) {

            final java.util.Map.Entry<String, WeakReference<ICheckpointProtocol>> e = itr.next();

            final ICheckpointProtocol chk = e.getValue().get();

            if (chk != null) {

                chk.invalidate(t);

            }
            
        }
        
        // Clear references. They are all invalid.
        indexCache.clear();
        
    }

    /**
     * Return a {@link CounterSet} reflecting the named indices that are
     * currently open (more accurately, those open named indices whose
     * references are in {@link Name2Addr}s internal {@link #indexCache}). When
     * index partitions are in use their {@link CounterSet}s are reported under
     * a path formed from name of the scale-out index and partition identifier.
     * Otherwise the {@link CounterSet}s are reported directly under the index
     * name.
     * 
     * @param counterSet
     *            When non-<code>null</code> the performance counters are
     *            entered into the caller's collection. Otherwise they are
     *            entered into a new collection.
     * @param found
     *            When non-<code>null</code>, the names of the indices whose
     *            performance counters are being returned is reported as a
     *            side-effect on this {@link Set}.
     * 
     * @return A new {@link CounterSet} reflecting the named indices that were
     *         open as of the time that this method was invoked.
     * 
     * @see IndexManager#getIndexCounters()
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/626">
     *      Expose performance counters for read-only indices </a>
     */
    protected CounterSet getIndexCounters(final CounterSet counterSet,
            final Set<String/* Name */> found) {

        assertUnisolatedInstance();

        final CounterSet tmp = counterSet == null ? new CounterSet()
                : counterSet;
        
        final Iterator<java.util.Map.Entry<String, WeakReference<ICheckpointProtocol>>> itr = indexCacheEntryIterator();

        while (itr.hasNext()) {

            final java.util.Map.Entry<String, WeakReference<ICheckpointProtocol>> entry = itr.next();

            final String name = entry.getKey();

            final ICheckpointProtocol btree = entry.getValue().get();

            if (btree == null) {

                // Note: Weak reference has been cleared.
                
                continue;
                
            }
            
            final IndexMetadata md = btree.getIndexMetadata();

            final LocalPartitionMetadata pmd = md.getPartitionMetadata();

            final String path;
            if (pmd != null) {

                // Note: [name] already includes the partition identifier.
                path = md.getName() + ICounterSet.pathSeparator + name;

            } else {

                path = name;

            }

            /*
             * Attach the B+Tree performance counters.
             * 
             * Note: These counters MUST NOT embed a hard reference to the
             * AbstractBTree. That could cause the BTree to be retained as long
             * as the caller holds the CounterSet object!
             */

            tmp.makePath(path).attach(btree.getCounters());

            if (found != null) {
             
                /*
                 * Report out the names of the indices whose counters are being
                 * returned.
                 */

                found.add(name);
                
            }
            
        }
        
        return tmp;

    }
    
    /**
     * An entry in the persistent index.
     * <p>
     * The {@link Entry} reports the {@link #name} of the index, its
     * {@link #checkpointAddr}, and the {@link #commitTime} when it was last
     * updated. If you want to know more about the index, then you need to open
     * it.
     * <p>
     * Do NOT open the index directly from the {@link #checkpointAddr}. That
     * will circumvent the canonical mapping imposed by the
     * {@link IIndexManager} on the open indices for a given name and commit
     * time. Instead, just ask the {@link IIndexManager} to open the index
     * having the specified {@link #name} as of the desired commit time.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
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

        public Entry(final String name, final long checkpointAddr,
                final long commitTime) {
            
            this.name = name;
            
            this.checkpointAddr = checkpointAddr;
            
            this.commitTime = commitTime;
            
        }
        
        @Override
        public String toString() {

            return "Entry{name=" + name + ",checkpointAddr=" + checkpointAddr
                    + ",commitTime=" + commitTime + "}";

        }
        
    }

    /**
     * The values are {@link Entry}s.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class EntrySerializer {

        public static transient final EntrySerializer INSTANCE = new EntrySerializer();

        private EntrySerializer() {

        }

        public byte[] serialize(final Entry entry) {

            try {

                // estimate capacity
                final int capacity = Bytes.SIZEOF_LONG + entry.name.length() * 2;
                
                final ByteArrayOutputStream baos = new ByteArrayOutputStream(capacity);
                
                final DataOutput os = new DataOutputStream(baos);

                os.writeLong(entry.commitTime);

                os.writeLong(entry.checkpointAddr);

                os.writeUTF(entry.name);
                
                return baos.toByteArray();

            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }

        }

        public Entry deserialize(final DataInput in) {

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

    /**
     * Encapsulates key and value formation for {@link Name2Addr}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static public class Name2AddrTupleSerializer extends
            DefaultTupleSerializer<String, Entry> {

        /**
         * 
         */
        private static final long serialVersionUID = 5699568938604974463L;
        
        /**
         * Used to (de-)serialize {@link Entry}s (NOT thread-safe).
         */
        private final EntrySerializer ser;
        
        /**
         * De-serialization ctor.
         */
        public Name2AddrTupleSerializer() {

            super();

            this.ser = EntrySerializer.INSTANCE;
            
        }

        /**
         * Ctor when creating a new instance.
         * 
         * @param keyBuilderFactory
         */
        public Name2AddrTupleSerializer(
                final IKeyBuilderFactory keyBuilderFactory) {
            
            super(keyBuilderFactory);
            
            this.ser = EntrySerializer.INSTANCE;

        }
        
        /**
         * Return the unsigned byte[] key for an index name.
         * 
         * @param obj
         *            The name of an index.
         */
        @Override
        public byte[] serializeKey(final Object obj) {

            final IKeyBuilder keyBuilder = getKeyBuilder();
            
            final byte[] a = keyBuilder.reset().append((String) obj).getKey();

//            log.error("name=" + obj + ", key=" + BytesUtil.toString(a)+", keyBuilder="+keyBuilder);
            
            return a;
            
        }
        
        /**
         * Return the byte[] value an {@link Entry}.
         * 
         * @param entry
         *            An Entry.
         */
        @Override
        public byte[] serializeVal(final Entry entry) {
            
            return ser.serialize(entry);

        }

        @SuppressWarnings("rawtypes")
        @Override
        public Entry deserialize(final ITuple tuple) {

            return ser.deserialize(tuple.getValueStream());

        }

        /**
         * The initial version (no additional persistent state).
         */
        private final static transient byte VERSION0 = 0;

        /**
         * The current version.
         */
        private final static transient byte VERSION = VERSION0;

        @Override
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            super.readExternal(in);
            
            final byte version = in.readByte();
            
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new UnsupportedOperationException("Unknown version: "
                        + version);
            }

        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            super.writeExternal(out);
            
            out.writeByte(VERSION);
            
        }

    } // Name2AddrTupleSerializer

    /**
     * Prefix scan of a {@link Name2Addr} index. This scan assumes that the
     * caller has provided for any possible thread-safety issues.
     * 
     * @param prefix
     *            The prefix.
     * @param n2a
     *            The index.
     * 
     * @return The names of the indices spanned by that prefix in that index.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/974" >
     *      Name2Addr.indexNameScan(prefix) uses scan + filter </a>
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/743">
     *      AbstractTripleStore.destroy() does not filter for correct prefix
     *      </a>
     */
    @SuppressWarnings("unchecked")
   public static final Iterator<String> indexNameScan(final String prefix,
            final IIndex n2a) {

        final byte[] fromKey;
        final byte[] toKey;
        final boolean hasPrefix = prefix != null && prefix.length() > 0;
//        final boolean restrictScan = true;

        if (hasPrefix ) //&& restrictScan) 
        {

            /*
             * When the namespace prefix was given, generate the toKey as the
             * fixed length successor of the fromKey.
             * 
             * Note: We MUST use StrengthEnum:=PRIMARY for the prefix scan in
             * order to avoid the secondary collation ordering effects.
             */

//            final IKeyBuilder keyBuilder = n2a.getIndexMetadata()
//                    .getTupleSerializer().getKeyBuilder();

//            final Properties properties = new Properties();
//
//            properties.setProperty(KeyBuilder.Options.STRENGTH,
//                    StrengthEnum.Primary.toString());
//
//            final IKeyBuilder keyBuilder = new DefaultKeyBuilderFactory(
//                    properties).getKeyBuilder();
            final IKeyBuilder keyBuilder = n2a.getIndexMetadata()
                    .getPrimaryKeyBuilder();
                    
            fromKey = keyBuilder.reset().append(prefix).getKey();

            toKey = SuccessorUtil.successor(fromKey.clone());

            if (log.isDebugEnabled()) {

                log.error("fromKey=" + BytesUtil.toString(fromKey));

                log.error("toKey  =" + BytesUtil.toString(toKey));

            }

        } else {

            // Do not restrict the scan.
            fromKey = null;
            toKey = null;

        }

        final ITupleIterator<Entry> itr = n2a.rangeIterator(fromKey, toKey);

        /*
         * Add resolver from the tuple to the name of the index.
         */
        final IStriterator sitr = new Striterator(itr).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            @Override
            protected Object resolve(Object obj) {

                return ((ITuple<Entry>) obj).getObject().name;

            }

        });

//        if (hasPrefix && !restrictScan) {
//
//            /*
//             * Only report the names that match the prefix.
//             * 
//             * Note: For the moment, the filter is hacked by examining the
//             * de-serialized Entry objects and only reporting those that start
//             * with the [prefix].
//             */
//            
//            sitr = sitr.addFilter(new Filter() {
//
//                private static final long serialVersionUID = 1L;
//
//                @Override
//                public boolean isValid(final Object obj) {
//                    
//                    final String name = (String) obj;
//
//                    if (name.startsWith(prefix)) {
//
//                        // acceptable.
//                        return true;
//                    }
//                    return false;
//                }
//            });
//
//        }

        return sitr;

    }

//    /**
//     * The SuccessorUtil does not work with CollatedKeys since it bumps the "meta/control" data
//     * at the end of the key, rather than the "value" data of the key.
//     * 
//     * It has been observed that the key data is delimited with a 01 byte, followed by meta/control
//     * data with the key itself delimited by a 00 byte.
//     * 
//     * Note that this has only been analyzed for the ICU collator, the standard Java collator does include 
//     * 00 bytes in the key.  However, it too appears to delimit the value key with a 01 byte so the
//     * same method should work.
//     * 
//     * @param src - original key
//     * @return the next key
//     */
//    private static byte[] successor(final byte[] src) {
//        final byte[] nxt = src.clone();
//        for (int i = 1; i < nxt.length; i++) {
//            if (nxt[i] == 01) { // end of data
//                nxt[i-1]++;
//                break;
//            }
//        }
//        
//        return nxt;
//    }
}
