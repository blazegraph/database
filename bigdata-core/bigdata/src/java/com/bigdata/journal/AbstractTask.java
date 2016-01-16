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
/*
 * Created on Oct 10, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.view.FusedView;
import com.bigdata.concurrent.NonBlockingLockManager;
import com.bigdata.counters.CounterSet;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.htree.AbstractHTree;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.NoSuchStoreException;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.TaskCounters;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class for tasks that may be submitted to the
 * {@link ConcurrencyManager}. Tasks may be isolated (by a transaction),
 * unisolated, read-committed, or historical reads. Tasks access named resources
 * (aka indices), which they pre-declare in their constructors.
 * <p>
 * A read-committed task runs against the most recently committed view of the
 * named index. A historical read task runs against a historical view of the
 * named index, but without guarantees of transactional isolation. Concurrent
 * readers are permitted without locking on the same index.
 * <p>
 * An unisolated task reads and writes on the "live" index. Note that only a
 * single thread may write on a {@link BTree} at a time. Therefore unisolated
 * tasks (often referred to as writers) obtain an exclusive lock on the named
 * index(s). When more than one named index is used, the locks are used to infer
 * a partial ordering of the writers allowing as much concurrency as possible.
 * Pre-declaration of locks allows us to avoid deadlocks in the lock system.
 * <p>
 * Isolated tasks are part of a larger transaction. Transactions are started and
 * committed using an {@link ITransactionManagerService}. Transactional tasks
 * run with full concurrency using an MVCC (Multi-Version Concurrency Control)
 * strategy. When a transaction is committed (by the
 * {@link ITransactionManagerService}) it must wait for lock(s) on the
 * unisolated named indices on which it has written before it may validate and
 * commit.
 * <p>
 * Note: You MUST submit a distinct instance of this task each time you
 * {@link ConcurrencyManager#submit(AbstractTask)} it.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractTask<T> implements Callable<T>, ITask<T> {

    static protected final Logger log = Logger.getLogger(AbstractTask.class);

    /**
     * Used to protect against re-submission of the same task object.
     */
    private final AtomicBoolean submitted = new AtomicBoolean(false);
    
    /**
     * The object used to manage exclusive access to the unisolated indices.
     */
    protected final ConcurrencyManager concurrencyManager;
    
    /**
     * The object used to manage local transactions.
     */
    protected final AbstractLocalTransactionManager transactionManager;
    
    /**
     * The object used to manage access to the resources from which views of the
     * indices are created.
     */
    protected final IResourceManager resourceManager;

    /**
     * Optionally verifies that the locks are held before/after the task
     * executes.
     * 
     * @todo The most common reason to see an exception when this is enabled
     *       that you did not submit the task to the concurrency manager and
     *       hence it was not processed by the lock manager. E.g., you invoked
     *       call() directly on some subclass of AbstractTask. That is a no-no,
     *       unless you are using the Journal as a personal (no concurrency)
     *       store. In that case it is Ok and some of the unit tests are written
     *       that way which is why this is turned off by default. In particular,
     *       it tends to interfere with some of the tx test suites since they
     *       were written before the concurrency API was added.
     */
    private static final boolean verifyLocks = false;
    
    /**
     * The object used to manage access to the resources from which views of the
     * indices are created.
     */
    @Override
    public final IResourceManager getResourceManager() {
        
        return resourceManager;
        
    }
    
    @Override
    synchronized public final IJournal getJournal() {

        if (journal == null) {

            journal = resourceManager.getJournal(timestamp);

            if (journal == null) {

                log.warn("No such journal: timestamp=" + timestamp);

                return null;
                
            }

            if (timestamp == ITx.UNISOLATED) {

                journal = new IsolatedActionJournal((AbstractJournal) journal);

            } else if (readOnly) {

                // disallow writes.
                journal = new ReadOnlyJournal((AbstractJournal) journal);
                
            }

        }

        return journal;

    }

    /** Guarded by <code>synchronized(AbstractTask)</code>. */
    private IJournal journal;
    
    /**
     * The transaction identifier -or- {@link ITx#UNISOLATED} if the operation
     * is NOT isolated by a transaction, -or- {@link ITx#READ_COMMITTED}, -or-
     * <code>timestamp</code> to read from the most recent commit point not
     * later than <i>timestamp</i>.
     */
    protected final long timestamp;

    /**
     * The timestamp of the group commit for an {@link ITx#UNISOLATED} task
     * which executes successfully and then iff the group commit succeeds.
     * Otherwise ZERO (0L).
     */
    long commitTime = 0L;
    
    /**
     * The timestamp of the group commit for an {@link ITx#UNISOLATED} task
     * which executes successfully and then iff the group commit succeeds.
     * Otherwise ZERO (0L).
     */
    public long getCommitTime() {

        return commitTime;
        
    }
    
    /**
     * True iff the operation is isolated by a transaction.
     */
    protected final boolean isReadWriteTx;

    /**
     * True iff the operation is not permitted to write.
     */
    protected final boolean readOnly;

    /**
     * The name of the resource(s) on which the task will operation (read,
     * write, add, drop, etc). This is typically the name of one or more
     * indices.
     * <p>
     * Note: When the operation is an unisolated writer an exclusive lock MUST
     * be obtain on the named resources(s) before the operation may execute.
     * <p>
     * Note: this is private so that people can not mess with the resource names
     * in {@link #doTask()}.
     */
    private final String[] resource;

    /**
     * The transaction object iff the operation is isolated by a transaction
     * and otherwise <code>null</code>. 
     */
    protected final Tx tx;
    
    /**
     * Cache of named indices resolved by this task for its {@link #timestamp}.
     * 
     * @see #getIndex(String name)
     * @see #getIndexLocal(String)
     */
    final private Map<String,ILocalBTreeView> indexCache;

    /**
     * Read-only copy of a {@link Name2Addr.Entry} with additional flags to
     * track whether an index has been registered or dropped by the task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * 
     * @todo are these bits (combined with the replacement of the {@link Entry}
     *       in {@link #n2a}) sufficiently flexible to allow a task to do a
     *       drop/add sequence, or a more general {add,drop}+ sequence.
     */
    static private class Entry extends Name2Addr.Entry {

       /** <code>true</code> iff the index is registered by the task. */
        boolean registeredIndex = false;
       
        /** <code>true</code> iff the index is dropped by the task. */
        boolean droppedIndex = false;
        
        Entry(final Name2Addr.Entry entry) {
            
            super(entry.name, entry.checkpointAddr, entry.commitTime);
            
        }

        /**
         * Ctor used when registering an index.
         * @param name
         * @param checkpointAddr
         * @param commitTime
         */
        Entry(final String name, final long checkpointAddr, final long commitTime) {
            
            super(name, checkpointAddr, commitTime);
            
            registeredIndex = true;
            
        }

    }
    
    /**
     * A map containing the {@link Entry}s for resources declared by
     * {@link ITx#UNISOLATED} tasks. The map is populated atomically by
     * {@link #setupIndices()} before the user task begins to execute.
     * <p>
     * There are several special cases designed to handle add and drop of named
     * indices in combination with transient flags on the {@link Entry}:
     * <ol>
     * 
     * <li>If an resource was declared by the task and there was no such index
     * in existence when the task began to execute, then there will NOT be an
     * {@link Entry} for the resource in this map.</li>
     * 
     * <li>If an existing index was dropped by the task, then then the resource
     * will be associated with a {@link Entry} whose [droppedIndex] flag is set
     * but the index will NOT be on the {@link #commitList}. When the task
     * checkpoints the indices, that [dropppedIndex] flag will be used to drop
     * the named index from the {@link ITx#UNISOLATED} {@link Name2Addr} object
     * (and it is an error if the named index does not exist in
     * {@link Name2Addr} at that time).</li>
     * 
     * <li>If an index is registered by a task, then either there MUST NOT be
     * an entry in {@link #n2a} as of the time that the task registers the index
     * -or- the entry MUST have the [droppedIndex] flag set. </li>
     * 
     * </ol>
     */
    private Map<String, Entry> n2a;

    /**
     * The commit list contains metadata for all indices that were made dirty by
     * an {@link ITx#UNISOLATED} task.
     */
    private ConcurrentHashMap<String, DirtyListener> commitList;

    /**
     * Atomic read of {@link Entry}s for declares resources into {@link #n2a}.
     * This is done before an {@link ITx#UNISOLATED} task executes. If the task
     * executes successfully then changes to {@link #n2a} are applied to the
     * unisolated {@link Name2Addr} object when the task completes. If the task
     * fails, then {@link #n2a} is simply discarded (since we have not written
     * on the unisolated {@link Name2Addr} we do not need to rollback any
     * changes).
     */
    private void setupIndices() {

        if (n2a != null)
            throw new IllegalStateException();
        
        n2a = new HashMap<String, Entry>(resource.length);

        /*
         * Note: getIndex() sets the listener on the BTree. That listener is
         * responsible for putting dirty indices onto the commit list.
         */
        commitList = new ConcurrentHashMap<String,DirtyListener>(resource.length);
        
        // the unisolated name2Addr object.
        final Name2Addr name2Addr = resourceManager.getLiveJournal()._getName2Addr();

        if (name2Addr == null) {
            
            /*
             * Note: I have seen name2Addr be [null] here on a system with too
             * many files open which caused a cascade of failures. I added this
             * thrown exception so that we could have a referent if the problem
             * shows up again.
             */

            throw new AssertionError("Name2Addr not loaded? : "
                    + resourceManager.getLiveJournal());
            
        }
        
        /*
         * Copy entries to provide an isolated view of name2Addr as of
         * the time that this task begins to execute.
         */
        synchronized(name2Addr) {
            
            for(String s : resource) {
                
                final Name2Addr.Entry tmp = name2Addr.getEntry(s);

                if (tmp != null) {
                
                    /*
                     * Exact match on a named index.
                     * 
                     * Add a read-only copy of the entry with additional state
                     * for tracking registration and dropping of named indices.
                     * 
                     * Note: We do NOT fetch the indices here, just copy their
                     * last checkpoint metadata from Name2Addr.
                     */
                    
                    n2a.put(s, new Entry(tmp));
                    
                } else {
                    
                    /**
                     * Add a read-only copy of the Name2Addr entry for all
                     * entries spanned by that namespace. This provides the
                     * additional state for tracking registration and dropping
                     * of named indices and also supports hierarchical locking
                     * pattersn.
                     * 
                     * Note: We do NOT fetch the indices here, just copy their
                     * last checkpoint metadata from Name2Addr.
                     * 
                     * @see <a
                     *      href="http://trac.blazegraph.com/ticket/566"
                     *      > Concurrent unisolated operations against multiple
                     *      KBs </a>
                     */
                    
                    final Iterator<String> itr = Name2Addr.indexNameScan(s,
                            name2Addr);

                    while (itr.hasNext()) {

                        final String t = itr.next();

                        final Name2Addr.Entry tmp2 = name2Addr.getEntry(t);

                        n2a.put(t, new Entry(tmp2));

                    }
                    
                }
                
            }
            
        }
        
    }
    
    /**
     * An instance of this {@link DirtyListener} is registered with each
     * {@link ITx#UNISOLATED} named index that we administer to listen for
     * events indicating that the index is dirty. When we get that event we
     * stick the {@link DirtyListener} on the {@link #commitList}. This makes
     * the commit protocol simpler since the {@link DirtyListener} has both the
     * name of the index and the reference to the index and we need both on hand
     * to do the commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private class DirtyListener implements IDirtyListener {
        
        private final String name;
        private final ICheckpointProtocol ndx;
        
        @Override
        public String toString() {
            
            return "DirtyListener{name="+name+"}";
            //return "DirtyListener{name="+name+", btree="+btree.getCheckpoint()+"}";
            
        }

        DirtyListener(final String name, final ICheckpointProtocol ndx) {
            
            assert name!=null;
            
            assert ndx!=null;
            
            this.name = name;
            
            this.ndx = ndx;
            
        }
        
        /**
         * Add <i>this</i> to the {@link AbstractTask#commitList}.
         * 
         * @param btree
         *            The index reporting that it is dirty.
         */
        @Override
        public void dirtyEvent(final ICheckpointProtocol btree) {

            assert btree == this.ndx;

            if (commitList.put(name, this) != null) {

                if (log.isInfoEnabled())
                    log.info("Added index to commit list: name=" + name);

            }
            
        }

    }

    /**
     * Release hard references to named indices to facilitate GC.
     */
    private void clearIndexCache() {

        if (log.isInfoEnabled())
            log.info("Clearing hard reference cache: " + indexCache.size()
                    + " indices accessed");
        
//        final Iterator<Map.Entry<String, ILocalBTreeView>> itr = indexCache
//                    .entrySet().iterator();
//
//        while (itr.hasNext()) {
//
//                final Map.Entry<String, ILocalBTreeView> entry = itr.next();
//
//                final String name = entry.getKey();
//
//                final ILocalBTreeView ndx = entry.getValue();
//
//                resourceManager.addIndexCounters(name, timestamp, ndx);
//        
//        }
        
        indexCache.clear();
        
        if (commitList != null) {

            /*
             * Clear the commit list so that we do not hold hard references.
             * 
             * Note: it is important to do this here since this code will be
             * executed even if the task fails so the commit list will always be
             * cleared and we will not hold hard references to indices accessed
             * by the task.
             */
            commitList.clear();
            
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: There are two ways in which a task may access an
     * {@link ITx#UNISOLATED} index, but in all cases access to the index is
     * delegated to this method. First, the task can use this method directly.
     * Second, the task can use {@link #getJournal()} and then use
     * {@link IJournal#getIndex(String)} on that journal, which is simply
     * delegated to this method. See {@link IsolatedActionJournal}.
     * 
     * @see http://trac.blazegraph.com/ticket/585 (GIST)
     */
    @Override
    synchronized final public ILocalBTreeView getIndex(final String name) {

        if (name == null)
            throw new NullPointerException();
        
        // validate that this is a declared index.
        assertResource(name);

        // verify still running.
        assertRunning();
        
        /*
         * Test the named index cache first.
         */
        {

            final ILocalBTreeView index = indexCache.get(name);

            if (index != null) {

                // Cached value.
                return index;

            }

        }

        if (timestamp == ITx.UNISOLATED) {

            final StaleLocatorReason reason = resourceManager.getIndexPartitionGone(name);
            
            if (reason != null) {

                throw new StaleLocatorException(name, reason);
                
            }
            
            // entry from isolated view of Name2Addr as of task startup (call()).
            final Entry entry = n2a.get(name);

            if (entry == null) {

                // index did not exist at that time.
                throw new NoSuchIndexException(name);

            }
            
            long checkpointAddr = entry.checkpointAddr;

            /*
             * Note: At this point we have an exclusive lock on the named
             * unisolated index. We recover the unisolated index from
             * Name2Addr's cache. If not found, then we load the unisolated
             * index from the store, set the [lastCommitTime], and enter it into
             * the unisolated Name2Addr's cache of unisolated indices.
             */
            ICheckpointProtocol ndx;
            
            // the unisolated name2Addr object.
            final Name2Addr name2Addr = resourceManager.getLiveJournal()
                    ._getName2Addr();

            synchronized (name2Addr) {

                /*
                 * RWStore: There are two reasons why we must use shadow
                 * allocations for unisolated operations against the RWStore.
                 * 
                 * (1) A rollback of an unisolated operation which caused
                 * mutations to the structure of an index will cause operations
                 * which access the index the rollback to fail since the backing
                 * allocations would have been immediately recycled by the
                 * RWStore (if running with a zero retention policy).
                 * 
                 * (2) Allocations made during the unisolated operation should
                 * be immediately recycled if the operation is rolled back. This
                 * will not occur unless the unisolated operation makes those
                 * allocations against a shadow allocation context. Given that
                 * it does so, the rollback logic must also discard the shadow
                 * allocator in order for the shadowed allocations to be
                 * reclaimed immediately.
                 * 
                 * In order to use shadow allocations, the unisolated index MUST
                 * be loaded using the IsolatedActionJournal. There are two
                 * places immediate below where it tests the cache and where it
                 * loads using the AbstractJournal, both of which are not
                 * appropriate as they fail to impose the IsolatedActionJournal
                 * with the consequence that the allocation contexts are not
                 * isolated. [Also, we do not want N2A to cache references to a
                 * B+Tree backed by a different shadow journal.]
                 */
                                
                if ((resourceManager.getLiveJournal().getBufferStrategy() instanceof IRWStrategy)) {
                    /*
                     * Note: Do NOT use the name2Addr cache for the RWStore.
                     * Each unisolated index view MUST be backed by a shadow
                     * journal!
                     * 
                     * But, fetch the btree from the cache to ensure we use the
                     * most recent checkpoint
                     */
                    ndx = null;

                    final ICheckpointProtocol tmp_ndx = name2Addr
                            .getIndexCache(name);

                    if (tmp_ndx != null) {

                        checkpointAddr = tmp_ndx.getCheckpoint()
                                .getCheckpointAddr();

                    }
                    
                } else {

                    // Recover from unisolated index cache.
                    ndx = name2Addr.getIndexCache(name);
                    
                }
                
                if (ndx == null) {

                    // wrap with the IsolatedActionJournal.
                    final IJournal tmp = getJournal();
//                  tmp = resourceManager.getLiveJournal();
                    
                    // re-load btree from the store.
                    ndx = Checkpoint.loadFromCheckpoint(//
                            tmp, // backing store.
                            checkpointAddr,//
                            false// readOnly
                            );

                    // set the lastCommitTime on the index.
                    ndx.setLastCommitTime(entry.commitTime);

                    // add to the unisolated index cache (must not exist).
                    name2Addr.putIndexCache(name, ndx, false/* replace */);

                    // set performance counters iff the class supports it.
                    if (ndx instanceof AbstractBTree) {
                        ((AbstractBTree) ndx).setBTreeCounters(resourceManager
                                .getIndexCounters(name));
                    } else if (ndx instanceof AbstractHTree) {
                        ((AbstractHTree) ndx).setBTreeCounters(resourceManager
                                .getIndexCounters(name));
                    }
                    
                }

            }

            try {

                // wrap B+Tree as FusedView: FIXME GIST : BTree specific code path.
                return getUnisolatedIndexView(name, (BTree) ndx);
                
            } catch (NoSuchStoreException ex) {
                
                /*
                 * Add a little more information to the stack trace.
                 */
                throw new NoSuchStoreException(entry.toString() + ":"
                        + ex.getMessage(), ex);
                
            }

        } else {

            final ILocalBTreeView tmp = resourceManager.getIndex(name, timestamp);

            if (tmp == null) {

                // Presume client has made a bad request
                throw new NoSuchIndexException(name + ", timestamp="
                        + timestamp);
                // return null; // @todo return null to conform with Journal#getIndex(name)?

            }

            /*
             * Put the index into a hard reference cache under its name so that
             * we can hold onto it for the duration of the operation.
             */

            indexCache.put(name, tmp);

            return tmp;

        }
        
    }
    
    /**
     * Given the name of an index and a {@link BTree}, obtain the view for all
     * source(s) described by the {@link BTree}s index partition metadata (if
     * any), inserts that view into the {@link #indexCache}, and return the
     * view.
     * <p>
     * Note: This method is used both when registering a new index (
     * {@link #registerIndex(String, BTree)}) and when reading an index view
     * from the source ({@link #getIndex(String)}).
     * 
     * @param name
     *            The index name.
     * @param btree
     *            The {@link BTree}.
     * 
     * @return The view.
     */
    private ILocalBTreeView getUnisolatedIndexView(final String name,
            final BTree btree) {

        // setup the task as the listener for dirty notices.
        btree.setDirtyListener(new DirtyListener(name, btree));

        // find all sources if a partitioned index.
        final AbstractBTree[] sources = resourceManager.getIndexSources(name,
                ITx.UNISOLATED, btree);

        assert !sources[0].isReadOnly();

        final ILocalBTreeView view;
        if (sources.length == 1) {

            view = (BTree) sources[0];

        } else {

            view = new FusedView(sources);

        }
        
        // put the index in our hard reference cache.
        indexCache.put(name, view);
        
        return view;
        
    }

    /**
     * Registers an index
     * 
     * @param name
     *            The index name.
     * @param btree
     *            The {@link BTree} that will absorb writes for the index.
     * 
     * @return The index on which writes may be made. Note that if the
     *         {@link BTree} describes an index partition with multiple sources
     *         then the returned object is a {@link FusedView} for that index
     *         partition as would be returned by
     *         {@link IResourceManager#getIndex(String, long)}.
     * 
     * @throws UnsupportedOperationException
     *             unless the task is {@link ITx#UNISOLATED}
     * @throws IndexExistsException
     *             if the index was already registered as of the time that this
     *             task began to execute.
     * 
     * @todo should allow add/drop of indices within fully isolated read-write
     *       transactions as well.
     * 
     * @see IBTreeManager#registerIndex(String, BTree)
     */
    synchronized public IIndex registerIndex(final String name, final BTree btree) {
        
        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();

        // task must be unisolated.
        assertUnisolated();
        
        // must be a declared resource.
        assertResource(name);

        // verify still running.
        assertRunning();

        if(n2a.containsKey(name)) {

            final Entry entry = n2a.get(name);
            
            if(!entry.droppedIndex) {
            
                throw new IndexExistsException(name);
                
            }
            
            // FALL THROUGH IFF INDEX WAS DROPPED BY THE TASK.
            
        }

        /*
         * Note: logic is parallel to that in Name2Addr.
         */
        
        // flush btree to the store to get the checkpoint record address.
        final long checkpointAddr = btree.writeCheckpoint();

        /*
         * Create new Entry.
         * 
         * Note: Entry has [registeredIndex := true].
         * 
         * Note: The commit time here is a placeholder. It will be replaced with
         * the actual commit time if the entry is propagated to Name2Addr and
         * Name2Addr does a commit.
         */
        
        final Entry entry = new Entry(name, checkpointAddr, 0L/* commitTime */);

        /*
         * Add entry to our isolated view (can replace an old [droppedIndex]
         * entry).
         */

        n2a.put(name, entry);

        /*
         * Make sure that the BTree is associated with the correct performance
         * counters.
         */

        btree.setBTreeCounters(resourceManager.getIndexCounters(name));

        /*
         * Note: delegate logic to materialize the view in case BTree is an
         * index partition with more than one source. This will also get the
         * index view into [indexCache].
         */

        final IIndex view = getUnisolatedIndexView(name, btree);
        
        /*
         * Verify that the caller's [btree] is part of the returned view.
         */
        if(view instanceof AbstractBTree) {
            
            assert btree == view;
            
        } else {
         
            assert btree == ((FusedView)view).getSources()[0];
            
        }
        
        /*
         * Fire the listener which will get the btree onto the commit list.
         * 
         * Note: We MUST put the newly registered BTree on the commit list even
         * if nothing else gets written on that BTree.
         */

        ((DirtyListener)btree.getDirtyListener()).dirtyEvent(btree);
                
        return view;
        
    }

    /**
     * Drops the named index.
     * 
     * @param name
     *            The name of the index.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     * @throws UnsupportedOperationException
     *             unless the task is {@link ITx#UNISOLATED}
     * @throws NoSuchIndexException
     *             if the named index is not registered as of the time that this
     *             task began to execute.
     *             
     * @see IIndexManager#dropIndex(String)
     */
    synchronized public void dropIndex(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        // task must be unisolated.
        assertUnisolated();
        
        // must be a declared resource.
        assertResource(name);

        // verify still running.
        assertRunning();

        if(!n2a.containsKey(name)) {

            throw new NoSuchIndexException(name);
            
        }

        // The entry for that index in our isolated view of name2Addr.
        final Entry entry = n2a.get(name);
        
        if(entry.droppedIndex) {
            
            // its already been dropped by the task.
            throw new NoSuchIndexException(name);
            
        }
        
        entry.droppedIndex = true;

        // clear from the index cache.
        indexCache.remove(name);

        // clear from the commit list.
        commitList.remove(name);
        
        /*
         * Note: Since the dropped indices are NOT on the commit list, when the
         * task is checkpointed we MUST scan n2a and then drop any dropped
         * indices without regard to whether they are on the commitList.
         */
        
    }

   /**
     * {@link Callable} checkpoints an index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     *         
     * @see <a href="http://trac.blazegraph.com/ticket/675" >
     *      Flush indices in parallel during checkpoint to reduce IO latency</a>
     */
    private class CheckpointIndexTask implements Callable<Void> {

        private final DirtyListener l;
        
        public CheckpointIndexTask(final DirtyListener l) {
        
            this.l = l;
            
        }

        @Override
        public Void call() throws Exception {

            if(log.isInfoEnabled())
                log.info("Writing checkpoint: "+l.name);
            
            try {

                l.ndx.writeCheckpoint();
                
            } catch (Throwable t) {
                
                // adds the name to the stack trace.
                throw new RuntimeException("Could not commit index: name="
                        + l.name, t);
                
            }

            // Done.
            return null;
      }

      @Override
      public String toString() {
         return getClass().getName() + "{name=" + l.name + "}";
      }
   
    }
    
    /**
     * Flushes any writes on unisolated indices touched by the task (those found
     * on the {@link #commitList}) and reconciles {@link #n2a} (our isolated
     * view of {@link Name2Addr}) with the {@link ITx#UNISOLATED}
     * {@link Name2Addr} object.
     * <p>
     * This method is invoked after an {@link ITx#UNISOLATED} task has
     * successfully completed its work, but while the task still has its locks.
     * 
     * @return The elapsed time in nanoseconds for this operation.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/675"
     *      >Flush indices in parallel during checkpoint to reduce IO
     *      latency</a>
     */
    private long checkpointTask() {

        assertUnisolated();
        
        assertRunning();
        
        /*
         * Checkpoint the dirty indices.
         */
        final int ndirty = commitList.size();
        
        final long begin = System.nanoTime();
        
        if (log.isInfoEnabled()) {

            log.info("There are " + ndirty + " dirty indices "
                    + commitList.keySet() + " : " + this);
                     
        }

        /*
         * Create task to checkpoint each entry in the snapshot of the commit
         * list.
         */
        final List<CheckpointIndexTask> tasks = new LinkedList<CheckpointIndexTask>();

        for (final DirtyListener l : commitList.values()) {

            assert indexCache.containsKey(l.name) : "Index not in cache? name="
                    + l.name;
        
            tasks.add(new CheckpointIndexTask(l));
            
        }
        
        /*
         * Submit checkpoint tasks in parallel.
         * 
         * This is done for each entry in the snapshot of the commit list.
         * 
         * Note: This relies on getResourceManager() providing access to the
         * IIndexManager interface.
         */
        final List<Future<Void>> futures;
        try {

            final ExecutorService executorService = resourceManager
                    .getLiveJournal().getExecutorService();
            
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

        /*
         * Check Futures.
         * 
         * Note: Per above, the Futures are done. We are just checking them for
         * errors.
         */
        for (Future<Void> f : futures) {

            try {

                f.get();
                
            } catch (InterruptedException e) {
                
                throw new RuntimeException(e);
                
            } catch (ExecutionException e) {
                
                throw new RuntimeException(e);
                
            }

        }

        if(log.isInfoEnabled()) { 

            final long elapsed = System.nanoTime() - begin;

            log.info("Flushed " + ndirty + " indices in "
                    + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
            
        }

        /*
         * Atomically apply changes to Name2Addr. When the next groupCommit
         * rolls around it will cause name2addr to run its commit protocol
         * (which will be a NOP since it SHOULD NOT be a dirty listener when
         * using the concurrency control) and then checkpoint itself, writing
         * the updated Entry records as part of its own index state. Finally the
         * AbstractJournal will record the checkpoint address of Name2Addr as
         * part of the commit record, etc. Until then, the changes written on
         * Name2Addr here will become visible as soon as we leave the
         * synchronized(name2addr) block.
         * 
         * Note: I've chosen to apply the changes to Name2Addr after the indices
         * have been checkpointed. This means that you will do all the IO for
         * the index checkpoints before you learn whether or not there is a
         * conflict with an add/drop for an index. However, the alternative is
         * to remain synchronized on [name2addr] during the index checkpoint,
         * which is overly constraining on concurrency. (If you validate first
         * but do not remained synchronized on name2addr then the validation can
         * be invalidated by concurrent tasks completing.)
         * 
         * However, unisolated tasks obtain exclusive locks for resources, so it
         * SHOULD NOT be possible to fail validation here. If validation does
         * fail (if the add/drop changes can not be propagated to name2addr)
         * then either the logic for add/drop using our isolated [n2a] is
         * busted, someone has gone around the concurrency control mechanisms to
         * access name2addr directly, or there is someplace where access to
         * name2addr is not synchronized.
         */

        // the unisolated name2Addr object.
        final Name2Addr name2Addr = resourceManager.getLiveJournal()._getName2Addr();

        synchronized(name2Addr) {

            for( final Entry entry : n2a.values() ) {

                if(entry.droppedIndex) {
                    
                    if (log.isInfoEnabled())
                        log.info("Dropping index on Name2Addr: " + entry.name);
                    
                    name2Addr.dropIndex(entry.name);
                    
                } else if(entry.registeredIndex) {

                    if (log.isInfoEnabled())
                        log.info("Registering index on Name2Addr: "
                                + entry.name);

                    name2Addr.registerIndex(entry.name, (BTree)commitList
                            .get(entry.name).ndx);

                } else {

                    final DirtyListener l = commitList.get(entry.name);

                    if (l != null) {
                     
                        /*
                         * Force the BTree onto name2addr's commit list.
                         * 
                         * Note: This will also change the dirty listener to be
                         * [Name2Addr].
                         * 
                         * Note: We checkpointed the index above. Therefore we
                         * mark it here as NOT needing to be checkpointed by
                         * Name2Addr. This flag is very important. Name2Addr
                         * DOES NOT acquire an exclusive lock on the unisolated
                         * index before attempting to checkpoint the index. The
                         * flag is set [false] here indicating to Name2Addr that
                         * it will write the current checkpoint record address
                         * on its next commit rather than attempting to
                         * checkpoint the index itself, which could lead to a
                         * concurrent modification problem.
                         */

                        if (log.isInfoEnabled())
                            log.info("Transferring to Name2Addr commitList: "
                                    + entry.name);

                        name2Addr
                                .putOnCommitList(l.name, l.ndx, false/*needsCheckpoint*/);

                    }
                    
                }
                
            }

        }

        /*
         * Do clean up.
         */

        // clear n2a.
        n2a.clear();
        
        // clear the commit list.
        commitList.clear();

        // Detach the allocation context used by the operation, but increment
        //  txCount
        
        ((IsolatedActionJournal) getJournal()).prepareCommit();
        
        final long elapsed = System.nanoTime() - begin;
        
        if (log.isInfoEnabled()) {

            log.info("End task checkpoint after "
                    + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
            
        }

        return elapsed;
        
    }

    /**
     * Discard any allocations for writes on unisolated indices touched by the
     * task for an {@link ITx#UNISOLATED} which fails, but while the task still
     * has its locks.
     */
    private void abortTask() {
        
        ((IsolatedActionJournal) getJournal()).abortContext();
        
    }

    /**
     * Hook invoked by group commit on success/failure. It is invoked from
     * within the group commit for each task which joins the commit group along
     * either the code path where the commit succeeds or the code path where it
     * fails. The boolean argument indicates whether or not the group commit
     * succeeded. Throws exceptions are trapped and logged.
     */
    void afterTaskHook(final boolean abort) {
        
        ((IsolatedActionJournal) getJournal()).completeTask();
        
    }
    
    /*
     * End isolation support for name2addr.
     */
    
    /**
     * Flag is cleared if the task is aborted.  This is used to refuse
     * access to resources for tasks that ignore interrupts.
     */
    volatile boolean aborted = false;
    
    /**
     * The {@link AbstractTask} increments various counters of interest to the
     * {@link ConcurrencyManager} using this object.
     */
    protected TaskCounters taskCounters;
    
    @Override
    public TaskCounters getTaskCounters() {
        
        return taskCounters;
        
    }
    
    /*
     * Timing data for this task.
     */
    
    /**
     * The time at which this task was submitted to the {@link ConcurrencyManager}.
     */
    public long nanoTime_submitTask;
    
    /**
     * The time at which this task was assigned to a worker thread for
     * execution.
     */
    public long nanoTime_assignedWorker;

    /**
     * The time at which this task began to do its work. If the task needs to
     * acquire exclusive resource locks, then this timestamp is set once those
     * locks have been acquired. Otherwise this timestamp will be very close to
     * the {@link #nanoTime_assignedWorker}.
     */
    public long nanoTime_beginWork;
    
    /**
     * The time at which this task finished its work. Tasks with write sets must
     * still do abort processing or await the next commit group.
     */
    public long nanoTime_finishedWork;
    
    /**
     * The elapsed time in nanoseconds for a write task to checkpoint its
     * index(s).
     */
    public long checkpointNanoTime;
    
    /**
     * Convenience constructor variant for one named resource.
     * 
     * @param concurrencyControl
     *            The object used to control access to the local resources.
     * @param timestamp
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - timestamp </code> to read from the most recent commit
     *            point not later than the absolute value of <i>timestamp</i> (a
     *            historical read).
     * @param resource
     *            The resource on which the task will operate. E.g., the names
     *            of the index. When the task is an unisolated write task an
     *            exclusive lock will be requested on the named resource and the
     *            task will NOT run until it has obtained that lock.
     *            <p>
     *            The name may identify either a namespace or a concrete index
     *            object. If a concrete index object is discovered, only that
     *            index is isolated. Otherwise all indices having the same
     *            prefix as the namespace are isolated.
     */
    protected AbstractTask(final IConcurrencyManager concurrencyManager,
            final long timestamp, final String resource) {

        this(concurrencyManager, timestamp, new String[] { resource });
        
    }
    
    /**
     * 
     * @param concurrencyControl
     *            The object used to control access to the local resources.
     * @param timestamp
     *            The transaction identifier, {@link ITx#UNISOLATED} for an
     *            unisolated view, {@link ITx#READ_COMMITTED} for a view as of
     *            the most recent commit point, or <code>timestamp</code> to
     *            read from the most recent commit point not later than that
     *            timestamp.
     * @param resource
     *            The resource(s) on which the task will operate. E.g., the
     *            names of the index(s). When the task is an unisolated write
     *            task an exclusive lock will be requested on each named
     *            resource and the task will NOT run until it has obtained those
     *            lock(s).
     *            <p>
     *            The name may identify either a namespace or a concrete index
     *            object. If a concrete index object is discovered, only that
     *            index is isolated. Otherwise all indices having the same
     *            prefix as the namespace are isolated.
     */
    protected AbstractTask(final IConcurrencyManager concurrencyManager,
            final long timestamp, final String[] resource) {

        if (concurrencyManager == null) {

            throw new NullPointerException();

        }

        if (resource == null) {

            throw new NullPointerException();

        }

        for (int i = 0; i < resource.length; i++) {

            if (resource[i] == null) {

                throw new NullPointerException();

            }
            
        }

        // make sure we have the real ConcurrencyManager for addCounters()
        this.concurrencyManager = (ConcurrencyManager) (concurrencyManager instanceof Journal ? ((Journal) concurrencyManager)
                .getConcurrencyManager()
                : concurrencyManager);

        this.transactionManager = (AbstractLocalTransactionManager) concurrencyManager
                .getTransactionManager();

        this.resourceManager = concurrencyManager.getResourceManager();
        
        this.timestamp = timestamp;

        /*
         * Note: A read-lock should be asserted for abs(timestamp) for the
         * duration of the task. This can be accomplished either by requesting
         * the necessary index views or by explicit handshaking with the
         * ResourceManager (no read locks are required for simple journals).
         * 
         * [This is currently handled by the weak value cache for clean indices
         * and the commit list for dirty indices].
         */

        this.resource = resource;
        
        this.indexCache = new HashMap<String,ILocalBTreeView>(resource.length);

        this.isReadWriteTx = TimestampUtility.isReadWriteTx(timestamp);
        
        this.readOnly = TimestampUtility.isReadOnly(timestamp);
        
        if (isReadWriteTx) {

            if (resourceManager instanceof ResourceManager) {
            
                /*
                 * This is a read-write transaction with a distributed database.
                 */

                // UUID of the dataService on which the tx will run.
                final UUID dataServiceUUID = ((ResourceManager) resourceManager)
                        .getDataServiceUUID();
                
                try {

                    /*
                     * Notify the transaction service. We send both the UUID of
                     * the dataService and the array of the named resources
                     * which the operation isolated by that transaction has
                     * requested. Transaction commits are placed into a partial
                     * order to avoid deadlocks where that ordered is determined
                     * by sorting the resources declared by the tx throughout its
                     * life cycle and the obtaining locks on all of those
                     * resources (in the distributed transaction service) before
                     * the commit may start. This is very similar to how we
                     * avoid deadlocks for unisolated operations running on a
                     * single journal or data service.
                     * 
                     * Note: Throws IllegalStateException if [timestamp] does
                     * not identify an active transaction.
                     * 
                     * FIXME In order to permit concurrent operations by the
                     * same transaction it MUST establish locks on the isolated
                     * resources.  Those resources MUST be locatable (they will
                     * exist on a temporary store choosen by the tx when it first
                     * started on this data service). See IsolatedActionJournal's 
                     * resource locator delegation pattern.
                     */

                    final IDistributedTransactionService txs = (IDistributedTransactionService) transactionManager
                            .getTransactionService();
                    
                    txs.declareResources(timestamp, dataServiceUUID, resource);

                } catch (IOException e) {

                    // RMI problem.
                    throw new RuntimeException(e);
                    
                }

                if (transactionManager.getTx(timestamp) == null) {

                    /**
                     * Start tx on this data service.
                     * 
                     * FIXME This should be passing the [readsOnCommitTime] into
                     * the Tx object. However, that information is not available
                     * when we are running on a cluster per the ticket below.
                     * While we COULD tunnel this when running tasks on a
                     * Journal, we tend not to do that. Tasks tend to be
                     * submitted primarily for the clustered database
                     * deployment.
                     * 
                     * @see http://trac.blazegraph.com/ticket/266
                     * (refactor native long tx id to thin object)
                     * 
                     * @see <a
                     * href="http://trac.blazegraph.com/ticket/546"
                     * > Add cache for access to historical index views on the
                     * Journal by name and commitTime. </a>
                     */
                    new Tx(transactionManager, resourceManager, timestamp,
                            timestamp/* readsOnCommitTime */);

                }

            }

            tx = transactionManager.getTx(timestamp); 
            
            if (tx == null) {
                
                throw new IllegalStateException("Unknown tx: "+timestamp);
                
            }

            tx.lock.lock();
            try {
                if (!tx.isActive()) {

                    throw new IllegalStateException("Not active: " + tx);

                }
            } finally {
                tx.lock.unlock();
            }
            
            taskCounters = this.concurrencyManager.countersTX;
            
        } else if (TimestampUtility.isReadOnly(timestamp)) {

            /*
             * A lightweight historical read.
             */

            tx = null;
            
            taskCounters = this.concurrencyManager.countersHR;
            
        } else {

            /*
             * Unisolated operation.
             */

            tx = null;

            taskCounters = this.concurrencyManager.countersUN;
            
        }

    }

    /**
     * The timestamp specified to the ctor. This effects which index checkpoints
     * are available to the task and whether the index(s) are read-only or
     * mutable.
     */
    public long getTimestamp() {
        
        return timestamp;
        
    }
    
    /**
     * Returns a copy of the array of resources declared to the constructor.
     */
    @Override
    public String[] getResource() {

        // clone so that people can not mess with the resource names.
        return resource.clone();

    }

    /**
     * Return the only declared resource.
     * 
     * @return The declared resource.
     * 
     * @exception IllegalStateException
     *                if more than one resource was declared.
     */
    @Override
    public String getOnlyResource() {
        
        if (resource.length > 1)
            throw new IllegalStateException("More than one resource was declared");
        
        return resource[0];
        
    }
    
    /**
     * Return <code>true</code> iff the task declared this as a resource.
     * 
     * @param theRequestedResource
     *            The name of a resource.
     * 
     * @return <code>true</code> iff <i>name</i> is a declared resource.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     */
    public boolean isResource(final String theRequestedResource) {

        if (theRequestedResource == null)
            throw new IllegalArgumentException();

        for (String theDeclaredResource : resource) {

            if (theDeclaredResource.equals(theRequestedResource)) {
                /*
                 * Exact match. This resource was declared.
                 */
                return true;
            }

            /**
             * Look for a prefix that spans one or more resources.
             * 
             * Note: Supporting this requires us to support efficient scans of
             * the indices in Name2Addr since getIndex(name) will fail if the
             * Name2Addr entry has not been buffered within the [n2a] cache.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/753" > HA
             *      doLocalAbort() should interrupt NSS requests and
             *      AbstractTasks </a>
             * @see <a href="http://trac.blazegraph.com/ticket/566" > Concurrent
             *      unisolated operations against multiple KBs </a>
             */
            if (theRequestedResource.startsWith(theDeclaredResource)) {
                
                // Possible prefix match.
                
                if (theRequestedResource.charAt(theDeclaredResource.length()) == '.') {

                    /*
                     * Prefix match.
                     * 
                     * E.g., name:="kb.spo.osp" and the task declared the
                     * resource "kb". In this case, "kb" is a PREFIX of the
                     * declared resource and the next character is the separator
                     * character for the resource names (this last point is
                     * important to avoid unintended contention between
                     * namespaces such as "kb" and "kb1").
                     */
                    return true;

                }

            }

        }

        return false;
        
    }
    
    /**
     * Asserts that the <i>resource</i> is one of the resource(s) declared to
     * the constructor. This is used to prevent tasks from accessing resources
     * that they did not declare (and on which they may not hold a lock).
     * 
     * @param resource
     *            A resource name.
     * 
     * @return The resource name.
     * 
     * @exception IllegalStateException
     *                if the <i>resource</i> was not declared to the
     *                constructor.
     */
    protected String assertResource(final String resource) {
        
        if (isResource(resource))
            return resource;

        throw new IllegalStateException("Not declared: task=" + getTaskName()
                + ", resource=" + resource + " is not in "
                + Arrays.toString(this.resource));
        
    }
    
    /**
     * Assert that the task is {@link ITx#UNISOLATED}.
     * 
     * @throws UnsupportedOperationException
     *             unless the task is {@link ITx#UNISOLATED}
     */
    protected void assertUnisolated() {

        if (timestamp == ITx.UNISOLATED) {

            return;
            
        }

        throw new UnsupportedOperationException("Task is not unisolated");

    }
    
    /**
     * Assert that the task is still running ({@link #aborted} is
     * <code>false</code>).
     * 
     * @throws RuntimeException
     *             wrapping an {@link InterruptedException} if the task has been
     *             interrupted.
     */
    protected void assertRunning() {
        
        if(aborted) {
            
            /*
             * The task has been interrupted by the write service which also
             * sets the [aborted] flag since the interrupt status may have been
             * cleared by looking at it.
             */

            throw new RuntimeException(new InterruptedException());
            
        }
        
    }
    
    /**
     * Returns Task{taskName,timestamp,elapsed,resource[]}
     */
    @Override
    public String toString() {
        
        return "Task{" + getTaskName() + ",timestamp="
                + TimestampUtility.toString(timestamp)+",resource="
                + Arrays.toString(resource) + "}";
        
    }
    
    /**
     * Returns the name of the class by default.
     */
    protected String getTaskName() {
        
        return getClass().getName();
        
    }
    
    /**
     * Implement the task behavior here.
     * <p>
     * Note: Long-running implementations MUST periodically test
     * {@link Thread#interrupted()} and MUST throw an exception, such as
     * {@link InterruptedException}, if they are interrupted. This behavior
     * allows tasks to be canceled in a timely manner.
     * <p>
     * If you ignore or fail to test {@link Thread#interrupted()} then your task
     * CAN NOT be aborted. If it is {@link Future#cancel(boolean)} with
     * <code>false</code> then the task will run to completion even though it
     * has been cancelled (but the {@link Future} will appear to have been
     * cancelled).
     * <p>
     * If you simply <code>return</code> rather than throwing an exception
     * then the {@link WriteExecutorService} will assume that your task
     * completed and your (partial) results will be made restart-safe at the
     * next commit!
     * 
     * @return The object that will be returned by {@link #call()} iff the
     *         operation succeeds.
     * 
     * @throws Exception
     *             The exception that will be thrown by {@link #call()} iff the
     *             operation fails.
     * 
     * @exception InterruptedException
     *                This exception SHOULD be thrown if
     *                {@link Thread#interrupted()} becomes true during
     *                execution.
     */
    abstract protected T doTask() throws Exception;

    /**
     * Adds the following fields to the {@link MDC} logging context:
     * <dl>
     * <dt>taskname</dt>
     * <dd>The name of the task as reported by {@link #getTaskName()}.</dd>
     * <dt>timestamp</dt>
     * <dd>The {@link #timestamp} specified to the ctor.</dd>
     * <dt>resources</dt>
     * <dd>The named resource(s) specified to the ctor IFF logging @ INFO or
     * above.</dd>
     * </dl>
     */
    protected void setupLoggingContext() {

        // Add to the logging context for the current thread.
            
        MDC.put("taskname", getTaskName());

        MDC.put("timestamp", Long.valueOf(timestamp));
        
        if(log.isInfoEnabled())
        MDC.put("resources", Arrays.toString(resource));
        
    }

    /**
     * Clear fields set by {@link #setupLoggingContext()} from the {@link MDC}
     * logging context.
     */
    protected void clearLoggingContext() {

        MDC.remove("taskname");

        MDC.remove("timestamp");

        if(log.isInfoEnabled())
        MDC.remove("resources");
        
    }
    
    /**
     * Delegates the task behavior to {@link #doTask()}.
     * <p>
     * For an unisolated operation, this method provides safe commit iff the
     * task succeeds and otherwise invokes abort() so that partial task
     * executions are properly discarded. When possible, the original exception
     * is re-thrown so that we do not encapsulate the cause unless it would
     * violate our throws clause.
     * <p>
     * Commit and abort are NOT invoked for an isolated operation regardless of
     * whether the operation succeeds or fails. It is the responsibility of the
     * "client" to commit or abort a transaction as it sees fit.
     * <p>
     * Note: Exceptions that are thrown from here will be wrapped as
     * {@link ExecutionException}s by the {@link ExecutorService}. Use
     * {@link InnerCause} to test for these exceptions.
     * 
     * @throws StaleLocatorException
     *             if the task requests an index partition which has been split,
     *             joined, or moved to another data service.
     * @throws NoSuchIndexException
     *             if the task requests an index that is not registered on the
     *             data service.
     * @throws InterruptedException
     *             can be thrown if the task is interrupted, for example while
     *             awaiting a lock, if the commit group is being discarded, or
     *             if the journal is being shutdown (which will cause the
     *             executor service running the task to be shutdown and thereby
     *             interrupt all running tasks).
     */
    @Override
    final public T call() throws Exception {
        
        try {

            /*
             * Increment by the amount of time that the task waited on the queue
             * before it began to execute.
             */
            
            final long waitingTime = (System.nanoTime() - nanoTime_submitTask);
            
            taskCounters.queueWaitingNanoTime.addAndGet(waitingTime);
            
            setupLoggingContext();
            
            final T ret = call2();
            
            clearLoggingContext();

            taskCounters.taskSuccessCount.incrementAndGet();

            return ret;

        } catch(Exception e) {
            
            /*
             * Note: covers RuntimeException, just not Throwable and the Error
             * hierarchy (which you are not supposed to catch).
             */
            
            taskCounters.taskFailCount.incrementAndGet();
            
            throw e;
            
        } finally {

            taskCounters.taskCompleteCount.incrementAndGet();
            
            // increment by the amount of time that the task was executing.
            taskCounters.serviceNanoTime.addAndGet(nanoTime_finishedWork
                    - nanoTime_beginWork);

            if (checkpointNanoTime != 0L) {

                /*
                 * Increment by the time required to checkpoint the indices
                 * written on by the task (IFF a write task and the task
                 * completes normally).
                 */
                taskCounters.checkpointNanoTime.addAndGet(checkpointNanoTime);

            }

            /*
             * Increment by the total time from submit to completion.
             * 
             * Note: This measure would not report the commit waiting time or
             * the commit service time. Therefore the [queuingNanoTime] is
             * special cased for the WriteExecutorService - see afterTask() on
             * that class.
             */
            if (timestamp != ITx.UNISOLATED) {

                taskCounters.queuingNanoTime.addAndGet(nanoTime_finishedWork
                        - nanoTime_submitTask);

            }
            
        }

    }

    final private T call2() throws Exception {

        nanoTime_assignedWorker = System.nanoTime();

        try {
            
            if (!submitted.compareAndSet(false, true)) {

                throw new ResubmitException(toString());
                
            }
            
            if (isReadWriteTx) {

                if (log.isInfoEnabled())
                    log.info("Running read-write tx: timestamp=" + timestamp);
                
//                if(tx.isReadOnly()) {
//
//                    try {
//
//                        nanoTime_beginWork = System.nanoTime();
//                        
//                        return doTask();
//
//                    } finally {
//
//                        nanoTime_finishedWork = System.nanoTime();
//                        
//                        // release hard references to named read-only indices.
//                        
//                        clearIndexCache();
//                        
//                    }
//
//                }
                
                /*
                 * Delegate handles handshaking for writable transactions.
                 */

                final Callable<T> delegate = new InnerReadWriteTxServiceCallable<T>(
                        this, tx);
                
                return delegate.call();

            }

            if (readOnly) {

                try {

                    nanoTime_beginWork = System.nanoTime();
                    
                    return doTask();
                    
                } finally {
                    
                    nanoTime_finishedWork = System.nanoTime();

                    // release hard references to the named read-only indices.
                    
                    clearIndexCache();
                
                    if(log.isInfoEnabled()) log.info("Reader is done: "+this);
                    
                }

            } else {

                /*
                 * Handle unisolated write tasks, which need to coordinate with
                 * the lock manager for the unisolated indices.
                 */

                assert timestamp == ITx.UNISOLATED : "timestamp="+timestamp;
                
                return doUnisolatedReadWriteTask();
                
            }

        } finally {

            if(log.isInfoEnabled()) log.info("done: "+this);

        }

    }
    
    /**
     * Call {@link #doTask()} for an unisolated write task.
     * 
     * @throws Exception
     */
    private T doUnisolatedReadWriteTask() throws Exception {
        
//        // lock manager.
//        final LockManager<String> lockManager = concurrencyManager.getWriteService().getLockManager();

        final Thread t = Thread.currentThread();
        
        if(log.isInfoEnabled())
            log.info("Unisolated write task: " + this + ", thread=" + t);

//        // declare resource(s) to lock (exclusive locks are used).
//        lockManager.addResource(resource);
//
//        // delegate will handle lock acquisition and invoke doTask().
//        final LockManagerTask<String,T> delegate = new LockManagerTask<String,T>(lockManager,
//                resource, new InnerWriteServiceCallable(this));
        
        final Callable<T> delegate = new InnerWriteServiceCallable<T>(this);
        
        final WriteExecutorService writeService = concurrencyManager
                .getWriteService();

        if (verifyLocks && resource.length > 0
                && writeService.getLockManager().getTaskWithLocks(resource) == null) {

            /*
             * Note: The most common reason for this exception is that you did
             * not submit the task to the concurrency manager and hence it was
             * not processed by the lock manager. E.g., you invoked call()
             * directly on some subclass of AbstractTask. That is a no-no.
             */
            
            throw new AssertionError("Task does not hold its locks: " + this);

        }
        
        writeService.beforeTask(t, this);

        boolean ran = false;

        try {

            final T ret;
            
            /*
             * By the time the call returns any lock(s) have been released.
             * 
             * Note: Locks MUST be released as soon as the task is done writing
             * so that it does NOT hold locks while it is awaiting commit. This
             * make it possible for other operations to write on the same index
             * in the same commit group.
             */
//            try {

                ret = delegate.call();
                
//            } finally {
//                
//                /*
//                 * Increment by the amount of time that the task was waiting to
//                 * acquire its lock(s).
//                 */
//
//                taskCounters.lockWaitingTime.addAndGet( delegate.getLockLatency() );
//                
//            }

            if (Thread.interrupted()) {
            
                /*
                 * @todo why test after return? if the task ran, it ran, right?
                 */

                throw new InterruptedException();

            }

            // set flag.
            ran = true;

            if (log.isInfoEnabled())
                log.info("Task Ok: class=" + this);
                      
            /*
             * Note: The WriteServiceExecutor will await a commit signal before
             * the thread is allowed to complete. This ensures that the caller
             * waits until a commit (or an abort).
             * 
             * Note: Waiting here does NOT prevent other tasks from gaining
             * access to the same resources since the locks were released above.
             */ 

            writeService.afterTask(this/* task */, null/* cause */);

            return ret;

        } catch (Throwable t2) {

            if (!ran) {

                // Note: Do not re-invoke afterTask() if it failed above.

                if (log.isInfoEnabled())
                    log.info("Task failed: class=" + this + " : " + t2);
                
                writeService.afterTask(this/* task */, t2/* cause */);

            }

            /*
             * Throw whatever exception was thrown by the task (or by afterTask
             * if it craps out).
             */
            
            if (t2 instanceof Exception)
                throw (Exception) t2;

            throw new RuntimeException(t2);

        } finally {
            
            // discard hard references to accessed indices.
            clearIndexCache();
            
        }

    }

    /**
     * Delegates various behaviors visible to the application code using the
     * {@link ITask} interface to the {@link AbstractTask} object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static abstract protected class DelegateTask<T> implements ITask<T> {
        
        final protected AbstractTask<T> delegate;
        
        protected DelegateTask(final AbstractTask<T> delegate) {
            
            if (delegate == null)
                throw new IllegalArgumentException(); 
            
            this.delegate = delegate;
            
        }

        @Override
        public IResourceManager getResourceManager() {

            return delegate.getResourceManager();
        
        }

        @Override
        public IJournal getJournal() {

            return delegate.getJournal();
            
        }

        @Override
        public String[] getResource() {

            return delegate.getResource();
            
        }

        @Override
        public String getOnlyResource() {

            return delegate.getOnlyResource();
            
        }

        @Override
        public IIndex getIndex(String name) {

            return delegate.getIndex(name);
            
        }
                
        @Override
        public TaskCounters getTaskCounters() {
            
            return delegate.getTaskCounters();
            
        }
        
        @Override
        public String toString() {
            
            return getClass().getName() + "(" + delegate.toString() + ")";
           
        }
        
    }
    
    /**
     * Inner class used to wrap up the call to {@link AbstractTask#doTask()} for
     * read-write transactions.
     */
    static protected class InnerReadWriteTxServiceCallable<T> extends DelegateTask<T> {

        private final Tx tx;
        
        InnerReadWriteTxServiceCallable(final AbstractTask<T> delegate, final Tx tx) {
            
            super( delegate );
            
            if (tx == null)
                throw new IllegalArgumentException();
            
            this.tx = tx;
            
        }

        /**
         * Wraps up the execution of {@link AbstractTask#doTask()}.
         */
        @Override
        public T call() throws Exception {

            // invoke on the outer class.

            try {

                delegate.nanoTime_beginWork = System.nanoTime();
                tx.lock.lock();
                try {
                    return delegate.doTask();
                } finally {
                    tx.lock.unlock();
                }
            } finally {

                delegate.nanoTime_finishedWork = System.nanoTime();

                /*
                 * Release hard references to the named indices. The backing
                 * indices are reading from the ground state identified by the
                 * start time of the ReadWrite transaction.
                 */
                
                delegate.clearIndexCache();
                
            }
            
        }

    }
    
    /**
     * An instance of this class is used as the delegate to coordinate the acquisition of locks
     * with the {@link NonBlockingLockManager} before the task can execute and to release
     * locks after the task has completed (whether it succeeds or fails).
     * <p>
     * Note: This inner class delegates the execution of the task to
     * {@link AbstractTask#doTask()} on the outer class.
     * <p>
     * Note: If there is only a single writer thread then the lock system
     * essentially does nothing. When there are multiple writer threads the lock
     * system imposes a partial ordering on the writers that ensures that writes
     * on a given named index are single-threaded and that deadlocks do not
     * prevent tasks from progressing. If there is strong lock contention then
     * writers will be more or less serialized.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static protected class InnerWriteServiceCallable<T> extends DelegateTask<T> {

        InnerWriteServiceCallable(final AbstractTask<T> delegate) {
            
            super(delegate);
            
        }

        /**
         * Note: Locks on the named indices are ONLY held during this call.
         */
        @Override
        public T call() throws Exception {

            // The write service on which this task is running.
            final WriteExecutorService writeService = delegate.concurrencyManager
                    .getWriteService();

            // setup view of the declared resources.
            delegate.setupIndices();
            
            delegate.nanoTime_beginWork = System.nanoTime();

            writeService.activeTaskCountWithLocksHeld.incrementAndGet();

            try {

                // invoke doTask() on AbstractTask with locks.
                final T ret = delegate.doTask();

                // checkpoint while holding locks.
                delegate.checkpointNanoTime = delegate.checkpointTask();

                return ret;

            } catch(Throwable t) {

                /*
                 * RWStore: If there is an error in the task execution, then for
                 * RWStore we need to explicitly undo the allocations for the
                 * B+Tree(s) on which this task wrote. If we do not take this
                 * step, then the records already written onto the store up to
                 * that point will never be released if the groupCommit
                 * succeeds. This is essentially a persistent memory leak on the
                 * store.
                 */
                delegate.abortTask();
                
                // rethrow the exception.
                throw new RuntimeException(t);
                
            } finally {
                
                /*
                 * @todo This is the ONLY place where it would be safe to handle
                 * after actions while holding the lock. As soon as we leave
                 * this method we have lost the exclusive lock on the index and
                 * another task could be running on it before we can do anything
                 * else. However, note that an after action typically requires
                 * that the task has already been committed or aborted. In order
                 * to support such after actions we would have to wait until the
                 * abort or commit before releasing the lock.
                 */
                
                delegate.nanoTime_finishedWork = System.nanoTime();
                
                writeService.activeTaskCountWithLocksHeld.decrementAndGet();
                
                try {
                    /*
                     * Release the locks held by the task.
                     */
                    writeService.getLockManager().releaseLocksForTask(
                            delegate.resource);
                } catch (Throwable t) {
                    // log an error but do not abort the task.
                    if (verifyLocks)
                        log.error(delegate, t); // log as an error.
                    // fall through
                }
                
            }
            
        }
        
    }
    
    /**
     * This is thrown if you attempt to reuse (re-submit) the same
     * {@link AbstractTask} instance.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class ResubmitException extends RejectedExecutionException {

        /**
         * 
         */
        private static final long serialVersionUID = -8661545948587322943L;
        
        public ResubmitException() {
            super();
        }
        
        public ResubmitException(final String msg) {
            
            super(msg);
            
        }

    }
    
    /**
     * A read-write view of an {@link IJournal} that is used to impose isolated
     * and atomic changes for {@link ITx#UNISOLATED} tasks that register or drop
     * indices. The intentions of the task are buffered locally (by this class)
     * for that task such that the index appears to have been registered or
     * dropped immediately. Those intentions are propagated to {@link Name2Addr}
     * on a task-by-task basis only when (a) this task is part of the current
     * commit group; and (b) the {@link WriteExecutorService} performs a group
     * commit. If there is a conflict between the state of {@link Name2Addr} at
     * the time of the commit and the intentions of the tasks in the commit
     * group (for example, if two tasks try to drop the same index), then the
     * 2nd task will be interrupted. The enumeration of the intentions of tasks
     * always begins with the task executed by the thread performing the commit
     * so that no other task's intention could cause the commit itself to be
     * interrupted.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    class IsolatedActionJournal implements IJournal {
        
        private final AbstractJournal delegate;
        
        private final IAllocationContext context;

        @SuppressWarnings("rawtypes")
        private final IResourceLocator resourceLocator;

        private final GlobalRowStoreHelper globalRowStoreHelper;
        
        @Override
        public String toString() {

            return getClass().getName() + "{task=" + AbstractTask.this + "}";

        }
        
        public void prepareCommit() {
//            final IBufferStrategy bufferStrategy = delegate.getBufferStrategy();
//            if (bufferStrategy instanceof RWStrategy) {
//                ((RWStrategy) bufferStrategy).getRWStore().activateTx();
//            }
            // now detach!
            detachContext();
        }

        // RawTx encapsulates the transaction protocol to prevent multiple calls to close()
        private final IRawTx m_rawTx;
        
        public void completeTask() {
            if (m_rawTx != null) {
                m_rawTx.close();
            }
        }

        /**
         * This class prevents {@link ITx#UNISOLATED} tasks from having direct
         * access to the {@link AbstractJournal} using
         * {@link AbstractTask#getJournal()}.
         * 
         * @param source
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public IsolatedActionJournal(final AbstractJournal source) {

            if (source == null)
                throw new IllegalArgumentException();

            // Note: The raw journal.
            this.delegate = source;

            // Note: isolated by this class.
            this.globalRowStoreHelper = new GlobalRowStoreHelper(this);
            
         /*
          * Setup a locator for resources. Resources that correspond to indices
          * declared by the task are accessible via the task itself.
          * 
          * Note: This SHOULD NOT expose the underlying Journal's resource
          * locator since that can allow a class to register an index without
          * going through this IsolatedActionJournal. In particular, the
          * GlobalRowStoreHelper.getGlobalRowStore() can cause the GRS to be
          * registered on the underlying index. This was breaking HA starts when
          * I first converted the code to support group commit at the NSS layer.
          * 
          * TODO I have left in support for the case where the underlying
          * Journal the ManagedJournal associated with a DataService in a
          * federation. While I have not verified it, the following comment
          * clearly contemplates providing access to non-remote resources in
          * this manner. "Other resources are assessible via the locator on the
          * underlying journal. When the journal is part of a federation, that
          * locator will be the federation's locator." I am not sure if this
          * comment reflects a sensible design decision or a Bad Idea, in which
          * case the delegate locator should be [null] for the federation as
          * well.
          */

            resourceLocator = new DefaultResourceLocator(//
                  this,// IndexManager (IsolatedActionJournal)
                  ((source instanceof ManagedJournal) ? source
                     .getResourceLocator() : null/* DO-NOT-DELEGATE */)//
            );

            final IBufferStrategy bufferStrategy = source.getBufferStrategy();
            if (bufferStrategy instanceof IRWStrategy) {
                // must grab the tx BEFORE registering the context to correctly
                //  bracket, since the tx count is decremented AFTER the
                //  context is released
                m_rawTx = ((IRWStrategy) bufferStrategy).newTx();
                context = ((IRWStrategy) bufferStrategy).newAllocationContext(true /*isolated*/);
                if (context == null) {
                	throw new AssertionError("Null context returned by " + bufferStrategy.getClass().getName());
                }
            } else {
                m_rawTx = null;
                context = null;
            }
        }

        /*
         * Overridden methods for registering or dropping indices.
         */
        
        /**
         * Delegates to the {@link AbstractTask}.
         */
        @Override
        public void dropIndex(final String name) {

            AbstractTask.this.dropIndex(name);
            
        }

        /**
         * Note: This is the core implementation for registering an index - it
         * delegates to the {@link AbstractTask}.
         */
        @Override
        public IIndex registerIndex(final String name, final BTree btree) {

            return AbstractTask.this.registerIndex(name, btree);

        }

        @Override
        public ICheckpointProtocol register(final String name, final IndexMetadata metadata) {

            /*
             * FIXME GIST : Support registration of index types other than BTree
             * (HTree, Stream, etc).
             * 
             * @see http://trac.blazegraph.com/ticket/585 (GIST)
             */

            throw new UnsupportedOperationException();
            
        }

        @Override
        public void registerIndex(final IndexMetadata indexMetadata) {

            // delegate to core impl.
            registerIndex(indexMetadata.getName(), indexMetadata);

        }

        /**
         * FIXME GIST This needs to use logic to handle the different types of
         * index objects that we can create. That logic is currently centralized
         * in {@link AbstractJournal#register(String, IndexMetadata)}. The same
         * pattern needs to be put to use here.
         */
        @Override
        public IIndex registerIndex(final String name,
                final IndexMetadata indexMetadata) {

            // Note: handles constraints and defaults for index partitions.
            delegate.validateIndexMetadata(name, indexMetadata);

            // Note: create on the _delegate_.
            final BTree btree = BTree.create(delegate, indexMetadata);

            // delegate to core impl.
            return registerIndex(name, btree);

        }

        /**
         * Note: access to an unisolated index is governed by the AbstractTask.
         */
        @Override
        public ICheckpointProtocol getUnisolatedIndex(final String name) {
            try {

                /*
                 * FIXME GIST. This will throw a ClassCastException if the
                 * returned index is an ILocalBTreeView.
                 * 
                 * @see http://trac.blazegraph.com/ticket/585 (GIST)
                 */

                return (ICheckpointProtocol) AbstractTask.this.getIndex(name);
                
            } catch(NoSuchIndexException ex) {
                
                // api conformance.
                return null;
                
            }
        }

        /**
         * Note: access to an unisolated index is governed by the AbstractTask.
         */
        @Override
        public IIndex getIndex(final String name) {

            try {

                return AbstractTask.this.getIndex(name);
                
            } catch(NoSuchIndexException ex) {
                
                // api conformance.
                return null;
                
            }
            
        }

        /**
         * Note: you are allowed access to historical indices without having to
         * declare a lock - such views will always be read-only and support
         * concurrent readers.
         */
        @Override
        public ICheckpointProtocol getIndexLocal(final String name,
                final long commitTime) {

            if (commitTime == ITx.UNISOLATED) {

                return getUnisolatedIndex(name);

            }

            /*
             * The index view is obtained from the resource manager.
             */

            if (resourceManager instanceof IJournal) {
   
               /*
                * This code path supports any type of index (BTree, HTree, etc).
                * 
                * Note: It does NOT support ILocalBTreeView (e.g., FusedView,
                * IsolatedFusedView).
                */

                return ((IJournal) resourceManager).getIndexLocal(name,
                        commitTime);

            }
                
            /**
             * FIXME GIST : This code path only supports BTree
             * (ILocalBTreeView). An attempt to resolve an HTree or other
             * non-BTree based named index data structure will probably result
             * in a ClassCastException.
             * 
             * @see <a
             *      href="http://trac.blazegraph.com/ticket/585"
             *      > GIST </a>
             */
            return (ICheckpointProtocol) resourceManager.getIndex(name, commitTime);
                
        }
            
        @Override
        public IIndex getIndex(final String name, final long timestamp) {

           if(TimestampUtility.isReadWriteTx(timestamp)) {
              
               /*
                * This code path supports read/write transactions. The interface
                * returned for a read/write transaction is an ILocalBTreeView and
                * will be an IsolatableFusedView (extends FusedView). These classes
                * do not implement ICheckpointProtocol. Therefore we can not
                * delegate this method to getIndexLocal() which returns an
                * ICheckpointProtocol instance.
                * 
                * See #1156 (Read/write tx support in REST API).
                */

               return resourceManager.getIndex(name, timestamp);
              
           }

            return (IIndex) getIndexLocal(name, timestamp);
            
        }

        /**
         * Returns an {@link ITx#READ_COMMITTED} view if the index exists -or-
         * an {@link ITx#UNISOLATED} view IFF the {@link AbstractTask} declared
         * the name of the backing index as one of the resources for which it
         * acquired a lock.
         */
        @Override
        public SparseRowStore getGlobalRowStore() {
            
            // did the task declare the resource name?
            if(isResource(GlobalRowStoreHelper.GLOBAL_ROW_STORE_INDEX)) {
                
               /*
                * Return an unisolated view of the index, but the view is still
                * scoped by the task. The index will be created if it does not
                * exist.
                */
               return globalRowStoreHelper.getGlobalRowStore();
                   
            }
               
            // read committed view IFF it exists otherwise [null]
            // TODO Review. Make sure we have tx protection to avoid recycling of the view.
            final long lastCommitTime = getLastCommitTime();
            
            return globalRowStoreHelper.get(lastCommitTime);

            //return new GlobalRowStoreHelper(this).get(ITx.READ_COMMITTED);
            
        }

        @Override
        public SparseRowStore getGlobalRowStore(final long timestamp) {

            if (!TimestampUtility.isReadOnly(timestamp)) {

                // A request for a mutable view.
                return getGlobalRowStore();

            }
            
            /*
             * Note: This goes around getIndex(name,timestamp) on this method
             * and uses that method on the delegate. This is because of the
             * restriction on access to declared indices. It's Ok to go around
             * like this since you do not need a lock for a read-only view.
             */
            
//            return new GlobalRowStoreHelper(this).getReadCommitted();

            final IIndex ndx = delegate.getIndex(
                    GlobalRowStoreHelper.GLOBAL_ROW_STORE_INDEX, timestamp);

            if (ndx != null) {

                return new SparseRowStore(ndx);

            }
            
            return null;
            
        }

        /**
         * Returns an {@link ITx#READ_COMMITTED} view if the file system exists
         * -or- an {@link ITx#UNISOLATED} view IFF the {@link AbstractTask}
         * declared the names of the backing indices as resources for which it
         * acquired a lock.
         */
        @Override
        public BigdataFileSystem getGlobalFileSystem() {
            
            // did the task declare the resource name?
            final String namespace = GlobalFileSystemHelper.GLOBAL_FILE_SYSTEM_NAMESPACE;
            if (isResource(namespace + "."+BigdataFileSystem.FILE_METADATA_INDEX_BASENAME)
                    && isResource(namespace + "."+BigdataFileSystem.FILE_DATA_INDEX_BASENAME)) {

                // unisolated view - will create if it does not exist.
                return new GlobalFileSystemHelper(this).getGlobalFileSystem();

            }

            // read committed view IFF it exists otherwise [null]
            return new GlobalFileSystemHelper(this).getReadCommitted();
            
        }

        /**
         * Returns an {@link TemporaryStore} local to a specific
         * {@link AbstractTask}.
         * <p>
         * Note: While data can not be shared across {@link AbstractTask}s using
         * the returned {@link TemporaryStore}, you can create an
         * {@link ILocatableResource} on a {@link TemporaryStore} and then
         * locate it from within the {@link AbstractTask}. This has the
         * advantage that the isolation and read/write constraints of the
         * {@link AbstractTask} will be imposed on access to the
         * {@link ILocatableResource}s.
         * 
         * FIXME GIST Reconsider the inner journal classes on AbstractTask. This
         * is a heavy weight mechanism for enforcing isolation for temporary
         * stores. It would be better to have isolation in the locator mechanism
         * itself. This will especially effect scale-out query using temporary
         * stores and will break semantics when the task is isolated by a
         * transaction rather than unisolated.
         */
        @Override
        public TemporaryStore getTempStore() {
            
            return tempStoreFactory.getTempStore();
            
        }
        final private TemporaryStoreFactory tempStoreFactory = new TemporaryStoreFactory();
        
        @Override
        public IResourceLocator<?> getResourceLocator() {
            
            return resourceLocator;
            
        }

        @Override
        public ILocalTransactionManager getLocalTransactionManager() {
            
            return delegate.getLocalTransactionManager();
            
        }

        @Override
        public IResourceLockService getResourceLockService() {
            
            return delegate.getResourceLockService();
            
        }

        @Override
        public ExecutorService getExecutorService() {
            
            return delegate.getExecutorService();
            
        }

        @Override
        public Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {
           return delegate.getQuorum();
        }
        
        @Override
        final public long awaitHAReady(final long timeout, final TimeUnit units)
                throws InterruptedException, TimeoutException,
                AsynchronousQuorumCloseException {
           return delegate.awaitHAReady(timeout, units);
        }
        
        /*
         *  Disallowed methods (commit protocol and shutdown protocol).
         */
        
        /**
         * {@inheritDoc}
         * <p>
         * Marks the task as aborted. The task will not commit. However, the
         * task will continue to execute until control returns from its
         * {@link AbstractTask#doTask()} method.
         */
        @Override
        public void abort() {
            aborted = true;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden as NOP. Tasks do not directly invoke commit() on the
         * Journal.
         */
        @Override
        public long commit() {
            if (aborted)
                throw new IllegalStateException("aborted");
            return 0;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteResources() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCommitter(int index, ICommitter committer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdownNow() {
            throw new UnsupportedOperationException();
        }

        /*
         * Methods which delegate directly to the live journal.
         */

//        public IKeyBuilder getKeyBuilder() {
//            return delegate.getKeyBuilder();
//        }
        
        @Override
        public void force(final boolean metadata) {
            delegate.force(metadata);
        }

        @Override
        public int getByteCount(final long addr) {
            return delegate.getByteCount(addr);
        }

        @Override
        public ICommitRecord getCommitRecord(final long timestamp) {
            return delegate.getCommitRecord(timestamp);
        }

        @Override
        public CounterSet getCounters() {
            return delegate.getCounters();
        }

        @Override
        public File getFile() {
            return delegate.getFile();
        }

        @Override
        public long getOffset(final long addr) {
            return delegate.getOffset(addr);
        }

        @Override
        public long getPhysicalAddress(final long addr) {
            return delegate.getPhysicalAddress(addr);
        }

        @Override
        public Properties getProperties() {
            return delegate.getProperties();
        }

        @Override
        public UUID getUUID() {
            return delegate.getUUID();
        }
        
        @Override
        public IResourceMetadata getResourceMetadata() {
            return delegate.getResourceMetadata();
        }

        @Override
        public long getRootAddr(final int index) {
            return delegate.getRootAddr(index);
        }

        @Override
        public long getLastCommitTime() {
            return delegate.getLastCommitTime();
        }
        
        @Override
        public IRootBlockView getRootBlockView() {
            return delegate.getRootBlockView();
        }

        @Override
        public boolean isFullyBuffered() {
            return delegate.isFullyBuffered();
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public boolean isReadOnly() {
            return delegate.isReadOnly();
        }

        @Override
        public boolean isStable() {
            return delegate.isStable();
        }

//        public void packAddr(DataOutput out, long addr) throws IOException {
//            delegate.packAddr(out, addr);
//        }

        @Override
        public ByteBuffer read(final long addr) {
            return delegate.read(addr);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public long toAddr(final int nbytes, final long offset) {
            return delegate.toAddr(nbytes, offset);
        }

        @Override
        public String toString(final long addr) {
            return delegate.toString(addr);
        }

       // @Override
//        public IRootBlockView getRootBlock(final long commitTime) {
//            return delegate.getRootBlock(commitTime);
//        }
//
//        public Iterator<IRootBlockView> getRootBlocks(final long startTime) {
//            return delegate.getRootBlocks(startTime);
//        }

        /*
         * IAllocationContext
         * 
         * The journal write() and delete() methods are overridden here to use
         * the IsolatedActionJournal as the IAllocationContext. This causes the
         * allocations to be scoped to the AbstractTask.
         */

        @Override
        public long write(final ByteBuffer data) {
            return delegate.write(data, context);
        }

//        @Override
//        public long write(final ByteBuffer data, final long oldAddr) {
//            return delegate.write(data, oldAddr, this);
//        }


        @Override
        public IPSOutputStream getOutputStream() {
            return delegate.getOutputStream(context);
        }

        @Override
        public InputStream getInputStream(long addr) {
            return delegate.getInputStream(addr);
        }

        @Override
        public void delete(final long addr) {
            delegate.delete(addr, context);
        }

//      public void delete(long addr, IAllocationContext context) {
//          delegate.delete(addr, context);
//      }
//
//      public long write(ByteBuffer data, IAllocationContext context) {
//          return delegate.write(data, context);
//      }
//
//      public long write(ByteBuffer data, long oldAddr, IAllocationContext context) {
//          return delegate.write(data, oldAddr, context);
//      }

        public void detachContext() {
            delegate.detachContext(context);
        }

        public void abortContext() {
            delegate.abortContext(context);
            
            completeTask();
        }

        @Override
        public ScheduledFuture<?> addScheduledTask(final Runnable task,
                final long initialDelay, final long delay, final TimeUnit unit) {
            return delegate.addScheduledTask(task, initialDelay, delay, unit);
        }

        @Override
        public boolean getCollectPlatformStatistics() {
            return delegate.getCollectPlatformStatistics();
        }

        @Override
        public boolean getCollectQueueStatistics() {
            return delegate.getCollectQueueStatistics();
        }

        @Override
        public int getHttpdPort() {
            return delegate.getHttpdPort();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to visit the name of all indices that were isolated and to
         * ignore the timestamp.
         */
        @SuppressWarnings("unchecked")
        @Override
        public Iterator<String> indexNameScan(final String prefix,
                final long timestampIsIgnored) {

            return new Striterator(n2a.values().iterator())
                    .addFilter(new Filter(){
                     private static final long serialVersionUID = 1L;
                     
                  /*
                   * Impose prefix restriction on the Name2Addr scan.
                   * 
                   * TODO This forces us to scan all indices that were isolated
                   * by the task. When using hierarchical locking that is
                   * typically on the order of ~10 indices. If also using
                   * durable named solution sets, then this could be quite a bit
                   * more and it might be worthwhile to make [n2a] on
                   * AbstractTask a TreeMap (using the same comparator
                   * semantics) such that the prefix scan could be turned into a
                   * range restricted scan.
                   */
                     @Override
                     public boolean isValid(Object obj) {
                        return ((Entry)obj).name.startsWith(prefix);
                     }}).addFilter(new Resolver() {
                        private static final long serialVersionUID = 1L;
                        /*
                         * Resolve Entry to the name of the index.
                         */
                        @Override
                        protected Object resolve(final Object obj) {
                            return ((Entry)obj).name;
                        }
                    });

        }

        @Override
        public boolean isDirty() {
            return delegate.isDirty();
        }

        @Override
        public boolean isGroupCommit() {
           return delegate.isGroupCommit();
        }

		@Override
		public boolean isHAJournal() {
			return false;
		}

    } // class IsolatatedActionJournal

    /**
     * A read-only view of an {@link IJournal} that is used to enforce read-only
     * semantics on tasks using {@link AbstractTask#getJournal()} to access the
     * backing store. Methods that write on the journal, that expose the
     * unisolated indices, or which are part of the commit protocol will throw
     * an {@link UnsupportedOperationException}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private class ReadOnlyJournal implements IJournal {

        private final IJournal delegate;
        
        @SuppressWarnings("rawtypes")
        private final DefaultResourceLocator resourceLocator;

        @Override
        public String toString() {

            return getClass().getName() + "{task=" + AbstractTask.this + "}";

        }
        
        @Override
		public boolean isHAJournal() {
			return false;
		}

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public ReadOnlyJournal(final AbstractJournal source) {

            if (source == null)
                throw new IllegalArgumentException();

            this.delegate = source;

            /*
             * Setup a locator for resources. Resources that correspond to
             * indices declared by the task are accessible via the task itself.
             * Other resources are accessible via the locator on the underlying
             * journal. When the journal is part of a federation, that locator
             * will be the federation's locator.
             */

            resourceLocator = new DefaultResourceLocator(//
                    this, // IndexManager
                    source.getResourceLocator()// delegate locator
            );

        }

        /*
         * Index access methods (overridden or disallowed depending on what they
         * do).
         */
        
        @Override
        public IIndex getIndex(final String name, final long timestamp) {
            
            if (timestamp == ITx.UNISOLATED)
                throw new UnsupportedOperationException();

            if (timestamp == AbstractTask.this.timestamp) {
                
                // to the AbstractTask
                try {

                    return AbstractTask.this.getIndex(name);
                    
                } catch(NoSuchIndexException ex) {
                    
                    // api conformance.
                    return null;

                }

        }

            // to the backing journal.
            return (IIndex) delegate.getIndexLocal(name, timestamp);

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Note: Does not allow access to {@link ITx#UNISOLATED} indices.
         */
        @Override
        public ICheckpointProtocol getIndexLocal(final String name,
                final long commitTime) {

            if (timestamp == ITx.UNISOLATED)
                throw new UnsupportedOperationException();

            if(timestamp == AbstractTask.this.timestamp) {
                
                // to the AbstractTask
                try {

                    /*
                     * FIXME GIST : This will throw a ClassCastException if the
                     * index type is ReadCommittedIndex or FusedView.
                     */
                    return (ICheckpointProtocol) AbstractTask.this
                            .getIndex(name);
                    
                } catch(NoSuchIndexException ex) {
                    
                    // api conformance.
                    return null;
                    
                }
                
            }
            
            // to the backing journal.
            return delegate.getIndexLocal(name, timestamp);

        }

        /**
         * {@inheritDoc}
         * <p>
         * Note: Does not allow access to {@link ITx#UNISOLATED} indices.
         */
        @Override
        public Iterator<String> indexNameScan(final String prefix,
                final long timestamp) {

            if (timestamp == ITx.UNISOLATED)
                throw new UnsupportedOperationException();

            // to the backing journal.
            return delegate.indexNameScan(prefix, timestamp);

        }

        /**
         * Note: Not supported since this method returns the
         * {@link ITx#UNISOLATED} index.
         */
        @Override
        public ICheckpointProtocol getUnisolatedIndex(String name) {

            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Note: Not supported since this method returns the
         * {@link ITx#UNISOLATED} index.
         */
        @Override
        public IIndex getIndex(String name) {

            throw new UnsupportedOperationException();
            
        }
        
        @Override
        public void dropIndex(String name) {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public ICheckpointProtocol register(String name, IndexMetadata metadata) {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public void registerIndex(IndexMetadata indexMetadata) {
            
            throw new UnsupportedOperationException();
            
        }

        @Override
        public IIndex registerIndex(String name, BTree btree) {
            
            throw new UnsupportedOperationException();
            
        }

        @Override
        public IIndex registerIndex(String name, IndexMetadata indexMetadata) {
            
            throw new UnsupportedOperationException();
            
        }

        /**
         * Returns an {@link ITx#READ_COMMITTED} view iff the index exists and
         * <code>null</code> otherwise.
         */
        @Override
        public SparseRowStore getGlobalRowStore() {

            /*
             * Note: This goes around getIndex(name,timestamp) on this method
             * and uses that method on the delegate. This is because of the
             * restriction on access to declared indices. It's Ok to go around
             * like this since you do not need a lock for a read-only view.
             */
            
//            return new GlobalRowStoreHelper(this).getReadCommitted();

            // last commit time.
            final long lastCommitTime = delegate.getRootBlockView()
                    .getLastCommitTime();
            
            final IIndex ndx = delegate.getIndex(
                    GlobalRowStoreHelper.GLOBAL_ROW_STORE_INDEX,
                    TimestampUtility.asHistoricalRead(lastCommitTime));

            if (ndx != null) {

                return new SparseRowStore(ndx);

            }
            
            return null;
            
        }

        @Override
        public SparseRowStore getGlobalRowStore(final long timestamp) {

            /*
             * Note: This goes around getIndex(name,timestamp) on this method
             * and uses that method on the delegate. This is because of the
             * restriction on access to declared indices. It's Ok to go around
             * like this since you do not need a lock for a read-only view.
             */
            
//            return new GlobalRowStoreHelper(this).getReadCommitted();

            if (!TimestampUtility.isReadOnly(timestamp)) {

                throw new IllegalArgumentException(
                        "Only read-only views are supported: timestamp="
                                + TimestampUtility.toString(timestamp));
                
            }
            
            final IIndex ndx = delegate.getIndex(
                    GlobalRowStoreHelper.GLOBAL_ROW_STORE_INDEX, timestamp);

            if (ndx != null) {

                return new SparseRowStore(ndx);

            }
            
            return null;
            
        }

        /**
         * Returns an {@link ITx#READ_COMMITTED} view iff the file system exists
         * and <code>null</code> otherwise.
         */
        @Override
        public BigdataFileSystem getGlobalFileSystem() {

            /*
             * Note: This goes around getIndex(name,timestamp) on this method
             * and uses that method on the delegate. This is because of the
             * restriction on access to declared indices. It's Ok to go around
             * like this since you do not need a lock for a read-only view.
             */
            
//            return new GlobalRowStoreHelper(this).getReadCommitted();

            final IIndexManager tmp = new DelegateIndexManager(this) {

                @Override
                public IIndex getIndex(final String name, final long timestampIsIgnored) {

                    // last commit time.
                    final long commitTime = delegate.getRootBlockView()
                            .getLastCommitTime();
                    
                    return delegate.getIndex(name, TimestampUtility
                            .asHistoricalRead(commitTime));

                }

            };

            return new GlobalFileSystemHelper(tmp).getReadCommitted();
            
        }

        /**
         * Returns an {@link TemporaryStore} local to a specific
         * {@link AbstractTask}.
         * <p>
         * Note: While data can not be shared across {@link AbstractTask}s using
         * the returned {@link TemporaryStore}, you can create an
         * {@link ILocatableResource} on a {@link TemporaryStore} and then
         * locate it from within the {@link AbstractTask}. This has the
         * advantage that the isolation and read/write constraints of the
         * {@link AbstractTask} will be imposed on access to the
         * {@link ILocatableResource}s.
         * 
         * FIXME Reconsider the inner journal classes on AbstractTask. This is a
         * heavy weight mechanism for enforcing isolation for temporary stores.
         * It would be better to have isolation in the locator mechanism itself.
         * This will especially effect scale-out query using temporary stores
         * and will break semantics when the task is isolated by a transaction
         * rather than unisolated.
         */
        @Override
        public TemporaryStore getTempStore() {
            
            return tempStoreFactory.getTempStore();
            
        }
        private TemporaryStoreFactory tempStoreFactory = new TemporaryStoreFactory();
        
        @Override
        public DefaultResourceLocator<?> getResourceLocator() {
            
            return resourceLocator;
            
        }
        
        @Override
        public ILocalTransactionManager getLocalTransactionManager() {
            
            return delegate.getLocalTransactionManager();
            
        }

        @Override
        public IResourceLockService getResourceLockService() {
            
            return delegate.getResourceLockService();
            
        }

        @Override
        public ExecutorService getExecutorService() {
            
            return delegate.getExecutorService();
            
        }

        /*
         * Disallowed methods (commit and shutdown protocols).
         */
        
        @Override
        public void abort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {
           throw new UnsupportedOperationException();
        }
        
        @Override
        final public long awaitHAReady(final long timeout, final TimeUnit units)
                throws InterruptedException, TimeoutException,
                AsynchronousQuorumCloseException {
           return delegate.awaitHAReady(timeout, units);
        }

        @Override
        public long commit() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteResources() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCommitter(int index, ICommitter committer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdownNow() {
            throw new UnsupportedOperationException();
        }

        /*
         * Disallowed methods (methods that write on the store).
         */

        @Override
        public void force(boolean metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(ByteBuffer data) {
            throw new UnsupportedOperationException();
        }       

//        @Override
//        public long write(ByteBuffer data, long oldAddr) {
//            throw new UnsupportedOperationException();
//        }
        
        @Override
        public void delete(long addr) {
            throw new UnsupportedOperationException();
        }

        /*
         * Methods that delegate directly to the backing journal.
         */
        
        @Override
        public int getByteCount(long addr) {
            return delegate.getByteCount(addr);
        }

        @Override
        public ICommitRecord getCommitRecord(long timestamp) {
            return delegate.getCommitRecord(timestamp);
        }

        @Override
        public CounterSet getCounters() {
            return delegate.getCounters();
        }

        @Override
        public File getFile() {
            return delegate.getFile();
        }

        @Override
        public long getOffset(long addr) {
            return delegate.getOffset(addr);
        }

        @Override
        public long getPhysicalAddress(final long addr) {
            return delegate.getPhysicalAddress(addr);
        }

        @Override
        public Properties getProperties() {
            return delegate.getProperties();
        }

        @Override
        public UUID getUUID() {
            return delegate.getUUID();
        }
        
        @Override
        public IResourceMetadata getResourceMetadata() {
            return delegate.getResourceMetadata();
        }

        @Override
        public long getRootAddr(int index) {
            return delegate.getRootAddr(index);
        }

        @Override
        public long getLastCommitTime() {
            return delegate.getLastCommitTime();
        }
        
        @Override
        public IRootBlockView getRootBlockView() {
            return delegate.getRootBlockView();
        }

        @Override
        public boolean isFullyBuffered() {
            return delegate.isFullyBuffered();
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public boolean isReadOnly() {
            return delegate.isReadOnly();
        }

        @Override
        public boolean isStable() {
            return delegate.isStable();
        }

        @Override
        public ByteBuffer read(long addr) {
            return delegate.read(addr);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public long toAddr(int nbytes, long offset) {
            return delegate.toAddr(nbytes, offset);
        }

        @Override
        public String toString(long addr) {
            return delegate.toString(addr);
        }

//        @Override
//        public IRootBlockView getRootBlock(long commitTime) {
//            return delegate.getRootBlock(commitTime);
//        }
//
//        public Iterator<IRootBlockView> getRootBlocks(long startTime) {
//            return delegate.getRootBlocks(startTime);
//        }

        @Override
        public ScheduledFuture<?> addScheduledTask(Runnable task,
                long initialDelay, long delay, TimeUnit unit) {
            return delegate.addScheduledTask(task, initialDelay, delay, unit);
        }

        @Override
        public boolean getCollectPlatformStatistics() {
            return delegate.getCollectPlatformStatistics();
        }

        @Override
        public boolean getCollectQueueStatistics() {
            return delegate.getCollectQueueStatistics();
        }

        @Override
        public int getHttpdPort() {
            return delegate.getHttpdPort();
        }

        @Override
        public IPSOutputStream getOutputStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getInputStream(long addr) {
            return delegate.getInputStream(addr);
        }

        @Override
        public boolean isDirty() {
            return false; // it's readOnly - cannot be dirty
        }

        @Override
        public boolean isGroupCommit() {
           return delegate.isGroupCommit();
        }
      
    } // class ReadOnlyJournal

    /**
     * Delegate pattern for {@link IIndexManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class DelegateIndexManager implements IIndexManager {
     
        private final IIndexManager delegate;
        
        public DelegateIndexManager(final IIndexManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public void dropIndex(String name) {
            delegate.dropIndex(name);
        }

        @Override
        public ExecutorService getExecutorService() {
            return delegate.getExecutorService();
        }

        @Override
        public BigdataFileSystem getGlobalFileSystem() {
            return delegate.getGlobalFileSystem();
        }

        @Override
        public SparseRowStore getGlobalRowStore() {
            return delegate.getGlobalRowStore();
        }

        @Override
        public SparseRowStore getGlobalRowStore(final long timestamp) {
            return delegate.getGlobalRowStore(timestamp);
        }

        @Override
        public IIndex getIndex(String name, long timestamp) {
            return delegate.getIndex(name, timestamp);
        }

        @Override
        public long getLastCommitTime() {
            return delegate.getLastCommitTime();
        }

        @Override
        public IResourceLocator<?> getResourceLocator() {
            return delegate.getResourceLocator();
        }

        @Override
        public IResourceLockService getResourceLockService() {
            return delegate.getResourceLockService();
        }

        @Override
        public void registerIndex(IndexMetadata indexMetadata) {
            delegate.registerIndex(indexMetadata);
        }

        @Override
        public void destroy() {
            delegate.destroy();
        }
        
        @Override
        public TemporaryStore getTempStore() {
            return delegate.getTempStore();
        }

        @Override
        public ScheduledFuture<?> addScheduledTask(Runnable task,
                long initialDelay, long delay, TimeUnit unit) {
            return delegate.addScheduledTask(task, initialDelay, delay, unit);
        }

        @Override
        public boolean getCollectPlatformStatistics() {
            return delegate.getCollectPlatformStatistics();
        }

        @Override
        public boolean getCollectQueueStatistics() {
            return delegate.getCollectQueueStatistics();
        }

        @Override
        public int getHttpdPort() {
            return delegate.getHttpdPort();
        }

        @Override
        public Iterator<String> indexNameScan(String prefix, long timestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CounterSet getCounters() {
            return delegate.getCounters();
        }

      @Override
      public boolean isGroupCommit() {
         return delegate.isGroupCommit();
      }
        
    }// DelegateIndexClass
    
}
