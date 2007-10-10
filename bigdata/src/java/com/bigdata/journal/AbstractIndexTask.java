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
 * Created on Oct 10, 2007
 */

package com.bigdata.journal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.concurrent.LockManager;
import com.bigdata.concurrent.LockManagerTask;
import com.bigdata.isolation.IIsolatedIndex;

/**
 * Abstract base class for tasks that may be submitted to the
 * {@link ConcurrentJournal}. Tasks may be isolated (by a transaction) or
 * unisolated. Tasks access named resources (aka indices), which they
 * pre-declare in their constructors.
 * <p>
 * Isolated tasks are part of a larger transaction. Transactions are started and
 * committed using an {@link ITransactionManager}. Several kinds of isolation
 * are supported - see {@link IsolationEnum}. Transactional tasks run with full
 * concurrency using an MVCC (Multi-Version Concurrency Control) strategy. When
 * a transaction is committed (by the {@link ITransactionManager}) it must wait
 * for lock(s) on the unisolated named indices on which it has written before it
 * may validate and commit.
 * <p>
 * Unisolated tasks are further divided into read only and read/write. A
 * read-only task reads against the last committed state of a named index.
 * Concurrent unisolated readers are permitted without locking on the same
 * index.
 * <p>
 * A read/write task reads and writes on the "live" index. Note that only a
 * single thread may write on a {@link BTree} at a time. Therefore read/write
 * tasks (often referred to as writers) obtain an exclusive lock on the named
 * index(s). When more than one named index is used, the locks are used to infer
 * a partial ordering of the writers allowing as much concurrency as possible.
 * Pre-declaration of locks allows us to avoid deadlocks in the lock system.
 * 
 * FIXME In order to allow concurrent tasks to do work on the same transaction
 * we need to use a per-transaction {@link LockManager} to produce a partial
 * order that governs access to the isolated (vs mutable unisolated) indices
 * accessed by that transaction.
 * <p>
 * Make sure that the {@link TemporaryStore} supports an appropriate level of
 * concurrency to allow concurrent writers on distinct isolated indices that are
 * being buffered on that store for a given transaction.
 * 
 * @todo Should it be possible to start/commit/abort a transaction from within
 *       an {@link AbstractIndexTask}? Only on the local journal? When the
 *       transaction manager is distributed? (I think not, certainly not when
 *       the tx is distributed and there is no need to handle start/stop of a
 *       local tx in this manner since you can just use unisolated reads or
 *       writes instead and have atomicity.)
 * 
 * @todo write a task design to support 1-phase commit (for a transaction that
 *       ran on a single journal) and 2/3-phase commit of transactions (for a
 *       transaction whose write sets are distributed across multiple journals).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractIndexTask implements Callable<Object> {

    static protected final Logger log = Logger
            .getLogger(AbstractIndexTask.class);

    /**
     * The journal against which the operation will be carried out.
     */
    protected final ConcurrentJournal journal;

    /**
     * The transaction identifier -or- {@link ITx#UNISOLATED} iff the operation
     * is not isolated by a transaction.
     */
    protected final long startTime;

    /**
     * True iff the operation is isolated by a transaction.
     */
    protected final boolean isolated;

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
    protected final ITx tx;

    /**
     * We note and save a reference to the most recent commit record when we
     * begin a task that is an <strong>unisolated reader</strong>. This allows
     * us to ensure that all index views requested by that reader are (a) from
     * the same committed state of the store; and (b) do NOT conflict with
     * unisolated writers (which are NOT thread-safe).
     */
    private ICommitRecord commitRecord = null;
    
    /**
     * Cache of resolved named indices.
     * 
     * @see #getIndex(String name)
     */
    final private Map<String,IIndexWithCounter> indexCache;

    /**
     * Convenience constructor variant for one named resource.
     * 
     * @param journal
     *            The journal on which the task will be executed.
     * @param startTime
     *            The transaction identifier or {@link ITx#UNISOLATED} iff the
     *            task is unisolated.
     * @param readOnly
     *            True iff the task is read-only.
     * @param resource
     *            The resource on which the task will operate. E.g., the names
     *            of the index. When the task is an unisolated write task an
     *            exclusive lock will be requested on the named resource and
     *            the task will NOT run until it has obtained that lock.
     */
    protected AbstractIndexTask(ConcurrentJournal journal, long startTime,
            boolean readOnly, String resource) {
        
        this(journal,startTime,readOnly,new String[]{resource});
        
    }
    
    /**
     * 
     * @param journal
     *            The journal on which the task will be executed.
     * @param startTime
     *            The transaction identifier or {@link ITx#UNISOLATED} iff the
     *            task is unisolated.
     * @param readOnly
     *            True iff the task is read-only.
     * @param resource
     *            The resource(s) on which the task will operate. E.g., the
     *            names of the index(s). When the task is an unisolated write
     *            task an exclusive lock will be requested on each named
     *            resource and the task will NOT run until it has obtained those
     *            lock(s).
     */
    protected AbstractIndexTask(ConcurrentJournal journal, long startTime,
            boolean readOnly, String[] resource) {

        if (journal == null)
            throw new NullPointerException();

        if (resource == null)
            throw new NullPointerException();

        for(int i=0; i<resource.length; i++) {
            
            if (resource[i] == null)
                throw new NullPointerException();
            
        }
        
        this.journal = journal;

        this.startTime = startTime;

        this.isolated = startTime != ITx.UNISOLATED;

        this.readOnly = readOnly;

        this.resource = resource;

        this.indexCache = new HashMap<String,IIndexWithCounter>(resource.length);
        
        if (startTime != 0L) {

            /*
             * Isolated read.
             */

            tx = journal.getTx(startTime);

            if (tx == null) {

                throw new IllegalStateException("Unknown tx");

            }

            if (!tx.isActive()) {

                throw new IllegalStateException("Tx not active");

            }

        } else {

            /*
             * Unisolated read.
             */

            tx = null;

        }

    }

    /**
     * Returns a copy of the array of resources declared to the constructor.
     */
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
    public String getOnlyResource() {
        
        if (resource.length > 1)
            throw new IllegalStateException("More than one resource was declared");
        
        return resource[0];
        
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
    protected String assertResource(String resource) {
        
        final String[] a = this.resource;
        
        for(int i=0; i<a.length; i++) {
            
            if(a[i].equals(resource)) {
                
                return resource;
                
            }
            
        }
        
        throw new IllegalStateException("Not declared by task: "+resource);
        
    }
    
    /**
     * Implement the task behavior here.
     * <p>
     * Note: Long-running implementations MUST periodically test
     * {@link Thread#interrupted()} and throw an {@link InterruptedException} if
     * they are interrupted. This behavior allows tasks to be cancelled in a
     * timely manner.
     * 
     * @return The object that will be returned by {@link #call()} iff the
     *         operation succeeds.
     * 
     * @throws Exception
     *             The exception that will be thrown by {@link #call()} iff the
     *             operation fails.
     */
    abstract protected Object doTask() throws Exception;

    /**
     * Coordinates with the {@link LockManager} and the
     * {@link WriteServiceExecutor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    class InnerCallable implements Callable<Object> {

        InnerCallable() { }

        /**
         * Note: If there is only a single writer thread then the lock
         * system essentially does nothing. When there are multiple writer
         * threads the lock system imposes a partial ordering on the writers
         * that ensures that writes on a given named index are
         * single-threaded and that deadlocks do not prevent tasks from
         * progressing.
         */
        public Object call() throws Exception {

            WriteExecutorService writeService = ((ConcurrentJournal) journal).writeService;

            writeService.beforeTask(Thread.currentThread(), this);

            boolean ran = false;

            try {

                final Object ret = doTask();

                ran = true;

                writeService.afterTask(this, null);

                return ret;

            } catch (Throwable t) {

                if (!ran) {

                    // Do not re-invoke it afterTask failed above.

                    writeService.afterTask(this, t);

                }

                if (t instanceof Exception)
                    throw (Exception) t;

                throw new RuntimeException(t);

            }

        }

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
     */
    final public Object call() throws Exception {

        try {
            
            if (isolated) {

                log.info("Running isolated operation: tx="+startTime);
                
                return doTask();

            }

            if (readOnly) {

                /*
                 * Read only task. We note the most recent commit record so that
                 * we can base all read-only views off of the same consistent
                 * historical state (and so that we do not conflict with
                 * unisolated writers on the live versions of the indices).
                 */

                commitRecord = journal.getCommitRecord();
                
                log.info("Running against committed state: commitCounter="
                        + commitRecord.getCommitCounter() + ", timestamp="
                        + commitRecord.getTimestamp());
                
                return doTask();

            } else {

                /*
                 * Read-write task. We use a delegate to force a schedule on the
                 * execution tasks that write on unisolated indices.
                 */
                
                // lock manager.
                LockManager<String> lockManager = ((ConcurrentJournal) journal).lockManager;

                // resource(s) to lock (exclusive locks are used).

                log.info("Unisolated write task: resources="
                        + Arrays.toString(resource));

                // declare resource(s) (will sort as a side-effect).
                lockManager.addResource(resource);

                // delegate will handle lock acquisition.
                Callable<Object> delegate = new LockManagerTask<String>(
                        lockManager, resource, new InnerCallable());
                
                /*
                 * Note: The lock(s) are only held during this call. By the time
                 * the call returns any lock(s) have been released.
                 * 
                 * If there is only a single write task worker then we can still
                 * assume that only the write set from this task exists on the
                 * named index(s) on which the task wrote.
                 * 
                 * However, when there are multiple write task workers new tasks
                 * MAY already be writing on the named index(s) on which this
                 * task just wrote.
                 * 
                 * Therefore when there are multiple write task workers the
                 * commit protocol forces new writer task workers to pause in
                 * order to ensure that write tasks are atomic (vs having
                 * half-completed write task made durable).
                 * 
                 * Note: locks MUST be released as soon as the task is done
                 * writing so that it does NOT hold locks while it is awaiting
                 * commit. This make it possible for other operations to write
                 * on the same index in the same commit group.
                 */

                Object ret = delegate.call();

                /* Note: The WriteServiceExecutor will await a commit signal before the thread is allowed to complete.  This ensures that the caller waits until a commit (or an abort). */ 
                
                return ret;
                
            }

        } finally {

            log.info("done");

        }
        
    }

    /**
     * Return an appropriate view of the named index for the operation.
     * <p>
     * When the task is isolated by a transaction, then the index will be
     * isolated by the transaction using the appropriate
     * {@link IsolationEnum isolation level}. If the transaction is read-only,
     * then the index will not be writable.
     * </p>
     * <p>
     * When the task is a read-only unisolated operation, the index will be
     * read-only and will read from the most recent committed state of the store
     * prior to the time at which the task began to execute. If multiple index
     * views are requested they will all use the same committed state of the
     * store.
     * </p>
     * <p>
     * When the task is an unisolated write operation the index will be the
     * unisolated writable (aka "live" or "current" index). Access to the
     * unisolated writable indices is single-threaded. This constraint is
     * enforced by a lock system using the named resources declared in the task
     * constructor.
     * </p>
     * 
     * @param name
     *            The index name.
     * 
     * @return An appropriate view of the named index.
     * 
     * @exception NoSuchIndexException
     *                if the named index does not exist at the time that the
     *                operation is executed.
     * 
     * @exception IllegalStateException
     *                if the named index was not declared to the constructor.
     */
    final public IIndexWithCounter getIndex(String name) {

        /*
         * Test the cache first.
         */
        {

            final IIndexWithCounter index = indexCache.get(name);

            if (index != null) {

                // Cached value.
                return index;

            }

        }

        final IIndexWithCounter tmp;

        if (isolated) {

            /*
             * isolated operation.
             */

            final IIsolatedIndex isolatedIndex = (IIsolatedIndex) tx
                    .getIndex(assertResource(name));

            if (isolatedIndex == null) {

                throw new NoSuchIndexException(name);

            }

            tmp = (IIndexWithCounter) isolatedIndex;

        } else {

            /*
             * unisolated operation.
             */

            if(readOnly) {
            
                /*
                 * Read-only unisolated index based on the last committed state
                 * of the store.
                 * 
                 * Note: We use this version of the index rather than the "live"
                 * index so that read operations on unisolated indices DO NOT
                 * conflict with write operations on unisolated indices.
                 */
                
                if(commitRecord.getCommitCounter()==0) {
                    
                    /*
                     * This catches the edge case where nothing has been
                     * committed onto the journal. In this case it is not
                     * possible to create a read-only unisolated view based on
                     * the last committed state since NOTHING has been
                     * committed.
                     */
                    
                    throw new NoSuchIndexException(name);
                    
                }
                
                IIndex foo = journal.getIndex(name,commitRecord);
                
                if (foo==null) {
                    
                    throw new NoSuchIndexException(name);

                }
                
                tmp = new ReadOnlyIndex(foo);
            
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock system is used to ensure that at most one
                 * task has access to this index at a time.
                 */
                
                tmp = (BTree) journal.getIndex(assertResource(name));

                if (tmp == null) {

                    throw new NoSuchIndexException(name);

                }

            }

        }

        indexCache.put(name,tmp);

        return tmp;

    }

}
