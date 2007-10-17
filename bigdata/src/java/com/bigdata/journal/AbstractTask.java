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

import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * <p>
 * Note: Use a distinct instance of this task each time you
 * {@link ConcurrentJournal#submit(AbstractTask)} it!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractTask implements Callable<Object> {

    static protected final Logger log = Logger.getLogger(AbstractTask.class);

    /**
     * Used to protect against re-submission of the same task object.
     */
    private final AtomicBoolean submitted = new AtomicBoolean(false);
    
    /**
     * The journal against which the operation will be carried out.
     * <p>
     * Note: This exposes unconstrained access to the journal that could be used
     * to violate the concurrency control mechanisms, therefore you SHOULD NOT
     * use this field unless you have a clear idea what you are about. You
     * should be able to write all application level tasks in terms of
     * {@link #getIndex(String)} and operations on the returned index. Using
     * {@link SequenceTask} you can combine application specific unisolated
     * write tasks with tasks that add or drop indices into atomic operations.
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
     * Flag is cleared if the task is aborted.  This is used to refuse
     * access to resources for tasks that ignore interrupts.
     */
    boolean aborted = false;
    
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
    protected AbstractTask(ConcurrentJournal journal, long startTime,
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
    protected AbstractTask(ConcurrentJournal journal, long startTime,
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

            if (tx.isReadOnly() != readOnly) {

                throw new IllegalArgumentException("Tx readOnly="
                        + tx.isReadOnly() + ", but task readOnly="
                        + readOnly);
                
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
     * {@link Thread#interrupted()} and MUST throw an exception, such as
     * {@link InterruptedException}, if they are interrupted. This behavior
     * allows tasks to be cancelled in a timely manner.
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
    abstract protected Object doTask() throws Exception;

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

        if (!submitted.compareAndSet(false, true)) {

            throw new ResubmitException(getClass().getName());
            
        }
        
        try {
            
            if (isolated) {

                log.info("Running isolated operation: tx="+startTime);
                
                if(tx.isReadOnly()) {

                    try {

                        return doTask();

                    } finally {
                        
                        // release hard references to named read-only indices.
                        
                        indexCache.clear();
                        
                    }

                }
                
                /*
                 * Delegate handles handshaking for writable transactions.
                 */

                Callable<Object> delegate = new InnerReadWriteTxServiceCallable(tx);
                
                return delegate.call();

            }

            if (readOnly) {

                /*
                 * Read only task. We note the most recent commit record so that
                 * we can base all read-only views off of the same consistent
                 * historical state (and so that we do not conflict with
                 * unisolated writers on the live versions of the indices).
                 */

                commitRecord = journal.getCommitRecord();
                
                log.info("Running unisolated reader against committed state: commitCounter="
                                + commitRecord.getCommitCounter()
                                + ", timestamp=" + commitRecord.getTimestamp());
                
                try {

                    return doTask();
                    
                } finally {
                    
                    // release hard references to the named read-only indices.
                    
                    indexCache.clear();
                
                    log.info("Unisolated reader is done");
                    
                }

            } else {

                /*
                 * Handle unisolated write tasks, which need to coordinate with
                 * the lock manager for the unisolated indices.
                 */

                return doReadWriteTask();
                
            }

        } finally {

            log.info("done");

        }
        
    }
    
    /**
     * Call {@link #doTask()} for an unisolated write task.
     * <p>
     * Note: This uses a delegate that coordinates with
     * {@link ConcurrentJournal#lockManager} to force a schedule on tasks that
     * write on unisolated indices.
     * 
     * @throws Exception
     */
    private Object doReadWriteTask() throws Exception {
        
        // lock manager.
        LockManager<String> lockManager = ((ConcurrentJournal) journal).lockManager;

        // resource(s) to lock (exclusive locks are used).

        log.info("Unisolated write task: resources="
                + Arrays.toString(resource));

        // declare resource(s).
        lockManager.addResource(resource);

        // delegate will handle lock acquisition and invoke doTask().
        Callable<Object> delegate = new LockManagerTask<String>(
                lockManager, resource, new InnerWriteServiceCallable());
        
        WriteExecutorService writeService = ((ConcurrentJournal) journal).writeService;

        writeService.beforeTask(Thread.currentThread(), this);

        boolean ran = false;

        try {

            final Object ret;
            
            try {

                /*
                 * Note: The lock(s) are only held during this call. By the time
                 * the call returns any lock(s) have been released. Locks MUST
                 * be released as soon as the task is done writing so that it
                 * does NOT hold locks while it is awaiting commit. This make it
                 * possible for other operations to write on the same index in
                 * the same commit group.
                 */

                ret = delegate.call();

                if(Thread.interrupted()) {
                    
                    throw new InterruptedException();
                    
                }

                // set flag.
                ran = true;
                
                log.info("Task Ok: class="+getClass().getName());
                
                /*
                 * Note: I am choosing NOT to flush dirty indices to the store
                 * after each task in case other tasks in the same commit group
                 * want to write on the same index. Flushing an index makes its
                 * nodes and leaves immutable, and that is not desirable if you
                 * are going to write on it again soon.
                 * 
                 * Note: This trades off against checkpointing the indices after
                 * each task which might make it possible to discard only part
                 * of a commit group.
                 */

            } finally {

                /*
                 * Release hard references to named indices. Dirty indices will
                 * exist on the Name2Addr's commitList until the next commit.
                 */

                indexCache.clear();

            }
            
            /*
             * Note: The WriteServiceExecutor will await a commit signal
             * before the thread is allowed to complete. This ensures
             * that the caller waits until a commit (or an abort).
             * 
             * Note: Waiting here does NOT prevent other tasks from
             * gaining access to the same resources since the locks were
             * released above.
             */ 

            writeService.afterTask(this, null);

            return ret;

        } catch (Throwable t) {

            if (!ran) {

                // Do not re-invoke it afterTask failed above.

                log.info("Task failed: class="+getClass().getName()+" : "+t);
                
                writeService.afterTask(this, t);

            }

            /*
             * Throw whatever exception was thrown by the task (or by afterTask
             * if it craps out).
             */
            
            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

        }

    }

    /**
     * Inner class used to wrap up the call to {@link AbstractTask#doTask()} for
     * {@link IsolationEnum#ReadWrite} transactions.
     * 
     * FIXME take note of which indices the transaction actually _writes_ on and
     * then inform the transaction manager which needs to keep track of that
     * information.
     * 
     * FIXME In order to allow concurrent tasks to do work on the same
     * transaction we need to use a per-transaction {@link LockManager} to
     * produce a partial order that governs access to the isolated (vs mutable
     * unisolated) indices accessed by that transaction.
     * <p>
     * Make sure that the {@link TemporaryRawStore} supports an appropriate
     * level of concurrency to allow concurrent writers on distinct isolated
     * indices that are being buffered on that store for a given transaction
     * (MRMW). Reads are writes are currently serialized in order to support
     * overflow from memory to disk.
     * <p>
     * The {@link Tx} needs to be thread-safe when instantiating the temporary
     * store and when granting a view of an index (the read-committed and
     * read-only txs should also be thread safe in this regard).
     * <p>
     * If an read-write isolated task is interrupted then we MAY need to re-open
     * the {@link TemporaryRawStore} since the interrupt MAY have cause the
     * channel to be closed. See {@link ClosedByInterruptException}.
     * 
     * @todo review problems with memory exhaustion when running junit. the
     *       write cache for the disk-only mode contributes to this, but maybe
     *       the underlying problem is that junit is holding onto all of the
     *       tests - either by itself or because of the proxy test case setup.
     *       <p>
     *       There are also several tests that are apparently attempting to
     *       close a store that is already closed.
     * 
     * @todo do we always inform the transaction manager or only after the
     *       commit when we know that the "writes" took?
     * 
     * @todo do we force the abort of a transaction if any task in that
     *       transaction fails? if we do not abort then we can have partial
     *       writes on the isolated indices, which is very bad. So we either
     *       have to checkpoint the isolated indices with the group commit on
     *       the store so that we can reload from the last metadata address for
     *       an isolated index on abort or we have to abort the entire tx if any
     *       task in the tx fails (at least for a writable transaction).
     */
    class InnerReadWriteTxServiceCallable implements Callable<Object> {

        final ITx tx;
        
        InnerReadWriteTxServiceCallable(ITx tx) {
            
            if (tx == null)
                throw new IllegalArgumentException();
            
            this.tx = tx;
            
        }

        /**
         * Wraps up the execution of {@link AbstractTask#doTask()}.
         */
        public Object call() throws Exception {

            // invoke on the outer class.

            try {

                return doTask();
                
            } finally {
                
                /*
                 * Release hard references to the named indices. The backing
                 * indices are reading from the ground state identified by the
                 * start time of the ReadWrite transaction.
                 */
                
                indexCache.clear();
                
            }
            
        }
        
    }
    
    /**
     * An instance of this class is used as the delegate for a
     * {@link LockManagerTask} in order to coordinate the acquisition of locks
     * with the {@link LockManager} before the task can execute and to release
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
     * @version $Id$
     */
    class InnerWriteServiceCallable implements Callable<Object> {

        InnerWriteServiceCallable() {
        }

        /**
         * 
         */
        public Object call() throws Exception {

            return doTask();
            
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

        if(name==null) {

            throw new NullPointerException();
            
        }
        
        if(aborted) {
            
            /*
             * The task has been interrupted by the write service which also
             * sets the [aborted] flag since the interrupt status may have been
             * cleared by looking at it.
             */

            throw new RuntimeException(new InterruptedException());
            
        }
        
        /*
         * Test the named index cache first.
         */
        {

            final IIndexWithCounter index = indexCache.get(name);

            if (index != null) {

                // Cached value.
                return index;

            }

        }

        // validate that this is a declared index.
        assertResource(name);
        
        final IIndexWithCounter tmp;

        if (isolated) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             */

            final IIsolatedIndex isolatedIndex = (IIsolatedIndex) tx
                    .getIndex(name);

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
                 * The read-only unisolated index is based on the last committed
                 * state of the store.
                 * 
                 * Note: We use this version of the index rather than the "live"
                 * index so that read operations on unisolated indices DO NOT
                 * conflict with write operations on unisolated indices.
                 */

                if (commitRecord.getCommitCounter() == 0) {
                    
                    /*
                     * This catches the edge case where nothing has been
                     * committed onto the journal. In this case it is not
                     * possible to create a read-only unisolated view based on
                     * the last committed state since NOTHING has been
                     * committed.
                     */
                    
                    throw new NoSuchIndexException(name);
                    
                }
                
                /*
                 * This is a historical state for the named index.
                 */
                
                IIndex foo = journal.getIndex(name,commitRecord);
                
                if (foo == null) {
                    
                    throw new NoSuchIndexException(name);

                }
                
                /*
                 * Wrap the index such that attempts to write on it will cause
                 * an UnsupportedOperationException to be thrown.
                 */

                tmp = new ReadOnlyIndex(foo);
            
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock system is used to ensure that at most one
                 * task has access to this index at a time.
                 */
                
                tmp = (BTree) journal.getIndex(name);

                if (tmp == null) {

                    throw new NoSuchIndexException(name);

                }

            }

        }

        /*
         * Put the index into a hard reference cache under its name so that we
         * can hold onto it for the duration of the operation.
         */

        indexCache.put(name,tmp);

        return tmp;

    }

    /**
     * This is thrown if you attempt to reuse (re-submit) the same
     * {@link AbstractTask} instance.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ResubmitException extends RejectedExecutionException {

        /**
         * 
         */
        private static final long serialVersionUID = -8661545948587322943L;
        
        public ResubmitException() {
            super();
        }
        
        public ResubmitException(String msg) {
            
            super(msg);
            
        }

    }
    
}
