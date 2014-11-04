/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
package com.bigdata.rdf.task;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IReadOnly;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.DatasetNotFoundException;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Base class is non-specific. Directly derived classes are suitable for
 * internal tasks (stored queries, stored procedures, etc) while REST API tasks
 * are based on a specialized subclass that also provides for access to the HTTP
 * request and response.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="http://trac.bigdata.com/ticket/566" > Concurrent unisolated
 *      operations against multiple KBs </a>
 */
abstract public class AbstractApiTask<T> implements IApiTask<T>, IReadOnly {

    /** The reference to the {@link IIndexManager} is set before the task is executed. */
    private final AtomicReference<IIndexManager> indexManagerRef = new AtomicReference<IIndexManager>();

    /** The namespace of the target KB instance. */
    protected final String namespace;
    
    /** The timestamp of the view of that KB instance. */
    protected final long timestamp;

    @Override
    abstract public boolean isReadOnly();
    
    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp of the view of that KB instance.
     */
    protected AbstractApiTask(final String namespace,
            final long timestamp) {
        this.namespace = namespace;
        this.timestamp = timestamp;
    }

    protected void clearIndexManager() {

        indexManagerRef.set(null);

    }

    protected void setIndexManager(final IIndexManager indexManager) {

        if (!indexManagerRef
                .compareAndSet(null/* expect */, indexManager/* update */)) {

            throw new IllegalStateException();

        }

    }

    protected IIndexManager getIndexManager() {

        final IIndexManager tmp = indexManagerRef.get();

        if (tmp == null)
            throw new IllegalStateException();

        return tmp;

    }

    /**
     * Return a view of the {@link AbstractTripleStore} for the given namespace
     * that will read on the commit point associated with the given timestamp.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            The timestamp or {@link ITx#UNISOLATED} to obtain a read/write
     *            view of the index.
     * 
     * @return The {@link AbstractTripleStore} -or- <code>null</code> if none is
     *         found for that namespace and timestamp.
     */
    protected AbstractTripleStore getTripleStore(final String namespace,
            final long timestamp) {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, timestamp);

        return tripleStore;

    }

    /**
     * Return a connection transaction, which may be either read-only or support
     * mutation depending on the timestamp associated with the task's view. When
     * the timestamp is associated with a historical commit point, this will be
     * a read-only connection. When it is associated with the
     * {@link ITx#UNISOLATED} view or a read-write transaction, this will be a
     * mutable connection.
     * <p>
     * This version uses the namespace and timestamp associated with the HTTP
     * request.
     * 
     * @throws RepositoryException
     */
    protected BigdataSailRepositoryConnection getQueryConnection()
            throws RepositoryException {

        /*
         * Note: [timestamp] will be a read-only tx view of the triple store if
         * a READ_LOCK was specified when the NanoSparqlServer was started
         * (unless the query explicitly overrides the timestamp of the view on
         * which it will operate).
         */
		return getQueryConnection(namespace, timestamp);

	}

    /**
     * This version uses the namespace and timestamp provided by the caller.

     * @param namespace
     * @param timestamp
     * @return
     * @throws RepositoryException
     */
	protected BigdataSailRepositoryConnection getQueryConnection(
			final String namespace, final long timestamp)
			throws RepositoryException {

        final AbstractTripleStore tripleStore = getTripleStore(namespace,
                timestamp);

        if (tripleStore == null) {

            throw new DatasetNotFoundException("Not found: namespace="
                    + namespace + ", timestamp="
                    + TimestampUtility.toString(timestamp));

        }

        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        if (TimestampUtility.isReadOnly(timestamp)) {

            return (BigdataSailRepositoryConnection) repo
                    .getReadOnlyConnection(timestamp);

        }

        // Read-write connection.
        final BigdataSailRepositoryConnection conn = repo.getConnection();

        conn.setAutoCommit(false);

        return conn;

    }

    /**
	 * Return an UNISOLATED connection.
	 * 
	 * @return The UNISOLATED connection.
	 * 
	 * @throws SailException
	 * @throws RepositoryException
	 * @throws DatasetNotFoundException
	 *             if the specified namespace does not exist.
	 */
    protected BigdataSailRepositoryConnection getUnisolatedConnection()
            throws SailException, RepositoryException {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

			throw new DatasetNotFoundException("Not found: namespace="
					+ namespace);

        }

        // Wrap with SAIL.
        final BigdataSail sail = new BigdataSail(tripleStore);

        final BigdataSailRepository repo = new BigdataSailRepository(sail);

        repo.initialize();

        final BigdataSailRepositoryConnection conn = (BigdataSailRepositoryConnection) repo
                .getUnisolatedConnection();

        conn.setAutoCommit(false);

        return conn;

    }
    
    /**
     * Submit a task and return a {@link Future} for that task. The task will be
     * run on the appropriate executor service depending on the nature of the
     * backing database and the view required by the task.
     * 
     * @param indexManager
     *            The {@link IndexManager}.
     * @param task
     *            The task.
     * 
     * @return The {@link Future} for that task.
     * 
     * @throws DatasetNotFoundException
     * 
     * @see <a href="http://trac.bigdata.com/ticket/753" > HA doLocalAbort()
     *      should interrupt NSS requests and AbstractTasks </a>
     * @see <a href="http://trac.bigdata.com/ticket/566" > Concurrent unisolated
     *      operations against multiple KBs </a>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static public <T> Future<T> submitApiTask(
            final IIndexManager indexManager, final AbstractApiTask<T> task)
            throws DatasetNotFoundException {

        final String namespace = task.getNamespace();
        
        final long timestamp = task.getTimestamp();
        
        if (!BigdataStatics.NSS_GROUP_COMMIT || indexManager instanceof IBigdataFederation
                || TimestampUtility.isReadOnly(timestamp)
                ) {

            /*
             * Execute the REST API task.
             * 
             * Note: For scale-out, the operation will be applied using
             * client-side global views of the indices. This means that
             * there will not be any globally consistent views of the
             * indices and that updates will be shard-wise local.
             * 
             * Note: This can be used for operations on read-only views (even on
             * a Journal). This is helpful since we can avoid some overhead
             * associated the AbstractTask lock declarations.
             */
            // Wrap Callable.
            final FutureTask<T> ft = new FutureTask<T>(
                    new ApiTaskForIndexManager(indexManager, task));

            if (true) {

                /*
                 * Caller runs (synchronous execution)
                 * 
                 * Note: By having the caller run the task here we avoid
                 * consuming another thread.
                 */
                ft.run();
                
            } else {
                
                /*
                 * Run on a normal executor service.
                 */
                indexManager.getExecutorService().submit(ft);
                
            }

            return ft;

        } else {

            /**
             * Run on the ConcurrencyManager of the Journal.
             * 
             * Mutation operations will be scheduled based on the pre-declared
             * locks and will have exclusive access to the resources guarded by
             * those locks when they run.
             * 
             * FIXME GROUP COMMIT: The hierarchical locking mechanisms will fail
             * on durable named solution sets because they use either HTree or
             * Stream and AbstractTask does not yet support those durable data
             * structures (it is still being refactored to support the
             * ICheckpointProtocol rather than the BTree in its Name2Addr
             * isolation logic).
             */

            // Obtain the necessary locks for R/w access to KB indices.
            final String[] locks = getLocksForKB((Journal) indexManager,
                    namespace);

            final IConcurrencyManager cc = ((Journal) indexManager)
                    .getConcurrencyManager();
            
            // Submit task to ConcurrencyManager. Will acquire locks and run.
            return cc.submit(new ApiTaskForJournal(cc, task.getTimestamp(),
                    locks, task));

        }

    }
    
    /**
     * Return the set of locks that the task must acquire in order to operate on
     * the specified namespace.
     * 
     * @param indexManager
     *            The {@link Journal}.
     * @param namespace
     *            The namespace of the KB instance.
     * 
     * @return The locks for the named indices associated with that KB instance.
     * 
     * @throws DatasetNotFoundException
     */
    private static String[] getLocksForKB(final Journal indexManager,
            final String namespace) throws DatasetNotFoundException {

        /*
         * Note: There are two possible approaches here. One is to explicitly
         * enumerate the index names for the triple store. The other is to
         * specify the namespace of the triple store and use hierarchical
         * locking.
         * 
         * This is now using hierarchical locking, so it just returns the
         * namespace.
         */
        return new String[]{namespace};
        
//        final long timestamp = indexManager.getLastCommitTime();
//
//        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//                .getResourceLocator().locate(namespace, timestamp);
//
//        if (tripleStore == null)
//            throw new DatasetNotFoundException("Not found: namespace="
//                    + namespace + ", timestamp="
//                    + TimestampUtility.toString(timestamp));
//
//        final Set<String> lockSet = new HashSet<String>();
//
//        lockSet.addAll(tripleStore.getSPORelation().getIndexNames());
//
//        lockSet.addAll(tripleStore.getLexiconRelation().getIndexNames());
//
//        final String[] locks = lockSet.toArray(new String[lockSet.size()]);
//
//        return locks;

    }
    
}
