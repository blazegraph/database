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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Abstract base class for REST API methods. This class is compatible with a
 * job-oriented concurrency control pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
 *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
 * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
 *      Concurrent unisolated operations against multiple KBs </a>
 * 
 *      FIXME GROUP COMMIT: Define DropKBTask and CreateKBTask for use by (a)
 *      the multi-tenancy API; and (b) variants without servlet request and
 *      response parameters for use by the unit tests and the NSS during its
 *      default KB create logic. These latter tasks should be a base class of
 *      the RestApiTask that supports the same delegation pattern, but which
 *      does not require the http request and response parameters. Fix up the
 *      callers of CreateKBTask and tripleStore.destroy() to use these tasks.
 */
abstract class RestApiTask<T> implements Callable<T> {

    private final AtomicReference<IIndexManager> indexManagerRef = new AtomicReference<IIndexManager>();
    /** The {@link HttpServletRequest}. */
    protected final HttpServletRequest req;
    /** The {@link HttpServletResponse}. */
    protected final HttpServletResponse resp;
    /** The namespace of the target KB instance. */
    protected final String namespace;
    /** The timestamp of the view of that KB instance. */
    protected final long timestamp;

    /** The namespace of the target KB instance. */
    public String getNamespace() {
        return namespace;
    }

    /** The timestamp of the view of that KB instance. */
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * @param req
     *            The {@link HttpServletRequest}.
     * @param resp
     *            The {@link HttpServletResponse}.
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp of the view of that KB instance.
     */
    protected RestApiTask(final HttpServletRequest req,
            final HttpServletResponse resp, final String namespace,
            final long timestamp) {
        this.req = req;
        this.resp = resp;
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

    protected void reportModifiedCount(final long nmodified, final long elapsed)
            throws IOException {

        BigdataRDFServlet.reportModifiedCount(resp, nmodified, elapsed);

    }

    protected BigdataSailRepositoryConnection getReadOnlyConnection() {

        throw new UnsupportedOperationException();

    }

    protected BigdataSailRepositoryConnection getReadWriteConnection()
            throws SailException, RepositoryException {

        throw new UnsupportedOperationException();

    }

    /**
     * Return a read-only view of the {@link AbstractTripleStore} for the given
     * namespace will read from the commit point associated with the given
     * timestamp.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            The timestamp.
     * 
     * @return The {@link AbstractTripleStore} -or- <code>null</code> if none is
     *         found for that namespace and timestamp.
     * 
     *         TODO Enforce historical query by making sure timestamps conform
     *         (we do not want to allow read/write tx queries unless update
     *         semantics are introduced ala SPARQL 1.1).
     */
    protected AbstractTripleStore getTripleStore(final String namespace,
            final long timestamp) {

        // if (timestamp == ITx.UNISOLATED)
        // throw new IllegalArgumentException("UNISOLATED reads disallowed.");

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
     */
    protected BigdataSailRepositoryConnection getUnisolatedConnection()
            throws SailException, RepositoryException {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            throw new RuntimeException("Not found: namespace=" + namespace);

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

    abstract static class RestApiQueryTask<T> extends RestApiTask<T> {
        /**
         * 
         * @param req
         *            The {@link HttpServletRequest}.
         * @param resp
         *            The {@link HttpServletResponse}.
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a query connection.
         */
        public RestApiQueryTask(final HttpServletRequest req,
                final HttpServletResponse resp,
                final String namespace, final long timestamp) {

            super(req, resp, namespace, timestamp);
            
        }

    }

    abstract static class RestApiMutationTask<T> extends RestApiTask<T> {
        /**
         * 
         * @param req
         *            The {@link HttpServletRequest}.
         * @param resp
         *            The {@link HttpServletResponse}.
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         */
        public RestApiMutationTask(final HttpServletRequest req,
                final HttpServletResponse resp,
                final String namespace, final long timestamp) {

            super(req, resp, namespace, timestamp);
            
        }

    }
    
}
