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

package com.bigdata.rdf.sail.remote;

import java.io.File;
import java.util.Collection;
import java.util.UUID;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * An fully compliant implementation of Sesame's {@link Repository} that wraps
 * the blazegraph remote API. Additional functionality is available using the
 * blazegraph {@link RemoteRepositoryManager}. The proper incantation to create
 * an instance of this class is:
 * 
 * <pre>
 * // Create client for the remote service.
 * final RemoteRepositoryManager mgr = new RemoteRepositoryManager(serviceURL);
 * try {
 *    // Obtain a Sesame Repository for the default sparql endpoint on that
 *    // service.
 *    final Repository repo = mgr.getRepositoryForDefaultNamespace()
 *          .getBigdataSailRemoteRepository();
 *    try {
 *       doWork(repo);
 *    } finally {
 *       repo.close();
 *    }
 * } finally {
 *    mgr.close();
 * }
 * </pre>
 * 
 * This pattern makes it possible to:
 * <ul>
 * <li>Obtain {@link BigdataSailRemoteRepository} objects for different sparql
 * end points on the same blazegraph server.</li>
 * <li>Those {@link BigdataSailRemoteRepository} objects are flyweight.</li>
 * <li>The {@link RemoteRepositoryManager} can be used to access additional
 * interfaces, including the multi-tenancy API and the transaction API.</li>
 * </ul>
 * 
 * <h2>Transactions</h2>
 * 
 * The database supports read/write transactions since 1.5.2. Transaction
 * manager is at the database layer, not the {@link Repository} or
 * {@link RepositoryConnection}. Therefore a namespace DOES NOT need to be
 * configured for isolatable indices in order to create and manipulate
 * transactions, but it DOES need to be configured with isolatable indices in
 * order for you to WRITE on the namespace using a transaction.
 * 
 * @see com.bigdata.rdf.sail.webapp.client.RemoteTransactionManager
 * @see com.bigdata.rdf.sail.BigdataSail.Options#ISOLATABLE_INDICES
 * @see BigdataSailRemoteRepositoryConnection
 */
public class BigdataSailRemoteRepository implements Repository {

	/**
	 * Initially open (aka initialized).
	 */
	private volatile boolean open = true;
	
   /**
    * non-<code>null</code> iff the {@link RemoteRepositoryManager} is allocated
    * by the constructor, in which case it is scoped to the life cycle of this
    * {@link BigdataSailRemoteRepository} instance.
    */
   private final RemoteRepositoryManager our_mgr;
   
	/**
	 * Always non-<code>null</code>. This is either an object that we have
	 * created ourselves or one that was passed in by the caller. If we create
	 * this object, then we own its life cycle and will close it when this
	 * object is closed. In this case it will also be a
	 * {@link RemoteRepositoryManager} rather than just a
	 * {@link RemoteRepository}.
	 */
    private final RemoteRepository remoteRepository;

    /**
     * This exists solely for {@link #getValueFactory()} - the value factory
     * is not used inside of this class.
     */
    private final ValueFactory valueFactory = ValueFactoryImpl.getInstance();
    
    /**
     * The object used to communicate with that remote repository.
     */
    public RemoteRepository getRemoteRepository() {
		
		return remoteRepository;
		
	}
	
   /**
    * Constructor that simply specifies an endpoint. This class will internally
    * allocate a {@link RemoteRepositoryManager} that is scoped to the life
    * cycle of this class. The allocated {@link RemoteRepositoryManager} will be
    * closed when this {@link BigdataSailRemoteRepository} is closed.
    * <p>
    * Note: This constructor pattern is NOT flyweight.
    * 
    * @param sparqlEndpointURL
    *           The SPARQL end point URL
    */
    @Deprecated // This is broken because the sparqlEndpointURL is not the serviceURL and that is what the RRM expects/needs.
	public BigdataSailRemoteRepository(final String sparqlEndpointURL) {

       if (sparqlEndpointURL == null)
          throw new IllegalArgumentException();

       /*
        * Allocate a RemoteRepositoryManager. This is NOT a flyweight operation.
        * 
        */
       this.our_mgr = new RemoteRepositoryManager(sparqlEndpointURL, false/*useLBS*/);

       this.remoteRepository = our_mgr.getRepositoryForURL(sparqlEndpointURL);

    }
    
   /**
    * Flyweight constructor wraps the blazegraph remote client for a SPARQL
    * endpoint as an openrdf {@link Repository}.
    * 
    * @see RemoteRepository#getBigdataSailRemoteRepository()
    */
	public BigdataSailRemoteRepository(final RemoteRepository repo) {

		if (repo == null)
			throw new IllegalArgumentException();
		
		// use the manager associated with the provided client.
		this.our_mgr = null;
		
		this.remoteRepository = repo;
		
	}
	
	@Override
   synchronized public void shutDown() throws RepositoryException {

      if (our_mgr != null) {

         /*
          * The RemoteRepositoryManager object was create by our constructor, so
          * we own its life cycle and will close it down here.
          */

         try {

            our_mgr.close();

         } catch (Exception e) {

            throw new RepositoryException(e);

         }

      }

      open = false;

   }

	@Override
	public BigdataSailRemoteRepositoryConnection getConnection() 
	        throws RepositoryException {
		
		return new BigdataSailRemoteRepositoryConnection(this);
		
	}

	@Override
	public void initialize() throws RepositoryException {
		if (!open)
			throw new RepositoryException("Can not re-initialize");
	}

	@Override
	public boolean isInitialized() {
		return open;
	}

	@Override
	public boolean isWritable() throws RepositoryException {
		return open;
	}

	/**
	 * Unsupported operation
	 * 
	 * @throws UnsupportedOperationException
	 */
	@Override
	public void setDataDir(File dataDir) {
		throw new UnsupportedOperationException();
	}

   /**
    * Unsupported operation
    * 
    * @throws UnsupportedOperationException
    */
	@Override
	public File getDataDir() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Return a client-only {@link ValueFactory} implementation.
	 */
	@Override
	public ValueFactory getValueFactory() {
		return valueFactory;
	}
	
	/**
	 * Cancel the query specified by the UUID. 
	 * 
	 * @param queryId
	 * @throws Exception
	 */
	public void cancel(UUID queryId) throws Exception {
		
		this.our_mgr.cancel(queryId);
		
	}
	
	/**
	 * Return a collection containing Metadata objects about the
	 * currently running queries. 
	 * 
	 * @return
	 * @throws Exception
	 */
	public Collection<RunningQuery> showQueries() throws Exception {
		
		return this.our_mgr.showQueries();
		
	}

}
