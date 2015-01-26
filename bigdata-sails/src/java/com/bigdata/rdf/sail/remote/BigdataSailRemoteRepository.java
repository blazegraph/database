/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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

package com.bigdata.rdf.sail.remote;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jetty.client.HttpClient;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;

import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * An implementation of Sesame's {@link Repository} that wraps a bigdata
 * {@link RemoteRepository}. This provides SAIL API based client access to
 * a bigdata remote NanoSparqlServer.
 * <p>
 * This implementation operates only in auto-commit mode (each mutation
 * operation results in a commit on the server).
 * <p>
 * This implementation also throws UnsupportedOperationExceptions all over the
 * place due to incompatibilities with our own remoting interface. If there is
 * something important that you need implemented for your application don't be
 * afraid to reach out and contact us.
 */
public class BigdataSailRemoteRepository implements Repository {

	/**
	 * Initially open (aka initialized).
	 */
	private volatile boolean open = true;
	
	/**
	 * non-<code>null</code> iff the executor is allocated by the constructor,
	 * in which case it is scoped to the life cycle of this
	 * {@link BigdataSailRemoteRepository} instance.
	 */
    private final ExecutorService executor;
    
	/**
	 * non-<code>null</code> iff the executor is allocated by the constructor,
	 * in which case it is scoped to the life cycle of this
	 * {@link BigdataSailRemoteRepository} instance.
	 */
	private final HttpClient client;

	/**
	 * Always non-<code>null</code>. This is either an object that we have
	 * created ourselves or one that was passed in by the caller. If we create
	 * this object, then we own its life cycle and will close it when this
	 * object is closed. In this case it will also be a
	 * {@link RemoteRepositoryManager} rather than just a
	 * {@link RemoteRepository}.
	 */
    private final RemoteRepository nss;

    /**
     * The object used to communicate with that remote repository.
     */
	public RemoteRepository getRemoteRepository() {
		
		return nss;
		
	}
	
	/**
	 * Ctor that simply specifies an endpoint and lets this class manage the
	 * ClientConnectionManager for the HTTP client and the manage the
	 * ExecutorService. More convenient, but does not account for whether or not
	 * to use the LBS.
	 * 
	 * @param sparqlEndpointURL
	 *            The SPARQL end point URL
	 */
	public BigdataSailRemoteRepository(final String sparqlEndpointURL) {

		this(sparqlEndpointURL, true/* useLBS */);

	}

	/**
	 * Ctor that simply specifies an endpoint and lets this class manage the
	 * ClientConnectionManager for the HTTP client and the manage the
	 * ExecutorService.
	 * 
	 * @param sparqlEndpointURL
	 *            The SPARQL end point URL
	 * @param useLBS
	 *            <code>true</code> iff the LBS pattern should be used.
	 */
	public BigdataSailRemoteRepository(final String sparqlEndpointURL,
			final boolean useLBS) {

		if (sparqlEndpointURL == null)
			throw new IllegalArgumentException();

		this.executor = Executors.newCachedThreadPool();

		// Note: Client *might* be AutoCloseable.
		this.client = HttpClientConfigurator.getInstance().newInstance();

		this.nss = new RemoteRepositoryManager(sparqlEndpointURL, useLBS,
				client, executor);
	
	}

	/**
	 * Ctor that allows the caller to manage the ClientConnectionManager for 
	 * the HTTP client and the manage the ExecutorService. More flexible.  
	 */
	public BigdataSailRemoteRepository(final RemoteRepository nss) {

		if (nss == null)
			throw new IllegalArgumentException();
		
		// use the executor on the caller's object.
		this.executor = null;
		
		// use the client on the caller's object.
		this.client = null;

		this.nss = nss;
		
	}
	
	@Override
	synchronized
	public void shutDown() throws RepositoryException {

		if (executor != null) {

			executor.shutdownNow();

			assert client != null;

			if (!(client instanceof AutoCloseable)) {
				/*
				 * The client does not implement AutoCloseable and it we own the
				 * life cycle of the client (because it was created by our
				 * constructor). In this case we need to invoke the jetty
				 * close() method on the client to avoid leaking resources.
				 */
				try {
					client.stop();
				} catch (Exception e) {
					throw new RepositoryException(e);
				}
			}

			/*
			 * The JettyRemoteRepositoryManager object was create by our
			 * constructor, so we own its life cycle and will close it down
			 * here.
			 */

			assert nss instanceof AutoCloseable;

			try {

				((AutoCloseable) nss).close();

			} catch (Exception e) {

				throw new RepositoryException(e);

			}

			open = false;
			
		}
		
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

	@Override
	public void setDataDir(File arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public File getDataDir() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ValueFactory getValueFactory() {
		throw new UnsupportedOperationException();
	}

}
