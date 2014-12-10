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

import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.JettyHttpClient;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

/**
 * An implementation of Sesame's RepositoryConnection interface that wraps a
 * bigdata RemoteRepository. This provides SAIL API based client access to a
 * bigdata remote NanoSparqlServer. This implementation operates only in
 * auto-commit mode (each mutation operation results in a commit on the server).
 * It also throws UnsupportedOperationExceptions all over the place due to
 * incompatibilities with our own remoting interface. If there is something
 * important that you need implemented for your application don't be afraid to
 * reach out and contact us.
 * 
 * TODO Implement buffering of adds and removes so that we can turn off
 * 		auto-commit. 
 * TODO Fix all the Query objects (TupleQuery, GraphQuery,
 * 		BooleanQuery) to support the various possible operations on them, such as
 * 		setting a binding. 
 * TODO Support baseURIs
 */
public class BigdataSailRemoteRepository implements Repository {

    private final ExecutorService executor;
    
    private final JettyRemoteRepository nss;

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
		
        this.executor = Executors.newCachedThreadPool();

        this.nss = new JettyRemoteRepositoryManager(sparqlEndpointURL, useLBS,
                executor);
	}

	/**
	 * Ctor that allows the caller to manage the ClientConnectionManager for 
	 * the HTTP client and the manage the ExecutorService. More flexible.  
	 */
	public BigdataSailRemoteRepository(final JettyRemoteRepository nss) {
		
		this.executor = null;
		
		this.nss = nss;
	}
	
	public JettyRemoteRepository getRemoteRepository() {
		
		return nss;
		
	}
	
	@Override
	public void shutDown() throws RepositoryException {

		// FIXME: this should be handled more cleanly
		if (nss instanceof JettyRemoteRepositoryManager) {
			((JettyRemoteRepositoryManager) nss).close();
		} else {
			System.err.println("DEBUG: Not closing JettyRemoteRepository");
		}
		
		if (executor != null)
			executor.shutdownNow();
		
	}

	@Override
	public BigdataSailRemoteRepositoryConnection getConnection() 
	        throws RepositoryException {
		
		return new BigdataSailRemoteRepositoryConnection(this);
		
	}

	@Override
	public void initialize() throws RepositoryException {
		// noop
	}

	@Override
	public boolean isInitialized() {
		return true;
	}

	@Override
	public boolean isWritable() throws RepositoryException {
		return true;
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
