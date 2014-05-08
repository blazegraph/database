/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.blueprints;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * This is a thin-client implementation of a Blueprints wrapper around the
 * client library that interacts with the NanoSparqlServer.  This is a functional
 * implementation suitable for writing POCs - it is not a high performance
 * implementation by any means (currently does not support caching, batched
 * update, or Blueprints query re-writes).  Does have a single "bulk upload"
 * operation that wraps a method on RemoteRepository that will POST a graphml
 * file to the blueprints layer of the bigdata server.
 * 
 * @see {@link BigdataSailRemoteRepository}
 * @see {@link BigdataSailRemoteRepositoryConnection}
 * @see {@link RemoteRepository}
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphClient extends BigdataGraph {

	final BigdataSailRemoteRepository repo;
	
	transient BigdataSailRemoteRepositoryConnection cxn;
	
    public BigdataGraphClient(final String bigdataEndpoint) {
        this(bigdataEndpoint, BigdataRDFFactory.INSTANCE);
    }
    
    public BigdataGraphClient(final String bigdataEndpoint, 
            final BlueprintsRDFFactory factory) {
        this(new BigdataSailRemoteRepository(bigdataEndpoint), factory);
    }
	
	public BigdataGraphClient(final RemoteRepository repo) {
		this(repo, BigdataRDFFactory.INSTANCE);
	}
	
	public BigdataGraphClient(final RemoteRepository repo, 
			final BlueprintsRDFFactory factory) {
	    this(new BigdataSailRemoteRepository(repo), factory);
	}
	
    public BigdataGraphClient(final BigdataSailRemoteRepository repo) {
        this(repo, BigdataRDFFactory.INSTANCE);
    }
    
    public BigdataGraphClient(final BigdataSailRemoteRepository repo, 
            final BlueprintsRDFFactory factory) {
        super(factory);
        
        this.repo = repo;
    }
    
    /**
     * Post a GraphML file to the remote server. (Bulk-upload operation.)
     */
    @Override
    public void loadGraphML(final String file) throws Exception {
        this.repo.getRemoteRepository().postGraphML(file);
    }
    
    /**
     * Get a {@link BigdataSailRemoteRepositoryConnection}.
     */
	protected BigdataSailRemoteRepositoryConnection cxn() throws Exception {
	    if (cxn == null) {
	        cxn = repo.getConnection();
	    }
	    return cxn;
	}
	
	/**
	 * Shutdown the connection and repository (client-side, not server-side).
	 */
	@Override
	public void shutdown() {
		try {
		    if (cxn != null) {
		        cxn.close();
		    }
			repo.shutDown();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
