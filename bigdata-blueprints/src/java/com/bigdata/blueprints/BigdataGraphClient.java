/**
Copyright (C) SYSTAP, LLC 2006-Infinity.  All rights reserved.

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

import java.util.Properties;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepository;
import com.tinkerpop.blueprints.Features;

/**
 * This is a thin-client implementation of a Blueprints wrapper around the
 * client library that interacts with the NanoSparqlServer. This is a functional
 * implementation suitable for writing POCs - it is not a high performance
 * implementation by any means (currently does not support caching or batched
 * update). Does have a single "bulk upload" operation that wraps a method on
 * RemoteRepository that will POST a graphml file to the blueprints layer of the
 * bigdata server.
 * 
 * @see {@link BigdataSailRemoteRepository}
 * @see {@link BigdataSailRemoteRepositoryConnection}
 * @see {@link JettyRemoteRepository}
 * 
 * @author mikepersonick
 * 
 */
public class BigdataGraphClient extends BigdataGraph {

    private static final Properties props = new Properties();
    static {
        /*
         * We don't want the BigdataGraph to close our connection after every
         * read.  The BigdataGraphClient represents a session with the server.
         */
        props.setProperty(BigdataGraph.Options.READ_FROM_WRITE_CONNECTION, "true");
    }
    
	final BigdataSailRemoteRepository repo;
	
	transient BigdataSailRemoteRepositoryConnection cxn;
	
    public BigdataGraphClient(final String bigdataEndpoint) {
        this(bigdataEndpoint, BigdataRDFFactory.INSTANCE);
    }
    
    public BigdataGraphClient(final String bigdataEndpoint, 
            final BlueprintsValueFactory factory) {
        this(new BigdataSailRemoteRepository(bigdataEndpoint), factory);
    }
	
	public BigdataGraphClient(final JettyRemoteRepository repo) {
		this(repo, BigdataRDFFactory.INSTANCE);
	}
	
	public BigdataGraphClient(final JettyRemoteRepository repo, 
			final BlueprintsValueFactory factory) {
	    this(new BigdataSailRemoteRepository(repo), factory);
	}
	
    public BigdataGraphClient(final BigdataSailRemoteRepository repo) {
        this(repo, BigdataRDFFactory.INSTANCE);
    }
    
    public BigdataGraphClient(final BigdataSailRemoteRepository repo, 
            final BlueprintsValueFactory factory) {
        super(factory, props);
        
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
	protected BigdataSailRemoteRepositoryConnection getWriteConnection() throws Exception {
	    if (cxn == null) {
	        cxn = repo.getConnection();
	    }
	    return cxn;
	}
	
    /**
     * Get a {@link BigdataSailRemoteRepositoryConnection}. No difference in
     * connection for remote clients.
     */
    protected BigdataSailRemoteRepositoryConnection getReadConnection() throws Exception {
        return getWriteConnection();
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
	
    protected static final Features FEATURES = new Features();

    @Override
    public Features getFeatures() {

        return FEATURES;
        
    }
    
    static {
        
        FEATURES.supportsSerializableObjectProperty = BigdataGraph.FEATURES.supportsSerializableObjectProperty;
        FEATURES.supportsBooleanProperty = BigdataGraph.FEATURES.supportsBooleanProperty;
        FEATURES.supportsDoubleProperty = BigdataGraph.FEATURES.supportsDoubleProperty;
        FEATURES.supportsFloatProperty = BigdataGraph.FEATURES.supportsFloatProperty;
        FEATURES.supportsIntegerProperty = BigdataGraph.FEATURES.supportsIntegerProperty;
        FEATURES.supportsPrimitiveArrayProperty = BigdataGraph.FEATURES.supportsPrimitiveArrayProperty;
        FEATURES.supportsUniformListProperty = BigdataGraph.FEATURES.supportsUniformListProperty;
        FEATURES.supportsMixedListProperty = BigdataGraph.FEATURES.supportsMixedListProperty;
        FEATURES.supportsLongProperty = BigdataGraph.FEATURES.supportsLongProperty;
        FEATURES.supportsMapProperty = BigdataGraph.FEATURES.supportsMapProperty;
        FEATURES.supportsStringProperty = BigdataGraph.FEATURES.supportsStringProperty;
        FEATURES.supportsDuplicateEdges = BigdataGraph.FEATURES.supportsDuplicateEdges;
        FEATURES.supportsSelfLoops = BigdataGraph.FEATURES.supportsSelfLoops;
        FEATURES.isPersistent = BigdataGraph.FEATURES.isPersistent;
        FEATURES.isWrapper = BigdataGraph.FEATURES.isWrapper;
        FEATURES.supportsVertexIteration = BigdataGraph.FEATURES.supportsVertexIteration;
        FEATURES.supportsEdgeIteration = BigdataGraph.FEATURES.supportsEdgeIteration;
        FEATURES.supportsVertexIndex = BigdataGraph.FEATURES.supportsVertexIndex;
        FEATURES.supportsEdgeIndex = BigdataGraph.FEATURES.supportsEdgeIndex;
        FEATURES.ignoresSuppliedIds = BigdataGraph.FEATURES.ignoresSuppliedIds;
//        FEATURES.supportsTransactions = BigdataGraph.FEATURES.supportsTransactions;
        FEATURES.supportsIndices = BigdataGraph.FEATURES.supportsIndices;
        FEATURES.supportsKeyIndices = BigdataGraph.FEATURES.supportsKeyIndices;
        FEATURES.supportsVertexKeyIndex = BigdataGraph.FEATURES.supportsVertexKeyIndex;
        FEATURES.supportsEdgeKeyIndex = BigdataGraph.FEATURES.supportsEdgeKeyIndex;
        FEATURES.supportsEdgeRetrieval = BigdataGraph.FEATURES.supportsEdgeRetrieval;
        FEATURES.supportsVertexProperties = BigdataGraph.FEATURES.supportsVertexProperties;
        FEATURES.supportsEdgeProperties = BigdataGraph.FEATURES.supportsEdgeProperties;
        FEATURES.supportsThreadedTransactions = BigdataGraph.FEATURES.supportsThreadedTransactions;
        
        // override
        FEATURES.supportsTransactions = false; //BigdataGraph.FEATURES.supportsTransactions;
        
    }

}
