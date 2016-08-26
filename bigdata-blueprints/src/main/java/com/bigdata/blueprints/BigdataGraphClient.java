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
package com.bigdata.blueprints;

import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sail.remote.BigdataSailFactory;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryType;
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
 * @see {@link RemoteRepository}
 * 
 * @author mikepersonick
 * 
 */
public class BigdataGraphClient extends BigdataGraph {
	
    private static final transient Logger log = Logger.getLogger(BigdataGraphClient.class);

    private static final Properties props = new Properties();

//    static {
//        /*
//         * We don't want the BigdataGraph to close our connection after every
//         * read.  The BigdataGraphClient represents a session with the server.
//         */
//        props.setProperty(BigdataGraph.Options.READ_FROM_WRITE_CONNECTION, "true");
//    }
    
	final BigdataSailRemoteRepository repo;
	
	transient BigdataSailRemoteRepositoryConnection cxn;

   /**
    * 
    * @param sparqlEndpointURL
    *           The URL of the SPARQL end point. This will be used to read and
    *           write on the graph using the blueprints API.
    */
   public BigdataGraphClient(final String sparqlEndpointURL) {
     
      this(sparqlEndpointURL, BigdataRDFFactory.INSTANCE);
      
   }

   /**
    * 
    * @param sparqlEndpointURL
    *           The URL of the SPARQL end point. This will be used to read and
    *           write on the graph using the blueprints API.
    * @param factory
    *           The {@link BlueprintsValueFactory}.
    */
    public BigdataGraphClient(final String sparqlEndpointURL, 
            final BlueprintsValueFactory factory) {

       this(BigdataSailFactory.connect(sparqlEndpointURL), factory);
       
    }
	
	public BigdataGraphClient(final RemoteRepository repo) {
		this(repo, BigdataRDFFactory.INSTANCE);
	}
	
	public BigdataGraphClient(final RemoteRepository repo, 
			final BlueprintsValueFactory factory) {
	    this(repo.getBigdataSailRemoteRepository(), factory);
	}
	
    public BigdataGraphClient(final BigdataSailRemoteRepository repo) {
        this(repo, BigdataRDFFactory.INSTANCE);
    }
    
   /**
    * Core implementation.
    * 
    * @param repo
    *           The {@link BigdataSailRemoteRepository} for the desired graph.
    * @param factory
    *           The {@link BlueprintsValueFactory}.
    */
   public BigdataGraphClient(final BigdataSailRemoteRepository repo,
         final BlueprintsValueFactory factory) {

      super(factory, props);

      if (repo == null)
         throw new IllegalArgumentException();

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
    * 
    * TODO Review this now that we support read/write tx for
    * BigdataSailRemoteRepositoryConnection (if namespace uses
    * ISOLATABLE_INDICES).
    */
	@Override
	public BigdataSailRemoteRepositoryConnection cxn() throws Exception {
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

	@Override
	protected UUID setupQuery(BigdataSailRepositoryConnection cxn,
			ASTContainer astContainer, QueryType queryType, String extQueryId) {
		//This is a NOOP when using the REST client as the query management is implemented
		//in the rest client.
		return null;
	}

	@Override
	protected void tearDownQuery(UUID queryId) {
		//This is a NOOP when using the REST client as the query management is implemented
		//in the rest client.
		
	}

	@Override
	public Collection<RunningQuery> getRunningQueries() {
		try {
			return this.repo.showQueries();
		} catch (Exception e) {
			if(log.isDebugEnabled()){
				log.debug(e);
			}
		}
		
		throw new RuntimeException("Error while showing queries.");
	}

	@Override
	public void cancel(UUID queryId) {

		assert(queryId != null);

		try {
			this.repo.cancel(queryId);
		} catch (Exception e) {
			if(log.isDebugEnabled()) {
				log.debug(e);
			}
		}
	}

	@Override
	public void cancel(String queryId) {
		assert(queryId != null);
		cancel(UUID.fromString(queryId));
	}

	@Override
	public void cancel(RunningQuery r) {
		assert(r != null);
		cancel(r.getQueryUuid());
	}

	@Override
	public RunningQuery getQueryById(UUID queryId2) {
		//TODO:  Implement for REST API
		return null;
	}

	@Override
	public RunningQuery getQueryByExternalId(String extQueryId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean isQueryCancelled(UUID queryId) {
		// TODO Auto-generated method stub
		return false;
	}
	
   @Override
    public boolean isReadOnly() {
        return false;
    }


}
