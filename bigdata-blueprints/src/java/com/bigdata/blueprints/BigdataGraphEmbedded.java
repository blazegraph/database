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

import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * This is the most basic possible implementation of the Blueprints Graph API.
 * It wraps an embedded {@link BigdataSailRepository} and holds open an
 * unisolated connection to the database for the lifespan of the Graph (until
 * {@link #shutdown()} is called.
 * 
 * @author mikepersonick
 *
 */
public class BigdataGraphEmbedded extends BigdataGraph implements TransactionalGraph {

	final BigdataSailRepository repo;
	
	transient BigdataSailRepositoryConnection cxn;
	
	/**
	 * Create a Blueprints wrapper around a {@link BigdataSail} instance.
	 */
    public BigdataGraphEmbedded(final BigdataSail sail) {
        this(sail, BigdataRDFFactory.INSTANCE);
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSail} instance with
     * a non-standard {@link BlueprintsRDFFactory} implementation.
     */
    public BigdataGraphEmbedded(final BigdataSail sail, 
            final BlueprintsRDFFactory factory) {
        this(new BigdataSailRepository(sail), factory);
    }
    
    /**
     * Create a Blueprints wrapper around a {@link BigdataSailRepository} 
     * instance.
     */
	public BigdataGraphEmbedded(final BigdataSailRepository repo) {
		this(repo, BigdataRDFFactory.INSTANCE);
	}
	
    /**
     * Create a Blueprints wrapper around a {@link BigdataSailRepository} 
     * instance with a non-standard {@link BlueprintsRDFFactory} implementation.
     */
	public BigdataGraphEmbedded(final BigdataSailRepository repo, 
			final BlueprintsRDFFactory factory) {
	    super(factory);
	    
	    this.repo = repo;
	}
	
	protected RepositoryConnection cxn() throws Exception {
	    if (cxn == null) {
	        cxn = repo.getUnisolatedConnection();
	        cxn.setAutoCommit(false);
	    }
	    return cxn;
	}
	
	@Override
	public void commit() {
		try {
		    if (cxn != null)
		        cxn.commit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rollback() {
		try {
		    if (cxn != null) {
    			cxn.rollback();
    			cxn.close();
    			cxn = null;
		    }
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

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

	@Override
	@Deprecated
	public void stopTransaction(Conclusion arg0) {
	}
	
	
    static {

//        FEATURES.supportsSerializableObjectProperty = false;
//        FEATURES.supportsBooleanProperty = true;
//        FEATURES.supportsDoubleProperty = true;
//        FEATURES.supportsFloatProperty = true;
//        FEATURES.supportsIntegerProperty = true;
//        FEATURES.supportsPrimitiveArrayProperty = false;
//        FEATURES.supportsUniformListProperty = false;
//        FEATURES.supportsMixedListProperty = false;
//        FEATURES.supportsLongProperty = true;
//        FEATURES.supportsMapProperty = false;
//        FEATURES.supportsStringProperty = true;
//
//        FEATURES.supportsDuplicateEdges = true;
//        FEATURES.supportsSelfLoops = true;
//        FEATURES.isPersistent = true;
//        FEATURES.isWrapper = false;
//        FEATURES.supportsVertexIteration = true;
//        FEATURES.supportsEdgeIteration = true;
//        FEATURES.supportsVertexIndex = false;
//        FEATURES.supportsEdgeIndex = false;
//        FEATURES.ignoresSuppliedIds = true;
        BigdataGraph.FEATURES.supportsTransactions = true;
//        FEATURES.supportsIndices = true;
//        FEATURES.supportsKeyIndices = true;
//        FEATURES.supportsVertexKeyIndex = true;
//        FEATURES.supportsEdgeKeyIndex = true;
//        FEATURES.supportsEdgeRetrieval = true;
//        FEATURES.supportsVertexProperties = true;
//        FEATURES.supportsEdgeProperties = true;
//        FEATURES.supportsThreadedTransactions = false;
    }


}
