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

import java.util.List;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

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
	
//	transient BigdataSailRepositoryConnection cxn;
	
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
	
    protected final ThreadLocal<RepositoryConnection> cxn = new ThreadLocal<RepositoryConnection>() {
        protected RepositoryConnection initialValue() {
            RepositoryConnection cxn = null;
            try {
                cxn = repo.getUnisolatedConnection();
                cxn.setAutoCommit(false);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return cxn;
        }
    };

	protected RepositoryConnection cxn() throws Exception {
//	    if (cxn == null) {
//	        cxn = repo.getUnisolatedConnection();
//	        cxn.setAutoCommit(false);
//	    }
	    return cxn.get();
	}
	
	@Override
	public void commit() {
		try {
//		    if (cxn != null)
//		        cxn.commit();
            final RepositoryConnection cxn = this.cxn.get();
            cxn.commit();
            cxn.close();
            this.cxn.remove();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rollback() {
		try {
//		    if (cxn != null) {
//    			cxn.rollback();
//    			cxn.close();
//    			cxn = null;
//		    }
		    final RepositoryConnection cxn = this.cxn.get();
		    cxn.rollback();
		    cxn.close();
		    this.cxn.remove();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void shutdown() {
		try {
//		    if (cxn != null) {
//		        cxn.close();
//		    }
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

        FEATURES.supportsTransactions = true;
        
    }


//    @Override
//    public synchronized Object getProperty(URI uri, String prop) {
//        return super.getProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized Object getProperty(URI uri, URI prop) {
//        return super.getProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized List<Object> getProperties(URI uri, String prop) {
//        return super.getProperties(uri, prop);
//    }
//
//    @Override
//    public synchronized List<Object> getProperties(URI uri, URI prop) {
//        return super.getProperties(uri, prop);
//    }
//
//    @Override
//    public synchronized Set<String> getPropertyKeys(URI uri) {
//        return super.getPropertyKeys(uri);
//    }
//
//    @Override
//    public synchronized Object removeProperty(URI uri, String prop) {
//        return super.removeProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized Object removeProperty(URI uri, URI prop) {
//        return super.removeProperty(uri, prop);
//    }
//
//    @Override
//    public synchronized void setProperty(URI uri, String prop, Object val) {
//        super.setProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void setProperty(URI uri, URI prop, Literal val) {
//        super.setProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void addProperty(URI uri, String prop, Object val) {
//        super.addProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void addProperty(URI uri, URI prop, Literal val) {
//        super.addProperty(uri, prop, val);
//    }
//
//    @Override
//    public synchronized void loadGraphML(String file) throws Exception {
//        super.loadGraphML(file);
//    }
//
//    @Override
//    public synchronized Edge addEdge(Object key, Vertex from, Vertex to, String label) {
//        return super.addEdge(key, from, to, label);
//    }
//
//    @Override
//    public synchronized Vertex addVertex(Object key) {
//        return super.addVertex(key);
//    }
//
//    @Override
//    public synchronized Edge getEdge(Object key) {
//        return super.getEdge(key);
//    }
//
//    @Override
//    public synchronized Iterable<Edge> getEdges() {
//        return super.getEdges();
//    }
//
//    @Override
//    synchronized Iterable<Edge> getEdges(URI from, URI to, String... labels) {
//        return super.getEdges(from, to, labels);
//    }
//
//    @Override
//    protected synchronized GraphQueryResult getElements(URI from, URI to, String... labels) {
//        return super.getElements(from, to, labels);
//    }
//
//    @Override
//    synchronized Iterable<Edge> getEdges(String queryStr) {
//        return super.getEdges(queryStr);
//    }
//
//    @Override
//    synchronized Iterable<Vertex> getVertices(URI from, URI to, String... labels) {
//        return super.getVertices(from, to, labels);
//    }
//
//    @Override
//    synchronized Iterable<Vertex> getVertices(String queryStr, boolean subject) {
//        return super.getVertices(queryStr, subject);
//    }
//
//    @Override
//    public synchronized Iterable<Edge> getEdges(String prop, Object val) {
//        return super.getEdges(prop, val);
//    }
//
//    @Override
//    public synchronized Vertex getVertex(Object key) {
//        return super.getVertex(key);
//    }
//
//    @Override
//    public synchronized Iterable<Vertex> getVertices() {
//        return super.getVertices();
//    }
//
//    @Override
//    public synchronized Iterable<Vertex> getVertices(String prop, Object val) {
//        return super.getVertices(prop, val);
//    }
//
//    @Override
//    public synchronized GraphQuery query() {
//        return super.query();
//    }
//
//    @Override
//    public synchronized void removeEdge(Edge edge) {
//        super.removeEdge(edge);
//    }
//
//    @Override
//    public synchronized void removeVertex(Vertex vertex) {
//        super.removeVertex(vertex);
//    }
//
//    @Override
//    public Features getFeatures() {
//        return super.getFeatures();
//    }
    
}
