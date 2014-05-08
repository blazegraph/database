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

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.store.BD;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * A base class for a Blueprints wrapper around a bigdata back-end.
 * 
 * @author mikepersonick
 *
 */
public abstract class BigdataGraph implements Graph {

	public static final URI VERTEX = new URIImpl(BD.NAMESPACE + "Vertex");
	
	public static final URI EDGE = new URIImpl(BD.NAMESPACE + "Edge");
	
//	final BigdataSailRepository repo;
//	
//	transient BigdataSailRepositoryConnection cxn;
	
	final BlueprintsRDFFactory factory;
	
//	public BigdataGraph(final BigdataSailRepository repo) {
//		this(repo, BigdataRDFFactory.INSTANCE);
//	}
	
	public BigdataGraph(//final BigdataSailRepository repo, 
			final BlueprintsRDFFactory factory) {
//		try {
//		    this.repo = repo;
//			this.cxn = repo.getUnisolatedConnection();
//			this.cxn.setAutoCommit(false);
			this.factory = factory;
//		} catch (RepositoryException ex) {
//			throw new RuntimeException(ex);
//		}
	}
	
	public String toString() {
	    return getClass().getSimpleName().toLowerCase();
	}
	
    /**
     * Post a GraphML file to the remote server. (Bulk-upload operation.)
     */
    public void loadGraphML(final String file) throws Exception {
        GraphMLReader.inputGraph(this, file);
    }
    
	protected abstract RepositoryConnection cxn() throws Exception;
	
//	public BigdataSailRepositoryConnection getConnection() {
//		return this.cxn;
//	}
//	
//	public BlueprintsRDFFactory getFactory() {
//		return this.factory;
//	}
	
//	public Value getValue(final URI s, final URI p) {
//		
//		try {
//		
//			final RepositoryResult<Statement> result = 
//					cxn.getStatements(s, p, null, false);
//			
//			if (result.hasNext()) {
//				
//				final Value o = result.next().getObject();
//				
//				if (result.hasNext()) {
//					throw new RuntimeException(s
//							+ ": more than one value for p: " + p
//							+ ", did you mean to call getValues()?");
//				}
//				
//				return o;
//				
//			}
//			
//			return null;
//			
//		} catch (Exception ex) {
//			throw new RuntimeException(ex);
//		}
//		
//	}
	
	public Object getProperty(final URI s, final URI p) {

		try {
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(s, p, null, false);
			
			if (result.hasNext()) {
				
				final Value value = result.next().getObject();
				
				if (result.hasNext()) {
					throw new RuntimeException(s
							+ ": more than one value for p: " + p
							+ ", did you mean to call getValues()?");
				}

				if (!(value instanceof Literal)) {
					throw new RuntimeException("not a property: " + value);
				}
			
				final Literal lit = (Literal) value;
				
				return factory.fromLiteral(lit);
			
			}
			
			return null;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}
	
//	public List<Value> getValues(final URI s, final URI p) {
//		
//		try {
//		
//			final RepositoryResult<Statement> result = 
//					cxn().getStatements(s, p, null, false);
//			
//			final List<Value> values = new LinkedList<Value>();
//			
//			while (result.hasNext()) {
//				
//				final Value o = result.next().getObject();
//				
//				values.add(o);
//				
//			}
//			
//			return values;
//			
//		} catch (Exception ex) {
//			throw new RuntimeException(ex);
//		}
//		
//	}
	
	public List<Object> getProperties(final URI s, final URI p) {

		try {
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(s, p, null, false);
			
			final List<Object> props = new LinkedList<Object>();
			
			while (result.hasNext()) {
				
				final Value value = result.next().getObject();
				
				if (!(value instanceof Literal)) {
					throw new RuntimeException("not a property: " + value);
				}
				
				final Literal lit = (Literal) value;
				
				props.add(factory.fromLiteral(lit));
				
			}
			
			return props;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}
	
	public Set<String> getPropertyKeys(final URI s) {
		
		try {
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(s, null, null, false);
			
			final Set<String> properties = new LinkedHashSet<String>();
			
			while (result.hasNext()) {
				
				final Statement stmt = result.next();
				
				if (!(stmt.getObject() instanceof Literal)) {
					continue;
				}
				
				if (stmt.getPredicate().equals(RDFS.LABEL)) {
					continue;
				}
				
				final String p = 
						factory.fromPropertyURI(stmt.getPredicate());
				
				properties.add(p);
				
			}
			
			return properties;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	public Object removeProperty(final URI s, final URI p) {

		try {
			
			final Object oldVal = getProperty(s, p);
			
			cxn().remove(s, p, null);
			
			return oldVal;
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	
	}

	public void setProperty(final URI s, final URI p, final Literal o) {
		
		try {
			
			cxn().remove(s, p, null);
			
			cxn().add(s, p, o);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	@Override
	public Edge addEdge(final Object key, final Vertex from, final Vertex to, 
			final String label) {
		
		if (label == null) {
			throw new IllegalArgumentException();
		}
		
		final String eid = key != null ? key.toString() : UUID.randomUUID().toString();
		
		final URI edgeURI = factory.toEdgeURI(eid);

		if (key != null) {
			
			final Edge edge = getEdge(key);
			
			if (edge != null) {
				if (!(edge.getVertex(Direction.OUT).equals(from) &&
						(edge.getVertex(Direction.OUT).equals(to)))) {
					throw new IllegalArgumentException("edge already exists: " + key);
				}
			}
			
		}
			
		try {
				
//			if (cxn().hasStatement(edgeURI, RDF.TYPE, EDGE, false)) {
//				throw new IllegalArgumentException("edge " + eid + " already exists");
//			}

			final URI fromURI = factory.toVertexURI(from.getId().toString());
			final URI toURI = factory.toVertexURI(to.getId().toString());
			
			cxn().add(fromURI, edgeURI, toURI);
			cxn().add(edgeURI, RDF.TYPE, EDGE);
			cxn().add(edgeURI, RDFS.LABEL, factory.toLiteral(label));
			
			return new BigdataEdge(new StatementImpl(fromURI, edgeURI, toURI), this);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public Vertex addVertex(final Object key) {
		
		try {
			
			final String vid = key != null ? 
					key.toString() : UUID.randomUUID().toString();
					
			final URI uri = factory.toVertexURI(vid);

//			if (cxn().hasStatement(vertexURI, RDF.TYPE, VERTEX, false)) {
//				throw new IllegalArgumentException("vertex " + vid + " already exists");
//			}
			
			cxn().add(uri, RDF.TYPE, VERTEX);

			return new BigdataVertex(uri, this);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public Edge getEdge(final Object key) {
		
		if (key == null)
			throw new IllegalArgumentException();
		
		try {
			
			final URI edge = factory.toEdgeURI(key.toString());
			
			final RepositoryResult<Statement> result = 
					cxn().getStatements(null, edge, null, false);
			
			if (result.hasNext()) {
				
				final Statement stmt = result.next();
				
				if (result.hasNext()) {
					throw new RuntimeException(
							"duplicate edge: " + key);
				}
				
				return new BigdataEdge(stmt, this);
				
			}
			
			return null;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public Iterable<Edge> getEdges() {
		
		final URI wild = null;
		return getEdges(wild, wild);
		
	}
	
	public Iterable<Edge> getEdges(final URI s, final URI o, final String... labels) {
		
		try {
			
//			final RepositoryResult<Statement> result = 
//					cxn().getStatements(s, p, o, false);
//
//			return new EdgeIterable(result);
			
			final StringBuilder sb = new StringBuilder();
			sb.append("construct { ?from ?edge ?to . } where {\n");
			sb.append("?edge rdf:type bd:Edge . ?from ?edge ?to .\n");
			if (labels != null && labels.length > 0) {
				if (labels.length == 1) {
					sb.append("?edge rdfs:label \"").append(labels[0]).append("\" .\n");
				} else {
					sb.append("?edge rdfs:label ?label .\n");
					sb.append("filter(?label in (");
					for (String label : labels) {
						sb.append("\""+label+"\", ");
					}
					sb.setLength(sb.length()-2);
					sb.append(")) .\n");
				}
			}
			sb.append("}");
			
			final String queryStr = sb.toString()
						.replace("?from", s != null ? "<"+s+">" : "?from")
							.replace("?to", o != null ? "<"+o+">" : "?to");
			
			final org.openrdf.query.GraphQuery query = 
					cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new EdgeIterable(stmts);

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	public Iterable<Vertex> getVertices(final URI s, final URI o, 
			final String... labels) {
		
		if (s != null && o != null) {
			throw new IllegalArgumentException();
		}
		
		if (s == null && o == null) {
			throw new IllegalArgumentException();
		}
		
		try {
			
//			final RepositoryResult<Statement> result = 
//					cxn().getStatements(s, null, o, false);
//			
//			return new VertexIterable(result, s == null);

			final StringBuilder sb = new StringBuilder();
			sb.append("construct { ?from ?edge ?to . } where {\n");
			sb.append("?edge rdf:type bd:Edge . ?from ?edge ?to .\n");
			if (labels != null && labels.length > 0) {
				if (labels.length == 1) {
					sb.append("?edge rdfs:label \"").append(labels[0]).append("\" .\n");
				} else {
					sb.append("?edge rdfs:label ?label .\n");
					sb.append("filter(?label in (");
					for (String label : labels) {
						sb.append("\""+label+"\", ");
					}
					sb.setLength(sb.length()-2);
					sb.append(")) .\n");
				}
			}
			sb.append("}");
			
			final String queryStr = sb.toString()
						.replace("?from", s != null ? "<"+s+">" : "?from")
							.replace("?to", o != null ? "<"+o+">" : "?to");
			
			final org.openrdf.query.GraphQuery query = 
					cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new VertexIterable(stmts, s == null);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}
	
	public final <T> Iterable<T> fuse(final Iterable<T>... args) {
		
		return new FusedIterable<T>(args);
	}
	

	@Override
	public Iterable<Edge> getEdges(final String prop, final Object val) {
		
		final URI p = factory.toPropertyURI(prop);
		final Literal o = factory.toLiteral(val);
		
		try {
		
			final String queryStr = IOUtils.toString(
					getClass().getResourceAsStream("edgesByProperty.rq"))
						.replace("?prop", "<"+p+">")
							.replace("?val", o.toString());
			
			final org.openrdf.query.GraphQuery query = 
					cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
			
			final GraphQueryResult stmts = query.evaluate();
			
			return new EdgeIterable(stmts);
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public Features getFeatures() {
		
		return FEATURES;
		
	}

	@Override
	public Vertex getVertex(final Object key) {
		
		if (key == null)
			throw new IllegalArgumentException();
		
		final URI uri = factory.toVertexURI(key.toString());
		try {
			if (cxn().hasStatement(uri, RDF.TYPE, VERTEX, false)) {
				return new BigdataVertex(uri, this);
			}
			return null;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public Iterable<Vertex> getVertices() {
		
		try {
			final RepositoryResult<Statement> result = 
					cxn().getStatements(null, RDF.TYPE, VERTEX, false);
			return new VertexIterable(result, true);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public Iterable<Vertex> getVertices(String prop, Object val) {
		
		final URI p = factory.toPropertyURI(prop);
		final Literal o = factory.toLiteral(val);
		try {
			final RepositoryResult<Statement> result = 
					cxn().getStatements(null, p, o, false);
			return new VertexIterable(result, true);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public GraphQuery query() {
		return new DefaultGraphQuery(this);
	}

	@Override
	public void removeEdge(final Edge edge) {
		try {
			final URI uri = factory.toURI(edge);
            if (!cxn().hasStatement(uri, RDF.TYPE, EDGE, false)) {
                throw new IllegalStateException();
            }
            final URI wild = null;
			// remove the edge statement
			cxn().remove(wild, uri, wild);
			// remove its properties
			cxn().remove(uri, wild, wild);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void removeVertex(final Vertex vertex) {
		try {
			final URI uri = factory.toURI(vertex);
            if (!cxn().hasStatement(uri, RDF.TYPE, VERTEX, false)) {
                throw new IllegalStateException();
            }
            final URI wild = null;
			// remove outgoing links and properties
			cxn().remove(uri, wild, wild);
			// remove incoming links
			cxn().remove(wild, wild, uri);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

//	@Override
//	public void commit() {
//		try {
//			cxn().commit();
//		} catch (RepositoryException e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	@Override
//	public void rollback() {
//		try {
//			cxn().rollback();
//			cxn.close();
//            cxn = repo.getUnisolatedConnection();
//		    cxn.setAutoCommit(false);
//		} catch (RepositoryException e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	@Override
//	public void shutdown() {
//		try {
//			cxn.close();
//			repo.shutDown();
//		} catch (RepositoryException e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	@Override
//	@Deprecated
//	public void stopTransaction(Conclusion arg0) {
//	}
	
	public class VertexIterable implements Iterable<Vertex>, Iterator<Vertex> {

		private final CloseableIteration<Statement, ? extends OpenRDFException> stmts;
		
		private final boolean subject;
		
		private final List<Vertex> cache;
		
		public VertexIterable(
				final CloseableIteration<Statement, ? extends OpenRDFException> stmts,
				final boolean subject) {
			this.stmts = stmts;
			this.subject = subject;
			this.cache = new LinkedList<Vertex>();
		}
		
		@Override
		public boolean hasNext() {
			try {
				return stmts.hasNext();
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Vertex next() {
			try {
				final Statement stmt = stmts.next();
				final URI v = (URI) 
						(subject ? stmt.getSubject() : stmt.getObject());
				if (!hasNext()) {
					stmts.close();
				}
				final Vertex vertex = new BigdataVertex(v, BigdataGraph.this);
				cache.add(vertex);
				return vertex;
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<Vertex> iterator() {
			return hasNext() ? this : cache.iterator();
		}
		
	}

	public class EdgeIterable implements Iterable<Edge>, Iterator<Edge> {

		private final CloseableIteration<Statement, ? extends OpenRDFException> stmts;
		
		private final List<Edge> cache;
		
		public EdgeIterable(
				final CloseableIteration<Statement, ? extends OpenRDFException> stmts) {
			this.stmts = stmts;
			this.cache = new LinkedList<Edge>();
		}
		
		@Override
		public boolean hasNext() {
			try {
				return stmts.hasNext();
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Edge next() {
			try {
				final Statement stmt = stmts.next();
				if (!hasNext()) {
					stmts.close();
				}
				final Edge edge = new BigdataEdge(stmt, BigdataGraph.this);
				cache.add(edge);
				return edge;
			} catch (OpenRDFException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<Edge> iterator() {
			return hasNext() ? this : cache.iterator();
		}
		
	}

	public class FusedIterable<T> implements Iterable<T>, Iterator<T> {
		
		private final Iterable<T>[] args;
		
		private transient int i = 0;
		
		private transient Iterator<T> curr;
		
		public FusedIterable(final Iterable<T>... args) {
			this.args = args;
			this.curr = args[0].iterator();
		}
		
		@Override
		public boolean hasNext() {
			if (curr.hasNext()) {
				return true;
			}
			while (!curr.hasNext() && i < (args.length-1)) {
				curr = args[++i].iterator();
				if (curr.hasNext()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public T next() {
			return curr.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public Iterator<T> iterator() {
			return this;
		}

	}
	
    protected static final Features FEATURES = new Features();

    static {

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }

}
