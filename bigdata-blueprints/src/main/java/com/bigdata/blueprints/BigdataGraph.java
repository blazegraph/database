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

import info.aduna.iteration.CloseableIteration;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.blueprints.BigdataGraphAtom.EdgeAtom;
import com.bigdata.blueprints.BigdataGraphAtom.EdgeLabelAtom;
import com.bigdata.blueprints.BigdataGraphAtom.ElementType;
import com.bigdata.blueprints.BigdataGraphAtom.ExistenceAtom;
import com.bigdata.blueprints.BigdataGraphAtom.PropertyAtom;
import com.bigdata.blueprints.BigdataGraphEdit.Action;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.sail.BigdataSailBooleanQuery;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.QueryCancelledException;
import com.bigdata.rdf.sail.RDRHistory;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * A base class for a Blueprints wrapper around a bigdata back-end.
 * 
 * @author mikepersonick
 *
 */
public abstract class BigdataGraph implements Graph {

    private static final transient Logger log = Logger.getLogger(BigdataGraph.class);
    
    private static final transient Logger sparqlLog = Logger.getLogger(
            BigdataGraph.class.getName() + ".SparqlLogger");

    /**
     * Maximum number of chars to print through the SparqlLogger.
     */
    public static final int SPARQL_LOG_MAX = 10000;
    
    public interface Options {
        
        /**
         * Allow multiple edges with the same edge id.  Useful for assigning
         * by-reference properties (e.g. vertex type).
         */
        String LAX_EDGES = BigdataGraph.class.getName() + ".laxEdges";
        
        /**
         * Use an append model for properties (rather than replace).
         */
        String LAX_PROPERTIES = BigdataGraph.class.getName() + ".laxProperties";
        
        /**
         * Set a global query timeout to apply to issuing queries.
         */
        String MAX_QUERY_TIME = BigdataGraph.class.getName() + ".maxQueryTime";
        
    }
    
    /**
     * Max Query Time used to globally set the query timeout.
     * 
     * Default is 0 (unlimited)
     */
    protected final int maxQueryTime;
    
    /**
     * URI used for typing elements.
     */
    protected final URI TYPE;
    
    /**
     * URI used to represent a Vertex.
     */
    protected final URI VERTEX;
    
    /**
     * URI used to represent a Edge.
     */
    protected final URI EDGE;

    /**
     * URI used for labeling edges.
     */
    protected final URI LABEL;

    /**
     * Factory for round-tripping between Blueprints data and RDF data.
     */
    protected final BlueprintsValueFactory factory;
    
    /**
     * Allow re-use of edge identifiers.
     */
    private final boolean laxEdges;
    
    /**
     * If true, use pure append mode (don't check old property values).
     */
    protected final boolean laxProperties;
    
    public BigdataGraph(final BlueprintsValueFactory factory) {
        this(factory, new Properties());
    }
    
    public BigdataGraph(final BlueprintsValueFactory factory,
            final Properties props) {

        this.factory = factory;
        
        this.laxEdges = Boolean.valueOf(props.getProperty(
                Options.LAX_EDGES, "false"));
        this.laxProperties = Boolean.valueOf(props.getProperty(
                Options.LAX_PROPERTIES, "false"));
        this.maxQueryTime = Integer.parseInt(props.getProperty(
                Options.MAX_QUERY_TIME, "0"));
        
        this.TYPE = factory.getTypeURI();
        this.VERTEX = factory.getVertexURI();
        this.EDGE = factory.getEdgeURI();
        this.LABEL = factory.getLabelURI();
        
    }
    
    /**
     * For some reason this is part of the specification (i.e. part of the
     * Blueprints test suite).
     */
    public String toString() {
        
        return getClass().getSimpleName().toLowerCase();
        
    }
    
    /**
     * Return the factory used to round-trip between Blueprints values and
     * RDF values.
     */
    public BlueprintsValueFactory getValueFactory() {
        return factory;
    }
    
    /**
     * Different implementations will return different types of connections
     * depending on the mode (client/server, embedded, read-only, etc.)
     */
    public abstract RepositoryConnection cxn() throws Exception;
    
    /**
     * Return a single-valued property for an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public Object getProperty(final URI uri, final String prop) {
        
        return getProperty(uri, factory.toPropertyURI(prop));
        
    }

    /**
     * Return a single-valued property for an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public Object getProperty(final URI uri, final URI prop) {

        try {
            
            final RepositoryResult<Statement> result = 
                    cxn().getStatements(uri, prop, null, false);
            
            try {
                
                if (result.hasNext()) {
                    
                    final Statement stmt = result.next();
                    
                    if (!result.hasNext()) {
    
                        /*
                         * Single value.
                         */
                        return getProperty(stmt.getObject());
                        
                    } else {
    
                        /*
                         * Multi-value, use a list.
                         */
                        final List<Object> list = new LinkedList<Object>();
    
                        list.add(getProperty(stmt.getObject()));
                        
                        while (result.hasNext()) {
                            
                            list.add(getProperty(result.next().getObject()));
                            
                        }
                        
                        return list;
                        
                    }
    
                }
                
                return null;
                
            } finally {
                
                result.close();
                
            }
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
    protected Object getProperty(final Value value) {
        
        if (!(value instanceof Literal)) {
            throw new RuntimeException("not a property: " + value);
        }
    
        final Literal lit = (Literal) value;
        
        final Object o = factory.fromLiteral(lit);
        
        return o;

    }
    
//    /**
//     * Return a multi-valued property for an edge or vertex.
//     * 
//     * TODO get rid of me
//     * 
//     * @see {@link BigdataElement}
//     */
//    public List<Object> getProperties(final URI uri, final String prop) {
//        
//        return getProperties(uri, factory.toPropertyURI(prop));
//        
//    }
//
//    /**
//     * Return a multi-valued property for an edge or vertex.
//     * 
//     * TODO get rid of me
//     * 
//     * @see {@link BigdataElement}
//     */
//    public List<Object> getProperties(final URI uri, final URI prop) {
//
//        try {
//            
//            final RepositoryResult<Statement> result = 
//                    getWriteConnection().getStatements(uri, prop, null, false);
//            
//            final List<Object> props = new LinkedList<Object>();
//            
//            while (result.hasNext()) {
//                
//                final Value value = result.next().getObject();
//                
//                if (!(value instanceof Literal)) {
//                    throw new RuntimeException("not a property: " + value);
//                }
//                
//                final Literal lit = (Literal) value;
//                
//                props.add(factory.fromLiteral(lit));
//                
//            }
//            
//            return props;
//            
//        } catch (RuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        
//    }
    
    /**
     * Return the property names for an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public Set<String> getPropertyKeys(final URI uri) {
        
        try {
            
            final RepositoryResult<Statement> result = 
                    cxn().getStatements(uri, null, null, false);

            try {
                
                final Set<String> properties = new LinkedHashSet<String>();
                
                while (result.hasNext()) {
                    
                    final Statement stmt = result.next();
                    
                    if (!(stmt.getObject() instanceof Literal)) {
                        continue;
                    }
                    
                    if (stmt.getPredicate().equals(LABEL)) {
                        continue;
                    }
                    
                    final String p = 
                            factory.fromURI(stmt.getPredicate());
                    
                    properties.add(p);
                    
                }
                
                return properties;
                
            } finally {
                
                result.close();
                
            }
                
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Remove all values for a particular property on an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public Object removeProperty(final URI uri, final String prop) {
        
        return removeProperty(uri, factory.toPropertyURI(prop));
        
    }
    
    /**
     * Remove all values for a particular property on an edge or vertex.
     * 
     * @see {@link BigdataElement}
     */
    public Object removeProperty(final URI uri, final URI prop) {

        try {
            
            final Object oldVal = getProperty(uri, prop);
            
            cxn().remove(uri, prop, null);
            
            return oldVal;
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    
    }

    /**
     * Set a single-value property on an edge or vertex (remove the old
     * value first).
     * 
     * @see {@link BigdataElement}
     */
    public void setProperty(final URI s, final String prop, final Object val) {

        setProperty(s, factory.toPropertyURI(prop), toLiterals(val));
        
    }
    
    protected Collection<Literal> toLiterals(final Object val) {
        
        final Collection<Literal> literals = new LinkedList<Literal>();
        
        if (val instanceof Collection) {
            
            @SuppressWarnings("unchecked")
            final Collection<Object> vals = (Collection<Object>) val;
                    
            for (Object o : vals) {
                
                literals.add(factory.toLiteral(o));
                
            }
            
        } else if (val.getClass().isArray()) {

            final int len = Array.getLength(val);

            for (int i = 0; i < len; i++) {
                
                final Object o = Array.get(val, i);
                
                literals.add(factory.toLiteral(o));
                
            }
            
        } else {
        
            literals.add(factory.toLiteral(val));
            
        }
        
        return literals;
        
    }
    
//    /**
//     * Set a single-value property on an edge or vertex (remove the old
//     * value first).
//     * 
//     * @see {@link BigdataElement}
//     */
//    public void setProperty(final URI uri, final URI prop, final Literal val) {
//        
//        try {
//
//            final RepositoryConnection cxn = getWriteConnection();
//            
//            if (!laxProperties) {
//                
//                // remove the old value
//                cxn.remove(uri, prop, null);
//                
//            }
//            
//            // add the new value
//            cxn.add(uri, prop, val);
//            
//        } catch (RuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        
//    }
    
    /**
     * Set a multi-value property on an edge or vertex (remove the old
     * values first).
     * 
     * @see {@link BigdataElement}
     */
    public void setProperty(final URI uri, final URI prop, 
            final Collection<Literal> vals) {
        
        try {

            final RepositoryConnection cxn = cxn();
            
            if (!laxProperties) {
                
                // remove the old value
                cxn.remove(uri, prop, null);
                
            }
            
            // add the new values
            for (Literal val : vals) {
                cxn.add(uri, prop, val);
            }
            
    /*
     * Add a bnode representing the array object ["a", "b", "c", "a"].
     * 
     * <s> <p> "a" .
     * <s> <p> "b" .
     * <s> <p> "c" .
     * << <s> <p> "a" >> <order> "1"^^xsd:int .
     * << <s> <p> "b" >> <order> "2"^^xsd:int .
     * << <s> <p> "c" >> <order> "3"^^xsd:int .
     * << <s> <p> "a" >> <order> "4"^^xsd:int .
     */
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
//    /**
//     * Add a property on an edge or vertex (multi-value property extension).
//     * 
//     * @see {@link BigdataElement}
//     */
//    public void addProperty(final URI uri, final String prop, final Object val) {
//        
//        setProperty(uri, factory.toPropertyURI(prop), factory.toLiteral(val));
//
//    }
//    
//    /**
//     * Add a property on an edge or vertex (multi-value property extension).
//     * 
//     * @see {@link BigdataElement}
//     */
//    public void addProperty(final URI uri, final URI prop, final Literal val) {
//        
//        try {
//            
//            getWriteConnection().add(uri, prop, val);
//            
//        } catch (RuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        
//    }
    
    /**
     * Post a GraphML file to the remote server. (Bulk-upload operation.)
     */
    public void loadGraphML(final String file) throws Exception {
        
        GraphMLReader.inputGraph(this, file);
        
    }
    
    /**
     * Add an edge.
     */
    @Override
    public Edge addEdge(final Object key, final Vertex from, final Vertex to, 
            final String label) {
        
        return addEdge(key, from, to, label, false);
        
    }
        
    /**
     * Add an edge.
     */
    public Edge addEdge(final Object key, final Vertex from, final Vertex to, 
            final boolean anonymous) {
        
        return addEdge(key, from, to, null, anonymous);
        
    }
        
    /**
     * Add an edge.
     */
    public Edge addEdge(final Object key, final Vertex from, final Vertex to, 
            final String label, final boolean anonymous) {
        
        if (log.isInfoEnabled())
            log.info("("+key+", "+from+", "+to+", "+label+")");
        
        /*
         * Null edge labels allowed for anonymous edges (in laxEdges mode).
         */
        if (label == null && !laxEdges) {
            throw new IllegalArgumentException();
        }
        
        if (key != null && !laxEdges) {
            
            final Edge edge = getEdge(key);
            
            if (edge != null) {
                if (!(edge.getVertex(Direction.OUT).equals(from) &&
                        (edge.getVertex(Direction.IN).equals(to)))) {
                    throw new IllegalArgumentException("edge already exists: " + key);
                }
            }
            
        }
            
        final String eid = key != null ? key.toString() : UUID.randomUUID().toString();
        
        final URI edgeURI = factory.toEdgeURI(eid);

        try {
                
            // do we need to check this?
//            if (cxn().hasStatement(edgeURI, TYPE, EDGE, false)) {
//                throw new IllegalArgumentException("edge " + eid + " already exists");
//            }

            final URI fromURI = factory.toVertexURI(from.getId().toString());
            final URI toURI = factory.toVertexURI(to.getId().toString());
            
            final RepositoryConnection cxn = cxn();
            
            cxn.add(fromURI, edgeURI, toURI);
            
			if (label != null) {
				cxn.add(edgeURI, LABEL, factory.toLiteral(label));

				//2015-07-15:  Please review this change for correctness.   It is
				//confirmed to resolve the projection test failure.
				if (!anonymous) {
					cxn.add(edgeURI, TYPE, EDGE);
				}

			}
            
            return new BigdataEdge(new StatementImpl(fromURI, edgeURI, toURI), this);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Add a vertex.
     */
    @Override
    public Vertex addVertex(final Object key) {
        
        if (log.isInfoEnabled())
            log.info("("+key+")");
        
        try {
            
            final String vid = key != null ? 
                    key.toString() : UUID.randomUUID().toString();
                    
            final URI uri = factory.toVertexURI(vid);

            // do we need to check this?
//            if (cxn().hasStatement(vertexURI, TYPE, VERTEX, false)) {
//                throw new IllegalArgumentException("vertex " + vid + " already exists");
//            }
            
            cxn().add(uri, TYPE, VERTEX);

            return new BigdataVertex(uri, this);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Lookup an edge.
     */
    @Override
    public Edge getEdge(final Object key) {
        
        if (log.isInfoEnabled())
            log.info("("+key+")");
        
        if (key == null)
            throw new IllegalArgumentException();
        
        try {
            
            final URI edge = factory.toEdgeURI(key.toString());
            
            final RepositoryResult<Statement> result = 
                    cxn().getStatements(null, edge, null, false);
            
            try {
            
                if (result.hasNext()) {
                    
                    final Statement stmt = result.next();
                    
                    if (result.hasNext()) {
                        throw new RuntimeException(
                                "duplicate edge: " + key);
                    }
                    
                    return new BigdataEdge(stmt, this);
                    
                }
                
                return null;
            
            } finally {
                
                result.close();
                
            }
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Iterate all edges.
     */
    @Override
    public Iterable<Edge> getEdges() {
        
        if (log.isInfoEnabled())
            log.info("");
        
        try {
            
            final URI wild = null;
            return getEdges(wild, wild);
            
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
    }
    
    /**
     * Find edges based on the from and to vertices and the edge labels, all
     * optional parameters (can be null). The edge labels can be null to include
     * all labels.
     * <p>
     * 
     * @param from
     *            the from vertex (null for wildcard)
     * @param to
     *            the to vertex (null for wildcard)
     * @param labels
     *            the edge labels to consider (optional)
     * @return the edges matching the supplied criteria
     */
    Iterable<Edge> getEdges(final URI from, final URI to, 
            final String... labels) throws Exception {

        final GraphQueryResult stmts = getElements(from, to, labels);
        
        /*
         * EdgeIterable will close the connection if necessary.
         */
        return new EdgeIterable(stmts);
        
    }

    /**
     * Translates the request to a high-performance SPARQL query:
     * 
     * construct {
     *   ?from ?edge ?to .
     * } where {
     *   ?edge rdf:type <Edge> .
     *   
     *   ?from ?edge ?to .
     *   
     *   # filter by edge label
     *   ?edge rdfs:label ?label .
     *   filter(?label in ("label1", "label2", ...)) .
     * }
     */
    protected GraphQueryResult getElements(final URI from, final URI to, 
            final String... labels) throws Exception {
        
        final StringBuilder sb = new StringBuilder();
        sb.append("construct { ?from ?edge ?to . } where {\n");
        sb.append("  ?edge <"+TYPE+"> <"+EDGE+"> .\n");
        sb.append("  ?from ?edge ?to .\n");
        if (labels != null && labels.length > 0) {
            if (labels.length == 1) {
                sb.append("  ?edge <"+LABEL+"> \"").append(labels[0]).append("\" .\n");
            } else {
                sb.append("  ?edge <"+LABEL+"> ?label .\n");
                sb.append("  filter(?label in (");
                for (String label : labels) {
                    sb.append("\""+label+"\", ");
                }
                sb.setLength(sb.length()-2);
                sb.append(")) .\n");
            }
        }
        sb.append("}");

        // bind the from and/or to
        final String queryStr = sb.toString()
                    .replace("?from", from != null ? "<"+from+">" : "?from")
                        .replace("?to", to != null ? "<"+to+">" : "?to");
     
        final org.openrdf.query.GraphQuery query = 
                cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
        
        final GraphQueryResult stmts = query.evaluate();

        return stmts;
            
    }
    
    /**
     * Find edges based on a SPARQL construct query.  The query MUST construct
     * edge statements: 
     * <p>
     * construct { ?from ?edge ?to } where { ... }
     * 
     * @see {@link BigdataGraphQuery}
     */
    Iterable<Edge> getEdges(final String queryStr) throws Exception { 
        
        final org.openrdf.query.GraphQuery query = 
                cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
        
        final GraphQueryResult stmts = query.evaluate();
        
        /*
         * EdgeIterable will close the connection if necessary.
         */
        return new EdgeIterable(stmts);

    }

    /**
     * Find vertices based on the supplied from and to vertices and the edge 
     * labels.  One or the other (from and to) must be null (wildcard), but not 
     * both. Use getEdges() for wildcards on both the from and to.  The edge 
     * labels can be null to include all labels.
     * 
     * @param from
     *             the from vertex (null for wildcard)
     * @param to
     *             the to vertex (null for wildcard)
     * @param labels
     *             the edge labels to consider (optional)
     * @return
     *             the vertices matching the supplied criteria
     */
    Iterable<Vertex> getVertices(final URI from, final URI to, 
            final String... labels) throws Exception {
        
        if (from != null && to != null) {
            throw new IllegalArgumentException();
        }
        
        if (from == null && to == null) {
            throw new IllegalArgumentException();
        }
        
        final GraphQueryResult stmts = getElements(from, to, labels);
        
        /*
         * VertexIterable will close the connection if necessary.
         */
        return new VertexIterable(stmts, from == null);
        
    }
    
    /**
     * Find vertices based on a SPARQL construct query. If the subject parameter
     * is true, the vertices will be taken from the subject position of the
     * constructed statements, otherwise they will be taken from the object
     * position.
     * 
     * @see {@link BigdataGraphQuery}
     */
    Iterable<Vertex> getVertices(final String queryStr, final boolean subject) 
            throws Exception {
        
        final org.openrdf.query.GraphQuery query = 
                cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
        
        final GraphQueryResult stmts = query.evaluate();
        
        /*
         * VertexIterable will close the connection if necessary.
         */
        return new VertexIterable(stmts, subject);
            
    }
    
    /**
     * Find edges with the supplied property value.
     * 
     * construct {
     *   ?from ?edge ?to .
     * }
     * where {
     *   ?edge <prop> <val> .
     *   ?from ?edge ?to .
     * }
     */
    @Override
    public Iterable<Edge> getEdges(final String prop, final Object val) {
        
        if (log.isInfoEnabled())
            log.info("("+prop+", "+val+")");
        
        final URI p = factory.toPropertyURI(prop);
        final Literal o = factory.toLiteral(val);
        
        try {
        
            final StringBuilder sb = new StringBuilder();
            sb.append("construct { ?from ?edge ?to . } where {\n");
            sb.append("  ?edge <"+p+"> "+o+" .\n");
            sb.append("  ?from ?edge ?to .\n");
            sb.append("}");

            final String queryStr = sb.toString();

            return getEdges(queryStr);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Lookup a vertex.
     */
    @Override
    public Vertex getVertex(final Object key) {
        
        if (log.isInfoEnabled())
            log.info("("+key+")");
        
        if (key == null)
            throw new IllegalArgumentException();
        
        final URI uri = factory.toVertexURI(key.toString());
        
        try {
            
            if (cxn().hasStatement(uri, TYPE, VERTEX, false)) {
                return new BigdataVertex(uri, this);
            }
            
            return null;
                
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    
    /**
     * Iterate all vertices.
     */
    @Override
    public Iterable<Vertex> getVertices() {
        
        if (log.isInfoEnabled())
            log.info("");
        
        try {
            
            final RepositoryResult<Statement> result = 
                    cxn().getStatements(null, TYPE, VERTEX, false);
            
            /*
             * VertexIterable will close the connection if necessary.
             */
            return new VertexIterable(result, true);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Find vertices with the supplied property value.
     */
    @Override
    public Iterable<Vertex> getVertices(final String prop, final Object val) {
        
        if (log.isInfoEnabled())
            log.info("("+prop+", "+val+")");
        
        final URI p = factory.toPropertyURI(prop);
        final Literal o = factory.toLiteral(val);
        
        try {
            
            final RepositoryResult<Statement> result = 
                    cxn().getStatements(null, p, o, false);
            
            /*
             * VertexIterable will close the connection if necessary.
             */
            return new VertexIterable(result, true);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Providing an override implementation for our GraphQuery to avoid the
     * low-performance scan and filter paradigm. See {@link BigdataGraphQuery}. 
     */
    @Override
    public GraphQuery query() {

        if (log.isInfoEnabled())
            log.info("");
        
//        return new DefaultGraphQuery(this);
        return new BigdataGraphQuery(this);
    }

    /**
     * Remove an edge and its properties.
     */
    @Override
    public void removeEdge(final Edge edge) {
        
        try {
            
            final RepositoryConnection cxn = cxn();
            
            final URI uri = factory.toURI(edge);
            
            if (!cxn.hasStatement(uri, TYPE, EDGE, false)) {
                throw new IllegalStateException();
            }
            
            final URI wild = null;
            
            // remove the edge statement
            cxn.remove(wild, uri, wild);
            
            // remove its properties
            cxn.remove(uri, wild, wild);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Remove a vertex and its edges and properties.
     * 
     * TODO FIXME I am not fully removing dependent edges.
     */
    @Override
    public void removeVertex(final Vertex vertex) {
        
        try {
            
            final RepositoryConnection cxn = cxn();
            
            final URI uri = factory.toURI(vertex);
            
            if (!cxn.hasStatement(uri, TYPE, VERTEX, false)) {
                throw new IllegalStateException();
            }
            
            final URI wild = null;
            
            // remove outgoing edges and properties
            cxn.remove(uri, wild, wild);
            
            // remove incoming edges
            cxn.remove(wild, wild, uri);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Translate a collection of Bigdata statements into an iteration of
     * Blueprints vertices.
     *  
     * @author mikepersonick
     *
     * TODO FIXME Find a better way to close the connection associated with
     * this iterable.
     */
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
                final Vertex vertex = new BigdataVertex(v, BigdataGraph.this);
                cache.add(vertex);
                return vertex;
            } catch (OpenRDFException e) {
                throw new RuntimeException(e);
            } finally {
                if (!hasNext()) {
                    try {
                        stmts.close();
                    } catch (OpenRDFException e) { 
                        log.warn("Could not close result");
                    }
                }
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

    /**
     * Translate a collection of Bigdata statements into an iteration of
     * Blueprints edges.
     *  
     * @author mikepersonick
     *
     * TODO FIXME Find a better way to close the connection associated with
     * this iterable.
     */
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
                final Edge edge = new BigdataEdge(stmt, BigdataGraph.this);
                cache.add(edge);
                return edge;
            } catch (OpenRDFException e) {
                throw new RuntimeException(e);
            } finally {
                if (!hasNext()) {
                    try {
                        stmts.close();
                    } catch (OpenRDFException e) { 
                        log.warn("Could not close result");
                    }
                }
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

    /**
     * Fuse two iterables together into one.  Useful for combining IN and OUT
     * edges for a vertex.
     */
    public final <T> Iterable<T> fuse(final Iterable<T>... args) {
        
        return new FusedIterable<T>(args);
    }
    
    /**
     * Fuse two iterables together into one.  Useful for combining IN and OUT
     * edges for a vertex.
     *  
     * @author mikepersonick
     */
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

    /**
     * Project a subgraph using a SPARQL query.
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public ICloseableIterator<BigdataGraphAtom> project(final String queryStr) 
            throws Exception {
    	return this.project(queryStr,UUID.randomUUID().toString());
    }
    
	/**
	 * Project a subgraph using a SPARQL query.
	 *
	 * This version allows passing an external system ID to allow association
	 * between queries in the query engine when using an Embedded Client.
	 * 
	 * <p>
	 * Warning: You MUST close this iterator when finished.
	 */
	@SuppressWarnings("unchecked")
	public ICloseableIterator<BigdataGraphAtom> project(final String queryStr,
			String externalQueryId) throws Exception {
        
        final RepositoryConnection cxn = cxn();
        
        if (sparqlLog.isTraceEnabled()) {
            sparqlLog.trace("query:\n"+ (queryStr.length() <= SPARQL_LOG_MAX 
                    ? queryStr : queryStr.substring(0, SPARQL_LOG_MAX)+" ..."));
        }
                
        final GraphQueryResult result;
        UUID queryId = null;

        try {
            
            final org.openrdf.query.GraphQuery query = 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
            
            setMaxQueryTime(query);
            
            if (query instanceof BigdataSailGraphQuery
					&& cxn instanceof BigdataSailRepositoryConnection) {

				final BigdataSailGraphQuery bdtq = (BigdataSailGraphQuery) query;
				queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
						bdtq.getASTContainer(), QueryType.CONSTRUCT,
						externalQueryId);
			}
        
            if (sparqlLog.isTraceEnabled()) {
                if (query instanceof BigdataSailGraphQuery) {
                    final BigdataSailGraphQuery bdgq = (BigdataSailGraphQuery) query;
                    sparqlLog.trace("optimized AST:\n"+bdgq.optimize());
                }
            }
        
            result = query.evaluate();

        } catch (Exception ex) {
            if (queryId != null) {
                /*
                 * In case the exception happens during evaluate().
                 */
                finalizeQuery(queryId);
            }
            throw ex;
        }
        
		final IStriterator sitr = new Striterator(new WrappedResult<Statement>(
				result, queryId));
        
        sitr.addFilter(new Filter() {
            private static final long serialVersionUID = 1L;
            @Override
            public boolean isValid(final Object e) {
                final Statement stmt = (Statement) e;
                // do not project history
                return stmt.getSubject() instanceof URI;
            }
        });
        
        sitr.addFilter(new Resolver() {
            private static final long serialVersionUID = 1L;
            @Override
            protected Object resolve(final Object e) {
                final Statement stmt = (Statement) e;
                return toGraphAtom(stmt);
            }
        });
        
        return (ICloseableIterator<BigdataGraphAtom>) sitr;
        
    }
    
    /**
     * Convert a unit of RDF data to an atomic unit of PG data.
     */
    protected BigdataGraphAtom toGraphAtom(final Statement stmt) {

        final URI s = (URI) stmt.getSubject();
        final URI p = (URI) stmt.getPredicate();
        final Value o = stmt.getObject();
        
        return toGraphAtom(s, p, o);
        
    }
    
    /**
     * Convert a unit of RDF data to an atomic unit of PG data.
     */
    protected BigdataGraphAtom toGraphAtom(final URI s, final URI p, final Value o) {
            
        final String sid = factory.fromURI(s);
        final String pid = factory.fromURI(p);
        
        final BigdataGraphAtom atom;
        if (o instanceof URI) {
            
            /*
             * Either an edge or an element type statement.
             */
            if (p.equals(factory.getTypeURI()) && 
                (o.equals(factory.getVertexURI()) || o.equals(factory.getEdgeURI()))) {
                
                /*
                 * Element type. 
                 */
                if (o.equals(factory.getVertexURI())) {
                    atom = new ExistenceAtom(sid, ElementType.VERTEX);
                } else {
                    atom = new ExistenceAtom(sid, ElementType.EDGE);
                }
                
            } else {
                
                /*
                 * Edge.
                 */
                final String oid = factory.fromURI((URI) o);
                atom = new EdgeAtom(pid, sid, oid);
                
            }
            
        } else {
            
            /*
             * A property or the edge label.
             */
            if (p.equals(factory.getLabelURI())) {
                
                /*
                 * Edge label.
                 */
                final String label = factory.fromLiteral((Literal) o).toString();
                atom = new EdgeLabelAtom(sid, label);
                
            } else {
                
                /*
                 * Property.
                 */
                final Object oval = factory.fromLiteral((Literal) o);
                atom = new PropertyAtom(sid, pid, oval);
            
            }
            
        }
        
        return atom;

    }
    
	/**
	 * Select results using a SPARQL query.
	 * <p>
	 * Warning: You MUST close this iterator when finished.
	 */
	@SuppressWarnings("unchecked")
	public ICloseableIterator<BigdataBindingSet> select(final String queryStr)
			throws Exception {
		return this.select(queryStr, UUID.randomUUID().toString());
	}

	/**
	 * Select results using a SPARQL query.
	 * <p>
	 * Warning: You MUST close this iterator when finished.
	 */
	@SuppressWarnings("unchecked")
	public ICloseableIterator<BigdataBindingSet> select(final String queryStr,
			String externalQueryId) throws Exception {

        final RepositoryConnection cxn = cxn();
        
        if (sparqlLog.isTraceEnabled()) {
            sparqlLog.trace("query:\n"+ (queryStr.length() <= SPARQL_LOG_MAX 
                    ? queryStr : queryStr.substring(0, SPARQL_LOG_MAX)+" ..."));
        }

        final TupleQueryResult result;
        UUID queryId = null;

        try {
            
            final TupleQuery query = (TupleQuery) 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);

            setMaxQueryTime(query);
            

			if (query instanceof BigdataSailTupleQuery
					&& cxn instanceof BigdataSailRepositoryConnection) {

				final BigdataSailTupleQuery bdtq = (BigdataSailTupleQuery) query;
				queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
						bdtq.getASTContainer(), QueryType.SELECT,
						externalQueryId);
			}
            
            if (sparqlLog.isTraceEnabled()) {
                if (query instanceof BigdataSailTupleQuery) {
                    final BigdataSailTupleQuery bdtq = (BigdataSailTupleQuery) query;
                    sparqlLog.trace("optimized AST:\n"+bdtq.optimize());
                }
            }
            
            result = query.evaluate();
        
        } catch (Exception ex) {
            if (queryId != null) {
                /*
                 * In case the exception happens during evaluate().
                 */
                finalizeQuery(queryId);
            }
            throw ex;
        }
        
		final IStriterator sitr = new Striterator(
				new WrappedResult<BindingSet>(result, queryId));
        
        sitr.addFilter(new Resolver() {
            private static final long serialVersionUID = 1L;
            @Override
            protected Object resolve(final Object e) {
                final BindingSet bs = (BindingSet) e;
                return convert(bs);
            }
        });
        
        return (ICloseableIterator<BigdataBindingSet>) sitr;
            
    }
    
    /**
     * Convert SPARQL/RDF results into PG form.
     */
    protected BigdataBindingSet convert(final BindingSet bs) {
        
        final BigdataBindingSet bbs = new BigdataBindingSet();
        
        for (String key : bs.getBindingNames()) {
            
            final Value val= bs.getBinding(key).getValue();
            
            final Object o;
            if (val instanceof Literal) {
                o = factory.fromLiteral((Literal) val);
            } else if (val instanceof URI) {
                o = factory.fromURI((URI) val);
            } else {
                continue;
            }
            
            bbs.put(key, o);
            
        }
        
        return bbs;
        
    }
    /**
     * Select results using a SPARQL query.
     */
    public boolean ask(final String queryStr) throws Exception {
    	return ask(queryStr, UUID.randomUUID().toString());
    }

    /**
     * Select results using a SPARQL query.
     */
	public boolean ask(final String queryStr, String externalQueryId)
			throws Exception {
        
        final RepositoryConnection cxn = cxn();
                
        UUID queryId = null;
        
        try {
            
            final BooleanQuery query = (BooleanQuery) 
                    cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);

            setMaxQueryTime(query);
            
            if (query instanceof BigdataSailBooleanQuery
					&& cxn instanceof BigdataSailRepositoryConnection) {

				final BigdataSailBooleanQuery bdtq = (BigdataSailBooleanQuery) query;
				queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
						bdtq.getASTContainer(), QueryType.ASK,
						externalQueryId);
			}
            
            final boolean result = query.evaluate();
            
//            finalizeQuery(queryId);
            
            return result;
            
        } finally {
            if (queryId != null) {
                /*
                 * In case the exception happens during evaluate().
                 */
                finalizeQuery(queryId);
            }
        }
            
    }
    /**
     * Update graph using SPARQL Update.
     */
    public void update(final String queryStr) throws Exception {
    	final String randomUUID = UUID.randomUUID().toString();
    	
    	update(queryStr, randomUUID);
    }
        
    
    /**
     * Update graph using SPARQL Update.
     */
	public void update(final String queryStr, final String extQueryId)
			throws Exception {
        
        try {
            
            final Update update = 
                    cxn().prepareUpdate(QueryLanguage.SPARQL, queryStr);
            
            update.execute();
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    
    /**
     * Sparql template for history query.
     */
    private static final String HISTORY_TEMPLATE =
            "prefix hint: <"+QueryHints.NAMESPACE+">\n" +
            "select ?s ?p ?o ?action ?time\n" +
            "where {\n"+
            "    bind(<< ?s ?p ?o >> as ?sid) . \n" +
            "    hint:Prior hint:history true . \n" +
            "    ?sid ?action ?time . \n" +
            "}";
    
    /**
     * If history is enabled, return an iterator of historical graph edits 
     * related to any of the supplied ids.  To enable history, make sure
     * the database is in statement identifiers mode and that the RDR History
     * class is enabled.
     * <p>
     * Warning: You MUST close this iterator when finished.
     * 
     * @see {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS}
     * @see {@link AbstractTripleStore.Options#RDR_HISTORY_CLASS}
     * @see {@link RDRHistory}
     */
    public ICloseableIterator<BigdataGraphEdit> history(final List<URI> ids) 
            throws Exception {
    	final String randomUUID = UUID.randomUUID().toString();
    	return history(ids, randomUUID);
    }

    @SuppressWarnings("unchecked")
	public ICloseableIterator<BigdataGraphEdit> history(final List<URI> ids,
			final String extQueryId) throws Exception {
        
        final RepositoryConnection cxn = cxn();
        
        final StringBuilder sb = new StringBuilder(HISTORY_TEMPLATE);
        
        if (ids.size() > 0) {
            final StringBuilder vc = new StringBuilder();
            vc.append("    values (?s) { \n");
            for (URI id : ids) {
                vc.append("        (<"+id+">) \n");
            }
            vc.append("    } \n");
            sb.insert(sb.length()-1, vc.toString());
        }
        
        final String queryStr = sb.toString();
                    
        if (sparqlLog.isTraceEnabled()) {
            sparqlLog.trace("query:\n"+ (queryStr.length() <= SPARQL_LOG_MAX 
                    ? queryStr : queryStr.substring(0, SPARQL_LOG_MAX)+" ..."));
        }
        
        final TupleQueryResult result;
        UUID queryId = null;
        
        try {
            
            final TupleQuery query = (TupleQuery) 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            
            if (query instanceof BigdataSailTupleQuery
    				&& cxn instanceof BigdataSailRepositoryConnection) {
    
    			final BigdataSailTupleQuery bdtq = (BigdataSailTupleQuery) query;
    			queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
    					bdtq.getASTContainer(), QueryType.SELECT,
    					extQueryId);
    		}
            
            if (sparqlLog.isTraceEnabled()) {
                if (query instanceof BigdataSailTupleQuery) {
                    final BigdataSailTupleQuery bdtq = (BigdataSailTupleQuery) query;
                    sparqlLog.trace("optimized AST:\n"+bdtq.optimize());
                }
            }
        
            result = query.evaluate();
            
        } catch (Exception ex) {
            if (queryId != null) {
                /*
                 * In case the exception happens during evaluate().
                 */
                finalizeQuery(queryId);
            }
            throw ex;
        }
            
        final IStriterator sitr = new Striterator(new WrappedResult<BindingSet>(
                result, queryId
                ));
        
        sitr.addFilter(new Resolver() {
            private static final long serialVersionUID = 1L;
            @Override
            protected Object resolve(final Object e) {
                final BindingSet bs = (BindingSet) e;
                final URI s = (URI) bs.getValue("s");
                final URI p = (URI) bs.getValue("p");
                final Value o = bs.getValue("o");
                final URI a = (URI) bs.getValue("action");
                final Literal t = (Literal) bs.getValue("time");
                
                if (!t.getDatatype().equals(XSD.DATETIME)) {
                    throw new RuntimeException("Unexpected timestamp in result: " + bs);
                }
                
                final BigdataGraphEdit.Action action;
                if (a.equals(RDRHistory.Vocab.ADDED)) {
                    action = Action.Add;
                } else if (a.equals(RDRHistory.Vocab.REMOVED)) {
                    action = Action.Remove;
                } else {
                    throw new RuntimeException("Unexpected action in result: " + bs);
                }
                
                final BigdataGraphAtom atom = toGraphAtom(s, p, o);
                
                final long timestamp = DateTimeExtension.getTimestamp(t.getLabel());
                
                return new BigdataGraphEdit(action, atom, timestamp);
            }
        });
        
        return (ICloseableIterator<BigdataGraphEdit>) sitr;
            
    }
    
    protected static final Features FEATURES = new Features();

    @Override
    public Features getFeatures() {

        return FEATURES;
        
    }
    
    static {
        
        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
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
        FEATURES.ignoresSuppliedIds = false;
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
    
//    /**
//     * You MUST close this iterator when finished with it.
//     */
//    public static interface CloseableIterator<T> extends Iterator<T> {
//        
//        /**
//         * Release any resources associated with this iterator.
//         */
//        void close();
//        
//    }
    
    public class WrappedResult<E> implements ICloseableIterator<E> {
        
        private final CloseableIteration<E,?> it;
        
        private final UUID queryId;
        
        public WrappedResult(final CloseableIteration<E,?> it) {
            this.it = it;
            this.queryId = null;
        }

        /**
         * Allows you to pass a query UUID to perform a tear down
         * when it exits.
         * 
         * @param it
         * @param queryId
         */
        public WrappedResult(final CloseableIteration<E,?> it, UUID queryId) {
            this.it = it;
            this.queryId = queryId;
        }

        @Override
        public boolean hasNext() {
            try {
                return it.hasNext();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public E next() {
            try {
                return (E) it.next();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void close() {
            try {
				finalizeQuery(queryId);
				it.close();
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}   
        }
        
//        @Override
//        protected void finalize() throws Throwable {
//            super.finalize();
//            System.err.println("closed: " + closed);
//        }
        
    }
    
    /**
     * Utility function to set the Query timeout to the global
     * setting if it is configured.
     */
    protected void setMaxQueryTime(final org.openrdf.query.Query query) {
        if (maxQueryTime > 0) {
            query.setMaxQueryTime(maxQueryTime);
        }
    }
    
	/**
	 * Return a Collection of running queries
	 * 
	 * @return
	 */
	public abstract Collection<RunningQuery> getRunningQueries();

	/**
	 * Kill a running query specified by the UUID. Do nothing if the query has
	 * completed.
	 * 
	 * @param queryId
	 */
	public abstract void cancel(UUID queryId);

	/**
	 * Kill a running query specified by the UUID String.
	 * Do nothing if the query has completed.
	 * 
	 * @param String uuid
	 */
	public abstract void cancel(String uuid);

	/**
	 * Kill a running query specified by the RunningQuery object. Do nothing if
	 * the query has completed.
	 * 
	 * @param r
	 */
	public abstract void cancel(RunningQuery r);
	
	/**
	 * Return the {@link RunningQuery} for a currently executing SPARQL QUERY or
	 * UPDATE request.
	 * 
	 * @param queryId2
	 *            The {@link UUID} for the request.
	 * 
	 * @return The {@link RunningQuery} iff it was found.
	 */
	public abstract RunningQuery getQueryById(final UUID queryId2);
    
	/**
	 * Return the {@link RunningQuery} for a currently executing SPARQL QUERY or
	 * UPDATE request.
	 * 
	 * @param queryId2
	 *            The {@link UUID} for the request.
	 * 
	 * @return The {@link RunningQuery} iff it was found.
	 */
	public abstract RunningQuery getQueryByExternalId(final String extQueryId);

	/**
	 * Embedded clients can override this to access query management
	 * capabilities.
	 * 
	 * @param cxn
	 * @param astContainer
	 * 
	 * @return
	 */
	protected abstract UUID setupQuery(
			final BigdataSailRepositoryConnection cxn,
			ASTContainer astContainer, QueryType queryType, String extQueryId);
	
	/**
	 * Wrapper method to clean up query and throw exception is interrupted. 
	 * 
	 * @param queryId
	 * @throws QueryCancelledException 
	 */
	protected void finalizeQuery(final UUID queryId)
			throws QueryCancelledException {

		//Need to call before tearDown
		final boolean isQueryCancelled = isQueryCancelled(queryId);
		
		tearDownQuery(queryId);
		
		if(isQueryCancelled){
		
			if(log.isDebugEnabled()) {
				log.debug(queryId + " execution canceled.");
			}
			
			throw new QueryCancelledException(queryId + " execution canceled.",
					queryId);
        }
		
	}

	/**
	 * Embedded clients can override this to access query management
	 * capabilities.
	 * 
	 * @param absQuery
	 */
	protected abstract void tearDownQuery(UUID queryId);
	
	/**
	 * Helper method to determine if a query was cancelled.
	 * 
	 * @param queryId
	 * @return
	 */
	protected abstract boolean isQueryCancelled(final UUID queryId);

	/**
	 * Is this a read-only view of the graph?
	 */
	public abstract boolean isReadOnly();
	
}
