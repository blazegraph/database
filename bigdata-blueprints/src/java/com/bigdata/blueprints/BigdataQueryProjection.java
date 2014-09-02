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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.blueprints.BigdataSelection.Bindings;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class BigdataQueryProjection {
    
    private static final transient Logger log = Logger.getLogger(BigdataQueryProjection.class);

    private final BlueprintsValueFactory factory;
    
    public BigdataQueryProjection(final BlueprintsValueFactory factory) {
        
        this.factory = factory;
        
    }
    
    public BigdataSelection convert(final TupleQueryResult result) 
            throws Exception {
        
        final BigdataSelection selection = new BigdataSelection();
        
        while (result.hasNext()) {
            
            final BindingSet bs = result.next();
            
            final Bindings bindings = selection.newBindings();
            
            for (String key : bs.getBindingNames()) {
                
                final Value val= bs.getBinding(key).getValue();
                
                final Object o;
                if (val instanceof Literal) {
                    o = factory.fromLiteral((Literal) val);
                } else if (val instanceof URI) {
                    o = factory.fromURI((URI) val);
                } else {
                    throw new RuntimeException("bnodes not legal: " + val);
                }
                
                bindings.put(key, o);
                
            }
            
        }
        
        return selection;
        
    }
        
    public BigdataGraphlet convert(final GraphQueryResult stmts) throws Exception {
        
        final PartialGraph elements = new PartialGraph();
        
        while (stmts.hasNext()) {
            
            final Statement stmt = stmts.next();
            
            if (log.isInfoEnabled()) {
                log.info(stmt);
            }
            
            final Value o = stmt.getObject();
            
            if (o instanceof URI) {
                
                handleEdge(elements, stmt);
                
            } else if (o instanceof Literal) {
                
                handleProperty(elements, stmt);
                    
            } else {
                
                // how did we get a bnode?
//                log.warn("ignoring: " + stmt);
                
            }
            
        }
        
        /*
         * Attach properties to edges.
         */
        final Iterator<Map.Entry<URI, PartialElement>> it = elements.properties.entrySet().iterator();
        
        while (it.hasNext()) {
            
            final Map.Entry<URI, PartialElement> e = it.next();
            
            final URI uri = e.getKey();
            
            final PartialElement element = e.getValue();
            
            boolean isEdge = false;
            
            for (Statement stmt : elements.edges.keySet()) {
                
                if (stmt.getPredicate().equals(uri)) {
                
                    isEdge = true;
                    
                    final PartialEdge edge = elements.edges.get(stmt);
                
                    edge.copyProperties(element);
                    
                }
                
            }
            
            if (isEdge) {
                it.remove();
            }
            
        }

        /*
         * Attach properties to vertices.
         */
        for (URI uri : elements.properties.keySet()) {
            
            final PartialElement element = elements.properties.get(uri);
            
            if (log.isInfoEnabled()) {
                log.info(uri + ": " + element);
            }
            
            final PartialVertex v = elements.putIfAbsent(uri);
            
            v.copyProperties(element);
            
        }
        
        /*
         * Fill in any missing edge label.
         */
        for (PartialEdge edge : elements.edges.values()) {
            
            if (edge.getLabel() == null) {
                
                edge.setLabel(edge.getId().toString());
                
            }
            
        }
        
//        /*
//         * Prune any incomplete edges.
//         */
//        final Iterator<Element> it = elements.values().iterator();
//        
//        while (it.hasNext()) {
//            
//            final Element e = it.next();
//            
//            if (e instanceof PartialEdge) {
//                
//                if (!((PartialEdge) e).isComplete()) {
//                    
//                    it.remove();
//                    
//                }
//                
//            }
//            
//        }

        return new BigdataGraphlet(
                elements.vertices.values(), elements.edges.values());
        
    }
    
    private void handleEdge(final PartialGraph elements, final Statement stmt) {
        
        if (log.isTraceEnabled()) {
            log.trace(stmt);
        }
        
        final PartialVertex from = elements.putIfAbsent((URI) stmt.getSubject());
        
        final PartialEdge edge = elements.putIfAbsent(stmt);
        
        final PartialVertex to = elements.putIfAbsent((URI) stmt.getObject());
        
        edge.setFrom(from);
        
        edge.setTo(to);
        
//        // use the default label
//        edge.setLabel(factory.fromEdgeURI(stmt.getPredicate()));
        
    }
    
    private void handleProperty(final PartialGraph elements, final Statement stmt) {

//        if (log.isInfoEnabled()) {
//            log.info(stmt);
//        }
        
        final URI uri = (URI) stmt.getSubject();
        
        final PartialElement element = elements.putElementIfAbsent(uri);
        
        final String prop = factory.fromURI(stmt.getPredicate());

        final Object val = factory.fromLiteral((Literal) stmt.getObject());
        
//        if (prop.equals("label") && element instanceof PartialEdge) {
//            
//            ((PartialEdge) element).setLabel(val.toString());
//            
//        } else {
            
            element.setProperty(prop, val);
            
//        }
        
    }
    
//    private PartialElement putIfAbsent(final URI uri) {
//
//        if (factory.isEdge(uri)) {
//            
//            return putEdgeIfAbsent(uri);
//            
//        } else if (factory.isVertex(uri)) {
//            
//            return putVertexIfAbsent(uri);
//            
//        } else {
//            
//            throw new RuntimeException("bad element: " + uri);
//            
//        }
//        
//    }
//
    private class PartialGraph {
        
        private final Map<URI, PartialElement> properties = new LinkedHashMap<URI, PartialElement>();
        
        private final Map<Statement, PartialEdge> edges = new LinkedHashMap<Statement, PartialEdge>();
        
        private final Map<URI, PartialVertex> vertices = new LinkedHashMap<URI, PartialVertex>();
        
        private PartialElement putElementIfAbsent(final URI uri) {
            
            final String id = uri.toString();
            
            if (properties.containsKey(uri)) {
                
                return (PartialElement) properties.get(uri);
                
            } else {
                
                final PartialElement e = new PartialElement(id);
                
                properties.put(uri, e);
                
                return e;
                
            }
            
        }
        
        private PartialVertex putIfAbsent(final URI uri) {
            
            final String id = factory.fromURI(uri);
            
            if (vertices.containsKey(uri)) {
                
                return (PartialVertex) vertices.get(uri);
                
            } else {
                
                final PartialVertex v = new PartialVertex(id);
                
                vertices.put(uri, v);
                
                return v;
                
            }
            
        }
        
        private PartialEdge putIfAbsent(final Statement stmt) {
            
            final URI uri = stmt.getPredicate();
            
            final String id = factory.fromURI(uri);
            
            if (edges.containsKey(stmt)) {
                
                return (PartialEdge) edges.get(stmt);
                
            } else {
                
                final PartialEdge e = new PartialEdge(id);
                
                edges.put(stmt, e);
                
                return e;
                
            }
            
        }
        
    }
    
    private static class PartialElement implements Element {

        private final String id;
        
        private final Map<String, Object> properties = 
                new LinkedHashMap<String, Object>();
        
        public PartialElement(final String id) {
            this.id = id;
        }
        
        @Override
        public Object getId() {
            return id;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object getProperty(final String name) {
            return properties.get(name);
        }

        @Override
        public Set<String> getPropertyKeys() {
            return properties.keySet();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object removeProperty(final String key) {
            return properties.remove(key);
        }

        @Override
        public void setProperty(final String key, final Object value) {
            
            /*
             * Gracefully turn a single value property into a 
             * multi-valued property.
             */
            if (properties.containsKey(key)) {
            
                final Object o = properties.get(key);
                
                if (o instanceof List) {
                    
                    @SuppressWarnings("unchecked")
                    final List<Object> list = (List<Object>) o;
                    list.add(value);
                    
                } else {
                    
                    final List<Object> list = new LinkedList<Object>();
                    list.add(o);
                    list.add(value);
                    
                    properties.put(key, list);
                    
                }
                
            } else {
            
                properties.put(key, value);
                
            }
            
        }
        
        public void copyProperties(final PartialElement element) {
            properties.putAll(element.properties);
        }
        
    }
    
    private static class PartialVertex extends PartialElement implements Vertex {
        
        public PartialVertex(final String id) {
            super(id);
        }

        @Override
        public Edge addEdge(String arg0, Vertex arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Edge> getEdges(Direction arg0, String... arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public VertexQuery query() {
            throw new UnsupportedOperationException();
        }
        
    }

    private static class PartialEdge extends PartialElement implements Edge {
        
        private String label;
        
        private Vertex from;
        
        private Vertex to;
        
        public PartialEdge(final String id) {
            super(id);
        }

        @Override
        public String getLabel() {
            return label;
        }

        @Override
        public Vertex getVertex(final Direction dir) throws IllegalArgumentException {
            
            if (dir == Direction.OUT) {
                return from;
            } else if (dir == Direction.IN) {
                return to;
            }
            
            throw new IllegalArgumentException();
            
        }
        
        private boolean isComplete() {
            return label != null && from != null && to != null;
        }
        
        private void setLabel(final String label) {
            this.label = label;
        }
        
        private void setFrom(final Vertex v) {
            this.from = v;
        }
        
        private void setTo(final Vertex v) {
            this.to = v;
        }
        
    }

}
