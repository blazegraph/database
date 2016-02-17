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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.internal.XSD;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;

/**
 * Default implementation of a {@link BlueprintsValueFactory} for converting 
 * blueprints data to RDF and back again.  Uses simple namespacing and URL
 * encoding to round trip URIs.
 * 
 * @author mikepersonick
 *
 */
public class DefaultBlueprintsValueFactory implements BlueprintsValueFactory {

	/**
	 * Namespace for non vertex and edge data (property names).
	 */
	public final String GRAPH_NAMESPACE;// = "http://www.bigdata.com/rdf/graph/";
	
	/**
	 * Namespace for vertices.
	 */
	public final String VERTEX_NAMESPACE;// = "http://www.bigdata.com/rdf/graph/vertex/";
	
    /**
     * Namespace for edges.
     */
	public final String EDGE_NAMESPACE;// = "http://www.bigdata.com/rdf/graph/edge/";
	
    /**
     * URI used for typing elements.
     */
	public final URI TYPE;
	
    /**
     * URI used to represent a Vertex.
     */
    public final URI VERTEX;
    
    /**
     * URI used to represent a Edge.
     */
    public final URI EDGE;

    /**
     * URI used for labeling edges.
     */
    public final URI LABEL;

    /**
     * The Sesame value factory for URIs and Literals.
     */
	protected final ValueFactory vf;
	
    /**
     * Construct an instance with a simple Sesame ValueFactoryImpl.
     */
    public DefaultBlueprintsValueFactory(final String graphNamespace,
            final String vertexNamespace, final String edgeNamespace,
            final URI type, final URI vertex, final URI edge, final URI label) {
        
        this(new ValueFactoryImpl(), graphNamespace, vertexNamespace,
                edgeNamespace, type, vertex, edge, label);
        
    }

	/**
	 * Construct an instance with the specified ValueFactoryImpl.
	 */
    public DefaultBlueprintsValueFactory(final ValueFactory vf,
            final String graphNamespace, final String vertexNamespace,
            final String edgeNamespace, final URI type, final URI vertex,
            final URI edge, final URI label) {
	    
		this.vf = vf;
		this.GRAPH_NAMESPACE = graphNamespace;
		this.VERTEX_NAMESPACE = vertexNamespace;
		this.EDGE_NAMESPACE = edgeNamespace;
		this.TYPE = type;
		this.VERTEX = vertex;
		this.EDGE = edge;
		this.LABEL = label;
		
	}
	
    @Override
    public URI getTypeURI() {
        return TYPE;
    }

    @Override
    public URI getVertexURI() {
        return VERTEX;
    }

    @Override
    public URI getEdgeURI() {
        return EDGE;
    }

    @Override
    public URI getLabelURI() {
        return LABEL;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public URI toVertexURI(final Object key) {
		
		try {

			final String id = key.toString();
			
			return vf.createURI(VERTEX_NAMESPACE, URLEncoder.encode(id, "UTF-8"));
				
		} catch (UnsupportedEncodingException e) {
			
			throw new RuntimeException(e);
			
		}
		
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public URI toEdgeURI(final Object key) {
		
		try {

			final String id = key.toString();
			
			return vf.createURI(EDGE_NAMESPACE, URLEncoder.encode(id, "UTF-8"));
				
		} catch (UnsupportedEncodingException e) {
			
			throw new RuntimeException(e);
			
		}
		
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public URI toURI(final Element e) {
		
		if (e instanceof Edge) {
			
			return toEdgeURI(e.getId());
			
		} else {
			
			return toVertexURI(e.getId());
			
		}
		
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public URI toPropertyURI(final String property) {
		
		try {

		    if (property.equals("label")) {
		     
		        /*
		         * Label is a reserved property for edge labels, we use
		         * rdfs:label for that.
		         */
		        return RDFS.LABEL;
		        
		    } else {
			
		        return vf.createURI(GRAPH_NAMESPACE, URLEncoder.encode(property, "UTF-8"));
		        
		    }
				
		} catch (UnsupportedEncodingException e) {
			
			throw new RuntimeException(e);
			
		}
		
	}

//	/**
//	 * Returns true if the URI is part of the vertex namespace.
//	 */
////    @Override
//	public boolean isVertex(final URI uri) {
//		
//		final String s = uri.stringValue();
//		
//		return s.startsWith(VERTEX_NAMESPACE);
//		
//	}
//	
//	/**
//	 * Returns true if the URI is part of the edge namespace.
//	 */
////	@Override
//	public boolean isEdge(final URI uri) {
//		
//		final String s = uri.stringValue();
//		
//		return s.startsWith(EDGE_NAMESPACE);
//		
//	}
//	
//    /**
//     * Returns true if the URI is part of the property namespace.
//     */
////    @Override
//    public boolean isProperty(final URI uri) {
//        
//        return !isVertex(uri) && !isEdge(uri);
//        
//    }
    
//    /**
//     * {@inheritDoc}
//     */
//	@Override
//	public String fromVertexURI(final URI uri) {
//		
//		return fromURI(uri);
//		
//	}
//
//    /**
//     * {@inheritDoc}
//     */
//	@Override
//	public String fromEdgeURI(final URI uri) {
//		
//		return fromURI(uri);
//		
//	}
//
//    /**
//     * {@inheritDoc}
//     */
//	@Override
//	public String fromPropertyURI(final URI uri) {
//		
//		return fromURI(uri);
//		
//	}

	/**
	 * Round-trip a URI back into a graph element ID or property name.
	 */
	@Override
	public String fromURI(final URI uri) {
		
		if (uri == null) {
			throw new IllegalArgumentException();
		}
		
		try {

			return URLDecoder.decode(uri.getLocalName(), "UTF-8");
			
		} catch (UnsupportedEncodingException e) {
			
			throw new RuntimeException(e);
			
		}
		
	}
	
    /**
     * {@inheritDoc}
     * <p>
     * Supports: Float, Double, Integer, Long, Boolean, Short, Byte, and String.
     */
	@Override
	public Literal toLiteral(final Object value) {

//		/*
//		 * Need to handle this better.
//		 */
//		if (value instanceof Collection) {
//			return vf.createLiteral(Arrays.toString(((Collection<?>) value).toArray()));
//		}
		
		if (value instanceof Float) {
			return vf.createLiteral((Float) value);
		} else if (value instanceof Double) {
			return vf.createLiteral((Double) value);
		} else if (value instanceof Integer) {
			return vf.createLiteral((Integer) value);
		} else if (value instanceof Long) {
			return vf.createLiteral((Long) value);
		} else if (value instanceof Boolean) {
			return vf.createLiteral((Boolean) value);
		} else if (value instanceof Short) {
			return vf.createLiteral((Short) value);
		} else if (value instanceof Byte) {
			return vf.createLiteral((Byte) value);
		} else if (value instanceof String) { // treat as string by default
			return vf.createLiteral((String) value);
		} else {
		    throw new IllegalArgumentException();
		}
		
	}
	
	/**
     * {@inheritDoc}
     * <p>
	 * Return a graph property from a datatyped literal using its
	 * XSD datatype.
     * <p>
     * Supports: Float, Double, Integer, Long, Boolean, Short, Byte, and String.
     */
	@Override
	public Object fromLiteral(final Literal l) {
		
		final URI datatype = l.getDatatype();
		
		if (datatype == null) {
			return l.getLabel();
		} else if (datatype.equals(XSD.FLOAT)) {
			return l.floatValue();
		} else if (datatype.equals(XSD.DOUBLE)) {
			return l.doubleValue();
		} else if (datatype.equals(XSD.INT)) {
			return l.intValue();
		} else if (datatype.equals(XSD.LONG)) {
			return l.longValue();
		} else if (datatype.equals(XSD.BOOLEAN)) {
			return l.booleanValue();
		} else if (datatype.equals(XSD.SHORT)) {
			return l.shortValue();
		} else if (datatype.equals(XSD.BYTE)) {
			return l.byteValue();
		} else {
			return l.getLabel();
		}
		
	}

}
