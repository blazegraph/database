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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.bigdata.rdf.internal.XSD;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;

/**
 * Default implementation of a {@link BlueprintsRDFFactory} for converting 
 * blueprints data to RDF and back again.  Uses simple namespacing and URL
 * encoding to round trip URIs.
 * 
 * @author mikepersonick
 *
 */
public class BigdataRDFFactory implements BlueprintsRDFFactory {

	public static BigdataRDFFactory INSTANCE = new BigdataRDFFactory();
	
	/**
	 * Namespace for non vertex and edge data (property names).
	 */
	public static final String GRAPH_NAMESPACE = "http://www.bigdata.com/rdf/graph/";
	
	/**
	 * Namesace for vertices.
	 */
	public static final String VERTEX_NAMESPACE = "http://www.bigdata.com/rdf/graph/vertex/";
	
    /**
     * Namesace for edges.
     */
	public static final String EDGE_NAMESPACE = "http://www.bigdata.com/rdf/graph/edge/";
	
	
	private final ValueFactory vf;
	
	/**
	 * Construct an instance with a simple Sesame ValueFactoryImpl.
	 */
	private BigdataRDFFactory() {
		this.vf = new ValueFactoryImpl();
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

			return vf.createURI(GRAPH_NAMESPACE, URLEncoder.encode(property, "UTF-8"));
				
		} catch (UnsupportedEncodingException e) {
			
			throw new RuntimeException(e);
			
		}
		
	}

	/**
	 * Returns true if the URI is part of the vertex namespace.
	 */
	public boolean isVertex(final URI uri) {
		
		final String s = uri.stringValue();
		
		return s.startsWith(VERTEX_NAMESPACE);
		
	}
	
	/**
	 * Returns true if the URI is part of the edge namespace.
	 */
	public boolean isEdge(final URI uri) {
		
		final String s = uri.stringValue();
		
		return s.startsWith(EDGE_NAMESPACE);
		
	}
	
    /**
     * {@inheritDoc}
     */
	@Override
	public String fromVertexURI(final URI uri) {
		
		return fromURI(uri);
		
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public String fromEdgeURI(final URI uri) {
		
		return fromURI(uri);
		
	}

    /**
     * {@inheritDoc}
     */
	@Override
	public String fromPropertyURI(final URI uri) {
		
		return fromURI(uri);
		
	}

	/**
	 * Round-trip a URI back into a graph element ID or property name.
	 */
	public static String fromURI(final URI uri) {
		
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
