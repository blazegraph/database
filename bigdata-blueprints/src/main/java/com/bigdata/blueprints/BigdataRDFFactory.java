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

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.store.BD;

/**
 * Default implementation of a {@link BlueprintsValueFactory} for converting 
 * blueprints data to RDF and back again.  Uses simple namespacing and URL
 * encoding to round trip URIs.
 * 
 * @author mikepersonick
 *
 */
public class BigdataRDFFactory extends DefaultBlueprintsValueFactory {

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
	
    /**
     * URI used to represent a Vertex.
     */
    public static final URI VERTEX = new URIImpl(BD.NAMESPACE + "Vertex");
    
    /**
     * URI used to represent a Edge.
     */
    public static final URI EDGE = new URIImpl(BD.NAMESPACE + "Edge");
    
    public static BigdataRDFFactory INSTANCE = new BigdataRDFFactory();
    
	/**
	 * Construct an instance with a simple Sesame ValueFactoryImpl.
	 */
	private BigdataRDFFactory() {
		super(new ValueFactoryImpl(), 
		        GRAPH_NAMESPACE, VERTEX_NAMESPACE, EDGE_NAMESPACE,
		        RDF.TYPE, VERTEX, EDGE, RDFS.LABEL);
	}
	
}
