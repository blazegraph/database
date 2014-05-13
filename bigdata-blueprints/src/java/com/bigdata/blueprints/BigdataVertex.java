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

import org.openrdf.model.URI;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * Vertex implementation that wraps a Vertex URI and points to a 
 * {@link BigdataGraph} instance.
 * 
 * @author mikepersonick
 *
 */
public class BigdataVertex extends BigdataElement implements Vertex {

	public BigdataVertex(final URI uri, final BigdataGraph graph) {
		super(uri, graph);
	}
	
	@Override
	public Object getId() {
	    
		return graph.factory.fromVertexURI(uri);
		
	}
	
	@Override
	public void remove() {
	    
		graph.removeVertex(this);
		
	}

	@Override
	public Edge addEdge(final String label, final Vertex to) {
	    
		return graph.addEdge(null, this, to, label);
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Edge> getEdges(final Direction dir, final String... labels) {
	    
		final URI wild = null;
		
		if (dir == Direction.OUT) {
		    
			return graph.getEdges(uri, wild, labels);
			
		} else if (dir == Direction.IN) {
		    
			return graph.getEdges(wild, uri, labels);
			
		} else {
		    
			return graph.fuse(
					graph.getEdges(uri, wild, labels),
					graph.getEdges(wild, uri, labels));
			
		}
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Vertex> getVertices(final Direction dir, final String... labels) {
	    
		final URI wild = null;
		
		if (dir == Direction.OUT) {
		    
			return graph.getVertices(uri, wild, labels);
			
		} else if (dir == Direction.IN) {
		    
			return graph.getVertices(wild, uri, labels);
			
		} else {
		    
			return graph.fuse(
					graph.getVertices(uri, wild, labels),
					graph.getVertices(wild, uri, labels));
			
		}
		
	}

	/**
	 * Not bothering to provide a SPARQL translation for vertex queries at
	 * this time.  I suspect that scan and filter works fine when starting from
	 * an individual vertex.
	 */
	@Override
	public VertexQuery query() {
	    
		return new DefaultVertexQuery(this);
		
	}

	@Override
	public String toString() {
	    
	    return "v["+uri.getLocalName()+"]";
	    
	}
	
}
