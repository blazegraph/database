package com.bigdata.blueprints;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * 
 * @author dmekonnen
 * 
 * A BigdataVertex is simple container for vertex attributes and is not bound to a specific graph at this time.
 * Edges are not tracked by this implementation, thus Edge based operations (getEdges, addEdge, getVertices)
 * are unimplemented.  Edges necessarily have a source and target vertex and so the BigdataEdge class will
 * track associated vertices.
 *
 */
public class BigdataVertex extends BigdataElement implements Vertex {
	
	public BigdataVertex(String id) {
		super(id);
	}
	
	public BigdataVertex(String id, String label) {
		super(id,label);
	}

	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		// "direction" is the direction of the edges
		// Direction.IN  - this vertex is the SPO object
		// Direction.OUT - this vertex is the SPO subject
		throw new NotImplementedException();
	}		

    public Iterable<Edge> getEdges(final Direction direction, final String... labels) {
		// Direction.IN  - this vertex is the SPO object
		// Direction.OUT - this vertex is the SPO subject
		throw new NotImplementedException();       
    }

	public VertexQuery query() {
		return new DefaultVertexQuery(this);
	}

	public Edge addEdge(String label, Vertex inVertex) {
		// this vertex is the SPO subject
		// the edge label is not useful for identifying a predicate in the RDF context,
		// we can interpret the "label" here as an IRI but should validate it as such.
		throw new NotImplementedException();
	}

	public String toString() {
		// toTTLString();
		return "Not Implemented";
	}
	 
}