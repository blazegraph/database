package com.bigdata.blueprints;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


public class BigdataEdge extends BigdataElement implements Edge {

	protected BigdataVertex vOut = null;
	protected BigdataVertex vIn  = null;
	
	
	public BigdataEdge(String id) {
		super(id);
	}
	
	public BigdataEdge(String id, String label) {
		super(id,label);
	}
	
	public BigdataEdge(String id, BigdataVertex out, BigdataVertex in, String label) {
		super(id,label);
		this.vOut = out;
		this.vIn = in;
	}
	
	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
		if( direction == Direction.IN ) {
			return vIn;
		}
		else if( direction == Direction.OUT ) {
			return vOut;
		}
		else {
			throw new NotImplementedException();
		}
	}

	@Override
	public void remove() {
		throw new NotImplementedException();
	}

	public String toString() {
		// toTTLString();
		return "Not Implemented";
	}
		
}
