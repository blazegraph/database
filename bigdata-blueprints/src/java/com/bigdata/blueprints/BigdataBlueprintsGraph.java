package com.bigdata.blueprints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;


public abstract class BigdataBlueprintsGraph implements BigdataEventTransactionalGraph {
	// elements that we will be deleting from the store
	private ArrayList<BigdataElement> removedElements = new ArrayList<BigdataElement>(); 
	// vertices that we will be adding to the store
	private HashMap<String,BigdataVertex> addedVertices = new HashMap<String,BigdataVertex>();  
	// elements that we will be adding to the store
	private HashMap<String,BigdataEdge> addedEdges = new HashMap<String,BigdataEdge>();
	
	public BigdataBlueprintsGraph () {  }
	
	public void commit() {
		throwUnimplemented( "commit" );
	}
	
	public void rollback() {
		throwUnimplemented( "rollback" ); 
	}
	
	public void stopTransaction(TransactionalGraph.Conclusion conclusion) {
		throwUnimplemented( "stopTransaction" ); 
	}
	
	public void shutdown() {
		throwUnimplemented( "shutdown" );
	}
	
	public Vertex getVertex(Object id) { 
		// we can only remove an item from the "add" queue
		return addedVertices.get( (String) id );
	}
	
	public BigdataBlueprintsGraph  getBasseGraph() { return this; }
	
	public Edge addEdge(Object id, BigdataVertex outVertex, BigdataVertex inVertex, String label) {
		BigdataEdge edge = new BigdataEdge( (String)id, outVertex, inVertex, label );
		addedEdges.put((String)id, edge);
		return edge;
	}

	public Features getFeatures() {
		throwUnimplemented( "getFeatures" );
		return (Features)null;
	}
	
	public Vertex addVertex(Object id) {
		BigdataVertex v = new BigdataVertex( (String)id );
		addedVertices.put( (String)id, v );
		return v;
	}
	
	public void removeVertex(BigdataVertex vertex) {
		addedVertices.remove( vertex.getId() ); // if present
		removedElements.add( vertex );
	}
	
	public Iterable<Vertex> getVertices(String key, Object value) {
		throwUnimplemented( "getVertices(String key, Object value)" );
		return (Iterable<Vertex>)null;		
	}
	
	public Iterable<Vertex> getVertices() {
		// we only return what is in the "add" queue
        final List<Vertex> vertexList = new ArrayList<Vertex>();
        vertexList.addAll( addedVertices.values() );
		return vertexList;
	}
	
	public Edge getEdge(Object id) {
		// we can only remove an item from the "add" queue
		return addedEdges.get( (String) id );
	}
	
	public void removeEdge(BigdataEdge edge) {
		addedEdges.remove( edge.getId() ); // if present
		removedElements.add( edge );
	}
	
	public Iterable<Edge> getEdges(String key, Object value) {
		throwUnimplemented( "getEdges(String key, Object value)" );
		return (Iterable<Edge>)null;
	}
	
	public Iterable<Edge> getEdges() {
		// we only return what is in the add queue
        final List<Edge> edgeList = new ArrayList<Edge>();
        edgeList.addAll( addedEdges.values() );
		return edgeList;		
	}
	
	public GraphQuery query() {
		throwUnimplemented( "queries" );
		return (GraphQuery)null;		
	}
	
	// @SuppressWarnings("deprecation")
	private void throwUnimplemented(String method) {
		// unchecked( new Exception( "The '" + method + "' has not been implemented." ) );
		throw new NotImplementedException();
	}
	
	
	/* Maybe use later
	 * 
	public static RuntimeException unchecked(Throwable e) {
		BigdataBlueprintsGraph.<RuntimeException>throwAny(e);
		return null;
	}
    
	@SuppressWarnings("unchecked")
	private static <E extends Throwable> void throwAny(Throwable e) throws E {
		throw (E)e;
	}
    */
	
}
  
