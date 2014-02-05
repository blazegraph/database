package com.bigdata.blueprints;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.Set;

import org.openrdf.model.vocabulary.RDFS;
import com.tinkerpop.blueprints.Element;

public class BigdataElement implements Element {

	protected String id = null; // must be a URI
	
	// implied here is that the properties exist in the graph store, we would need a 2nd property setter
	private HashMap<String,String> properties = new HashMap<String,String>();
	// properties that we will be deleting from the store
	private HashMap<String,String> removedProperties = new HashMap<String,String>(); 
	// properties that we will be adding to the store
	private HashMap<String,String> addedProperties = new HashMap<String,String>(); 
	
	public BigdataElement(String id) {
		this.id = id;
	}
	
	public BigdataElement(String id, String label) {
		this.id = id;
		setProperty( RDFS.LABEL.toString(), label );
	}

	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key) {
		return (T) properties.get(key);
	}
	
	public Set<String> getPropertyKeys() {
		Set<String> keys = properties.keySet();
		keys.addAll( addedProperties.keySet() );
		return keys;
	}
	
	public void setProperty(String key, Object value) {
		addedProperties.put(key,(String)value );
		properties.put(key, (String)value);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T removeProperty(String key) {
		removedProperties.put(key, key);
		return (T) properties.remove(key);
	}
	
	public void remove() {
		// delete from graph
		throw new NotImplementedException();
	}

	public Object getId() {
		return id;
	}
	
	public boolean equals(Object obj) {
		return obj.toString().equals(this.toString());
	}
	
	public String getLabel() {
		return getProperty( RDFS.LABEL.toString() );
	}

}
