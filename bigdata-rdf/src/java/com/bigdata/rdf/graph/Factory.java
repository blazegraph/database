package com.bigdata.rdf.graph;

/**
 * Singleton pattern for initializing a vertex state or edge state object
 * given the vertex or edge.
 * 
 * @param <V>
 *            The vertex or the edge.
 * @param <T>
 *            The object that will model the state of that vertex or edge in
 *            the computation.
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public abstract class Factory<V, T> {

    /**
     * Factory pattern.
     * 
     * @param value
     *            The value that provides the scope for the object.
     *            
     * @return The factory generated object.
     */
    abstract public T initialValue(V value);
    
}