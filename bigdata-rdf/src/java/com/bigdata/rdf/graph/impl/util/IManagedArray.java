package com.bigdata.rdf.graph.impl.util;

/**
 * An interface for a managed array. Implementations of this interface may
 * permit transparent extension of the managed array.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IManagedByteArray.java 4548 2011-05-25 19:36:34Z thompsonbry $
 */
public interface IManagedArray<T> extends IArraySlice<T> {

    /**
     * Return the capacity of the backing buffer.
     */
    int capacity();

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total values.
     * The buffer may be grown by this operation but it will not be truncated.
     * 
     * @param capacity
     *            The minimum #of values in the buffer.
     */
    void ensureCapacity(int capacity);
    
}
