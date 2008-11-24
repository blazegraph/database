package com.bigdata.config;

/**
 * Interface for validating property values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public interface IValidator<E> {

    /**
     * Convert a value to an instance of the generic type.
     * 
     * @param key
     *            The key.
     * @param val
     *            The value.
     * 
     * @return The converted value.
     */
    public E parse(String key, String val);
    
    /**
     * 
     * @param key
     *            The key under which the value was discovered.
     * @param val
     *            The value.
     * @param arg
     *            The parsed value.
     *            
     * @throws ConfigurationException
     */
    public void accept(String key, String val, E arg) throws ConfigurationException;
    
}