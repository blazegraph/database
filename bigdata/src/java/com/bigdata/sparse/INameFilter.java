package com.bigdata.sparse;

/**
 * Filter used to select column names.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INameFilter {

    /**
     * Return true to select values for the property with the given name.
     * 
     * @param name
     *            The property (aka column) name.
     */
    public boolean accept(String name);
    
}