package com.bigdata.bfs;

/**
 * Thrown when the identified document already exists.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ExistsException extends RuntimeException {
    
    /**
     * 
     */
    private static final long serialVersionUID = 7490932737424526172L;

    public ExistsException(String id) {

        super("id="+id);
        
    }
    
}