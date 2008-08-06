package com.bigdata.bfs;

/**
 * Thrown when the identified document was not found.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NotFoundException extends RuntimeException {
    
    /**
     * 
     */
    private static final long serialVersionUID = -6959588673977924047L;

    public NotFoundException(String id) {

        super("id="+id);
        
    }
    
}