package com.bigdata.journal;

/**
 * Indicates that the current data version for the persistent identifier has
 * been deleted. An application MUST NOT attempt to resolve the persistent
 * identifier against the corresponding read-optimized database since that
 * could result in a read of an earlier version.
 */
public class DataDeletedException extends RuntimeException {
    
    private static final long serialVersionUID = -1457365876715608045L;

    /**
     * The persistent identifier.
     */
    public final int id;
    
    public DataDeletedException(int id) {
       
        super( "Data is deleted: id="+id );
        
        this.id = id;
        
    }
    
}