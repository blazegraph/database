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
     * The transaction.
     */
    public final Tx tx;
    
    /**
     * The persistent identifier.
     */
    public final long id;
    
    public DataDeletedException(Tx tx,long id) {
       
        super( "Data is deleted: tx="+tx+", id="+id );
        
        this.tx = tx;
        
        this.id = id;
        
    }
    
}