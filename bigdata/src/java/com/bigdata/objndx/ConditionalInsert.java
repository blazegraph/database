package com.bigdata.objndx;

/**
 * User defined function supporting the conditional insert of a value iff no
 * entry is found under a search key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class ConditionalInsert implements UserDefinedFunction {
    
    private static final long serialVersionUID = 6010273297534716024L;

    final Object value;
    
    /**
     * 
     * @param value The value to insert if the key is not found.  Note that a
     * <code>null</code> will result in a null value being associated with the
     * key if the key is not found.
     */
    public ConditionalInsert(Object value) {
        
        this.value = value;
        
    }

    /**
     * Do not insert if an entry is found.
     */
    public Object found(byte[] key, Object oldval) {
        
        return oldval;
        
    }

    /**
     * Insert if not found.
     */
    public Object notFound(byte[] key) {
        
        if(value==null) return INSERT_NULL;
        
        return value;
        
    }

    public Object returnValue(byte[] key, Object oldval) {

        return oldval;
        
    }
    
}