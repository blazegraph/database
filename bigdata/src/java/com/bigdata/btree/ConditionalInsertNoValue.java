package com.bigdata.btree;

/**
 * User defined function supporting the conditional insert of a value iff no
 * entry is found under a search key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class ConditionalInsertNoValue implements UserDefinedFunction {
    
    private static final long serialVersionUID = 2942811843264522254L;

    public ConditionalInsertNoValue(Object value) {
        
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
        
        return INSERT_NULL;
        
    }

    public Object returnValue(byte[] key, Object oldval) {

        return oldval;
        
    }
    
}