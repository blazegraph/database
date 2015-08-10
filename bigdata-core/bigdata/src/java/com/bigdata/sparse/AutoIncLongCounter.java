package com.bigdata.sparse;

/**
 * A singleton object that causes the associated property value to be assigned
 * the next higher 64-bit integer value when it is written on the
 * {@link SparseRowStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AutoIncLongCounter implements IAutoIncrementCounter {

    public static final AutoIncLongCounter INSTANCE = new AutoIncLongCounter();
    
    private AutoIncLongCounter() {
        
    }
    
}