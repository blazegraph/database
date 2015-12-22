package com.bigdata.sparse;

/**
 * A singleton object that causes the associated property value to be assigned
 * the next higher 32-bit integer value when it is written on the
 * {@link SparseRowStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AutoIncIntegerCounter implements IAutoIncrementCounter {

    public static final AutoIncIntegerCounter INSTANCE = new AutoIncIntegerCounter();
    
    private AutoIncIntegerCounter() {
        
    }
    
}