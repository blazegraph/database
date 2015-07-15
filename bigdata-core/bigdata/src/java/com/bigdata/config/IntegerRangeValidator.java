package com.bigdata.config;

/**
 * Variant that imposes a range constraint on the value.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IntegerRangeValidator extends IntegerValidator {

    final int min;
    final int max;
    
    public IntegerRangeValidator(int min,int max) {
    
        this.min = min;
        
        this.max = max;
        
    }
    
    /**
     * Accepts all values in the range specified to the ctor.
     */
    public void accept(String key, String val, Integer arg)
            throws ConfigurationException {

        if (arg < min || arg > max) {

            throw new ConfigurationException(key, val, "Must be in [" + min
                    + ":" + max + "]");
            
        }
        
    }

}