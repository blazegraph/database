package com.bigdata.counters.query;

import java.util.Vector;

/**
 * The ordered set of values bound for a URL query parameter.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class URLQueryParam {
    
    public final String name;
    public final Vector<String> values;
    
    public URLQueryParam(String name, String value) {

        if (name == null)
            throw new IllegalArgumentException();

        this.name = name;

        if (value == null) {

            this.values = null;

        } else {

            Vector<String> values = new Vector<String>();

            values.add(value);

            this.values = values;

        }
        
    }
    
    public URLQueryParam(String name, String[] values) {
        
        if (name == null)
            throw new IllegalArgumentException();
        
        if (values == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        Vector<String> tmp = new Vector<String>();
        
        for(String s : values) {

            tmp.add(s);
            
        }
        
        this.values = tmp;
        
    }
    
}