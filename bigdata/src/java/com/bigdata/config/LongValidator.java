/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Nov 23, 2008
 */

package com.bigdata.config;

/**
 * Base impl for {@link Long}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LongValidator implements IValidator<Long> {

    /**
     * Allows all values.
     */
    public static final transient IValidator<Long> DEFAULT = new LongValidator();

    /**
     * Only allows non-negative values (GTE ZERO).
     */
    public static final transient IValidator<Long> GTE_ZERO = new LongValidator() {
        
        public void accept(String key, String val, Long arg) {
            
            if (arg < 0)
                throw new ConfigurationException(key, val,
                        "Must be non-negative");
            
        }
        
    };

    /**
     * Only allows positive values (GT ZERO).
     */
    public static final transient IValidator<Long> GT_ZERO = new LongValidator() {
        
        public void accept(String key, String val, Long arg) {

            if (arg <= 0)
                throw new ConfigurationException(key, val,
                        "Must be positive");
            
        }
        
    };
    
    protected LongValidator() {
        
    }

    public Long parse(String key, String val) {
        
        return Long.parseLong(val);
        
    }
    
    /**
     * Accepts all values by default.
     */
    public void accept(String key, String val, Long arg)
            throws ConfigurationException {
        
    }
    
}
