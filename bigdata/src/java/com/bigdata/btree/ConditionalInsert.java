/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.btree;

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