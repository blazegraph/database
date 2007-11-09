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
/*
 * Created on May 17, 2007
 */

package com.bigdata.btree;

public class MutableValueBuffer implements IValueBuffer {

    private int nvals; 
    private final byte[][] vals;
    
    public MutableValueBuffer(int nvals, byte[][] vals) {
        
        assert nvals >= 0;
        assert vals != null;
        assert vals.length > 0;
        assert nvals <= vals.length;
        
        this.nvals = nvals;
        
        this.vals = vals;
        
    }
    
    final public int capacity() {
        
        return vals.length;
        
    }
    
    final public byte[] getValue(int index) {
        
        if (index >= nvals) {

            throw new IndexOutOfBoundsException();
            
        }
        
        return vals[index];

    }

    final public int getValueCount() {
        
        return nvals;
        
    }
    
}