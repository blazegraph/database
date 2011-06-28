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
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.io.SerializerUtil;


/**
 * Test helper provides an entry (aka value) for a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleEntry implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private transient static int nextId = 1;

    private final int id;
    
    /**
     * Create a new entry.
     */
    public SimpleEntry() {
        
        id = nextId++;
        
    }

    public SimpleEntry(int id){
    
        this.id = id;
        
    }
    
    public int id() {
        
        return id;
        
    }
    
    public String toString() {
        
        return ""+id;
        
    }

    /**
     * Note: <code>equals</code> has been overriden to transparently
     * de-serialize the given object when it is a <code>byte[]</code>. This
     * is a hack that provides backwards compatibility for some of the unit
     * tests when assume that objects (and not byte[]s) are stored in the
     * B+Tree and visited by the {@link ITupleIterator}.
     */
    public boolean equals(Object o) {
        
        if( this == o ) return true;
        
        if( o == null ) return false;
        
        if(o instanceof ITuple) {
            
            // hack to convert return from IEntryIterator to byte[].
            
            o = ((ITuple)o).getValue();
            
        }
        
        if(o instanceof byte[]) {

            // hack to deserialize a byte[].
            return id == ((SimpleEntry) SerializerUtil.deserialize((byte[])o)).id;
            
        }
        
        return id == ((SimpleEntry)o).id;
        
    }

}
