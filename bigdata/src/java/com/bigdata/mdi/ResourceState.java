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
package com.bigdata.mdi;

import com.bigdata.btree.IndexSegment;

/**
 * Enumeration of life-cycle states for {@link IndexSegment}s in a
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum ResourceState {

    New("New",(short)0),
    Live("Live",(short)1),
    Dead("Dead",(short)2);
    
    final private String name;
    final private short id;
    
    ResourceState(String name, short id) {
        this.name = name;
        this.id = id;
    }
    
    public String toString() {return name;}
    
    public short valueOf() {return (short)id;}
    
    static public ResourceState valueOf(short id) {
        switch(id) {
        case 0: return New;
        case 1: return Live;
        case 2: return Dead;
        default: throw new IllegalArgumentException("Unknown: code="+id);
        }
    }
    
}