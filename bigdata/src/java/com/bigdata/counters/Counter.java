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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

/**
 * A named counter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 *            The data type for the counter value.
 * 
 * @todo declare units, description.
 * @todo unit conversions.
 */
public abstract class Counter<T> implements ICounter {
    
    private final ICounterSet parent;
    private final String name;
    
    public Counter(ICounterSet parent, String name) {
        
        if (parent == null)
            throw new IllegalArgumentException();
        
        if (name == null)
            throw new IllegalArgumentException();
        
        this.parent = parent;
        
        this.name = name;
        
    }

    public ICounterSet getParent() {
        
        return parent;
        
    }
    
    public String getName() {
        
        return name;
        
    }
    
    public String getPath() {
        
        if(parent.isRoot()) {
            
            return parent.getPath()+name;
            
        }
        
        return parent.getPath() + ICounterSet.pathSeparator + name;
        
    }
    
    public String toString() {
        
        return getPath();
        
    }
    
}
