/*

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

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Abstract base class used to filter objects in an {@link ITupleIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class TupleFilter implements ITupleFilter {

    protected final Object state;

    public TupleFilter() {
        
        this( null );
        
    }

    /**
     * Constructor initializes a user-defined object that will be available
     * during {@link #isValid()} tests.
     * 
     * @param state
     *            The user defined object.
     */
    public TupleFilter(Object state) {
        
        this.state = state;
        
    }

    /**
     * Lazily allocated.
     */
    private ArrayList<ITupleFilter> filters = null; 

    /**
     * Chains a filter after this one.
     * 
     * @param filter
     */
    synchronized public void add(ITupleFilter filter) {
        
        if (filter == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        if (filters == null) {
            
            filters = new ArrayList<ITupleFilter>();
            
        }
        
        filters.add( filter );
        
    }
    
    public boolean isValid(ITuple tuple) {
        
        if (filters == null)
            return true;
        
        Iterator<ITupleFilter> itr = filters.iterator();
        
        while(itr.hasNext()) {

            ITupleFilter filter = itr.next();
            
            if(!filter.isValid(tuple)) {
                
                return false;
                
            }
            
        }

        return true;
        
    }

    public void rewrite(ITuple tuple) {

        if (filters == null) {
         
            return;
            
        }

        Iterator<ITupleFilter> itr = filters.iterator();
        
        while(itr.hasNext()) {

            ITupleFilter filter = itr.next();
            
            filter.rewrite(tuple);
            
        }
        
    }
    
}
