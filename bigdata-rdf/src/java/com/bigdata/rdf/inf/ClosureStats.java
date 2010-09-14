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
package com.bigdata.rdf.inf;

import com.bigdata.counters.CAT;

/**
 * Statistics collected when performing inference.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClosureStats {
    
    /**
     * The #of entailments that were added to the database (this includes axioms
     * and statements entailed given the data). This reports only the actual
     * change in the #of statements in the database across the closure
     * operation.
     */
    public final CAT mutationCount = new CAT();

    /**
     * Time to compute the entailments and store them within the database
     * (milliseconds).
     */
    public final CAT elapsed = new CAT();

    public ClosureStats() {
        
    }
    
    /**
     * 
     * @param mutationCount
     * @param elapsed
     */
    public ClosureStats(final long mutationCount,final long elapsed) {
        
        this.mutationCount.set(mutationCount);
        
        this.elapsed.set( elapsed);
        
    }
    
    public void add(final ClosureStats o) {
        
        this.mutationCount.add( o.mutationCount.get());
        
        this.elapsed.add(o.elapsed.get());
        
    }
    
    public String toString() {

        return getClass().getSimpleName() + "{mutationCount=" + mutationCount.estimate_get()
                + ", elapsed=" + elapsed.estimate_get() + "ms}";
        
    }
    
}
