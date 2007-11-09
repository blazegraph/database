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
package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.Var;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A constant (a term identifier for the lexicon).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class Id extends VarOrId {
    
    /**
     * Label is cached iff the constant is resolved against the db.
     */
    private String label = null;
    
    final public boolean isVar() {
        
        return false;
        
    }

    final public boolean isConstant() {
        
        return true;
        
    }

    public Id(long id) {
        
        super(id);
        
        assert id>0;
        
    }
    
    public String toString() {
        
        return ""+id;
        
    }

    public String toString(AbstractTripleStore db) {

        if (label != null) {
            
            // cached.
            return label;
            
        }
        
        if (db == null) {

            return toString();
            
        } else {

            // cache label.
            label = db.toString(id);

            return label;
            
        }
        
    }
    
    final public boolean equals(VarOrId o) {
    
        if (o instanceof Id && id == ((Id) o).id) {

            return true;
            
        }
        
        return false;
        
    }
    
    final public int hashCode() {
        
        // same has function as Long.hashCode().
        
        return (int) (id ^ (id >>> 32));
        
    }
    
    public int compareTo(VarOrId arg0) {

        // order vars before ids
        if(arg0 instanceof Var) return 1;
        
        Id o = (Id)arg0;
        
        /*
         * Note: logic avoids possible overflow of [long] by not computing the
         * difference between two longs.
         */
        
        int ret = id < o.id ? -1 : id > o.id ? 1 : 0;
        
        return ret;
        
    }

}
