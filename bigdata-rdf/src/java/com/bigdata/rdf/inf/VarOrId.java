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

import org.openrdf.model.Value;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;

/**
 * A class that models either a constant identifier for an RDF {@link Value} or
 * an unbound variable.
 */
abstract public class VarOrId implements Comparable<VarOrId>{

    static protected final transient long NULL = IRawTripleStore.NULL;
    
    /**
     * The reserved value {@link #NULL} is used to denote variables, otherwise
     * this is the long integer assigned by {@link ITripleStore#addTerm(Value)}.
     */
    public final long id;

    abstract public boolean isVar();

    abstract public boolean isConstant();
    
    protected VarOrId(long id) {
        
        this.id = id;
        
    }

    abstract public boolean equals(VarOrId o);

    abstract public int hashCode();
    
    abstract public String toString();
    
    abstract public String toString(AbstractTripleStore db);
    
}
