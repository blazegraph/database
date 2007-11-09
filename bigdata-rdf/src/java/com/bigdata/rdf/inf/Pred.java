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

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A predicate is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we
 * are only concerned with predicates of the form <code>triple(s,p,o)</code>
 * or <code>magic(triple(s,p,o))</code>. Since this is a boolean
 * distinction, we capture it with a boolean flag rather than allowing a
 * predicate name and arity.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class Pred {

    public final boolean magic;

    public final VarOrId s;

    public final VarOrId p;

    public final VarOrId o;

    /**
     * Return the variable or constant at the specified index.
     * 
     * @param index
     *            The index. Use 0 for the subject, 1 for the predicate, or 2
     *            for the object.
     *            
     * @return The variable or constant at the specified index.
     */
    public final VarOrId get(int index) {
        switch (index) {
        case 0:
            return s;
        case 1:
            return p;
        case 2:
            return o;
        default:
            throw new IndexOutOfBoundsException(""+index);
        }
    }
    
    /**
     * Return the index of the variable or constant in the {@link Pred}.
     * 
     * @param t
     *            The variable or constant.
     * 
     * @return The index of that variable or constant. The index will be 0 for
     *         the subject, 1 for the predicate, or 2 for the object. if the
     *         variable or constant does not occur in this {@link Pred} then
     *         <code>-1</code> will be returned.
     */
    public final int indexOf(VarOrId t) {

        // variables use a singleton factory.
        if( s == t ) return 0;
        if( p == t ) return 1;
        if( o == t ) return 2;
        
        // constants do not give the same guarentee.
        if(s.equals(t)) return 0;
        if(p.equals(t)) return 1;
        if(o.equals(t)) return 2;
        
        return -1;
        
    }
    
    /**
     * Return true iff all arguments of the predicate are bound (vs
     * variables).
     */
    public boolean isConstant() {

        return !s.isVar() && !p.isVar() && !o.isVar();

    }

    /**
     * The #of arguments in the predicate that are variables (vs constants).
     */
    public int getVariableCount() {
        
        return (s.isVar() ? 1 : 0) + (p.isVar() ? 1 : 0) + (o.isVar() ? 1 : 0);
        
    }
    
    /**
     * Create either a magic/1 or a triple/3 predicate.
     * 
     * @param magic
     * @param s
     * @param p
     * @param o
     */
    protected Pred(boolean magic, VarOrId s, VarOrId p, VarOrId o) {
        assert s != null;
        assert p != null;
        assert o != null;
        this.magic = magic;
        this.s = s;
        this.p = p;
        this.o = o;
    }

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("(");
        
        sb.append( s );
        
        sb.append(", ");

        sb.append( p );
        
        sb.append(", ");
        
        sb.append( o );
        
        sb.append(")");
        
        if(magic) {
            
            sb.append("[magic]");
            
        }
        
        return sb.toString();
        
    }
    
    public String toString(AbstractTripleStore db) {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("(");
        
        sb.append( s.toString(db) );
        
        sb.append(", ");

        sb.append( p.toString(db) );
        
        sb.append(", ");
        
        sb.append( o.toString(db) );
        
        sb.append(")");
        
        if(magic) {
            
            sb.append("[magic]");
            
        }
        
        return sb.toString();
        
    }
    
}
