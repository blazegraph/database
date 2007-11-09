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

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.inf.Rule.Var;

/**
 * Rejects (x y z) iff x==z and y==owl:sameAs, where x, y, and z are variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RejectAnythingSameAsItself implements IConstraint {

    private final Var s;
    private final Var p;
    private final Var o;
    private final long owlSameAs;
    
    /**
     * 
     * @param s
     * @param p
     * @param o
     * @param owlSameAs
     */
    public RejectAnythingSameAsItself(Var s, Var p, Var o, Id owlSameAs) {
        
        if (s == null || p == null || o == null || owlSameAs == null)
            throw new IllegalArgumentException();

        if (s == p || p == o || s == o)
            throw new IllegalArgumentException();
        
        this.s = s;

        this.p = p;

        this.o = o;
        
        this.owlSameAs = owlSameAs.id;
        
    }
    
    public boolean accept(State state) {
        
        // get binding for "x".
        long s = state.get(this.s);
       
        if(s==NULL) return true; // not yet bound.

        // get binding for "y".
        long p = state.get(this.p);
    
        if(p==NULL) return true; // not yet bound.
    
        // get binding for "z".
        long o = state.get(this.o);
    
        if(o==NULL) return true; // not yet bound.
    
        if (s == o && p == owlSameAs) {

            // reject this case.
            
            return false;
            
        }

        return true;
        
   }

}
