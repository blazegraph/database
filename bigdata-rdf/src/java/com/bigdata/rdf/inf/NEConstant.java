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
 * Imposes the constraint <code>var != constant</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NEConstant implements IConstraint {
    
    private final Var var;
    private final long constant;

    public NEConstant(Var var, long constant) {
        
        this.var = var;
        
        this.constant = constant;
        
    }
    
    public boolean accept(State s) {
        
        // get binding for the variable.
        long var = s.get(this.var);
    
        if(var==NULL) return true; // not yet bound.
    
        return var != constant; 

   }

}