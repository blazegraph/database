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
package com.bigdata.bop.constraint;

import com.bigdata.bop.AbstractBOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;

/**
 * Imposes the constraint <code>x OR y</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OR extends AbstractBOp implements IConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

//    /**
//     * 
//     */
//    private static final long serialVersionUID = 7750833040265209718L;
//    
//    private final IConstraint x;
//    private final IConstraint y;
    
    public OR(final IConstraint x, final IConstraint y) {

        super(new BOp[]{x,y});
        
        if (x == null || y == null)
            throw new IllegalArgumentException();

//        this.x = x;
//        
//        this.y = y;
        
    }
    
    public boolean accept(final IBindingSet s) {

        return ((IConstraint) args[0]).accept(s)
                || ((IConstraint) args[1]).accept(s);

   }

//    public IVariable[] getVariables() {
//        
//        IVariable[] x = this.x.getVariables();
//        IVariable[] y = this.y.getVariables();
//        IVariable[] xy = new IVariable[x.length + y.length];
//        System.arraycopy(x, 0, xy, 0, x.length);
//        System.arraycopy(y, 0, xy, x.length, y.length);
//        return xy;
//        
//    }
    
}
