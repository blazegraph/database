/*

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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

import java.util.Map;

import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;

/**
 * Imposes the constraint <code>x AND y</code>.
 */
public class AND extends BOpConstraint {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8146965892831895463L;

	/**
     * Required deep copy constructor.
     */
    public AND(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public AND(final AND op) {
        super(op);
    }

    public AND(final IConstraint x, final IConstraint y) {

        super(new BOp[] { x, y }, null/*annocations*/);

        if (x == null || y == null)
            throw new IllegalArgumentException();

    }

    public boolean accept(final IBindingSet s) {

        return ((IConstraint) get(0)).accept(s)
                && ((IConstraint) get(1)).accept(s);

    }
    
}
