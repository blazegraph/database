/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * Use inline numerical comparison techniques to implement the == operator.
 */
public class InlineEQ extends AbstractInlineConstraint {

    private static final long serialVersionUID = -859713006378534024L;

    /**
     * Required shallow copy constructor.
     */
    public InlineEQ(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public InlineEQ(final InlineEQ op) {
        super(op);
    }

    public InlineEQ(final IVariable<IV> v, final IV iv) {
        
        super(v, iv);
        
    }
    
    protected boolean _accept(final int compare) {

        return compare == 0;

    }
    
}
