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
/*
 * Created on Sep 2, 2010
 */

package com.bigdata.bop.constraint;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IConstraint;

/**
 * Abstract base class for constraint operators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BOpConstraint extends BOpBase implements IConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     * @param op
     */
    public BOpConstraint(BOpBase op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     * @param args
     * @param annotations
     */
    public BOpConstraint(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

}
