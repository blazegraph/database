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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop;

import java.util.Map;

/**
 * Operator quotes its operand (in the Lisp sense) and returns its operand when
 * it is evaluated. The purpose of quoting another operator is to prevent its
 * direct evaluation. This is typically done when the child operand will be
 * interpreted, rather than evaluated, within some context.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo I think that we can avoid quoting operators by using annotations (for
 *       some cases) and through explicit interaction between operators for
 *       others (such as between a join and a predicate). If that proves to be
 *       true then this class will be dropped.
 */
public class QuoteOp extends BOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public QuoteOp(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public QuoteOp(final QuoteOp op) {
        super(op);
    }
    
    /**
     * Quote an operator.
     * 
     * @param op
     *            The quoted operand.
     */
    public QuoteOp(final BOp op) {

        super(new BOp[] { op });

        if (op == null)
            throw new IllegalArgumentException();
        
    }

}
