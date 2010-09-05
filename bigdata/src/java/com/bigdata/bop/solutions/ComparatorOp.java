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
 * Created on Sep 4, 2010
 */

package com.bigdata.bop.solutions;

import java.util.Comparator;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;

/**
 * Base class for operators which impose a sort order on binding sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ComparatorOp extends BOpBase implements
        Comparator<IBindingSet> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations {

        /**
         * An {@link ISortOrder}[] specifying the variables on which the sort
         * will be imposed and the order (ascending or descending) for each
         * variable.
         */
        String ORDER = ComparatorOp.class.getName() + ".order";

    }

    /**
     * @param op
     */
    public ComparatorOp(BOpBase op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public ComparatorOp(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

}
