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
 * Created on Aug 17, 2010
 */

package com.bigdata.bop;

import java.util.Arrays;
import java.util.Iterator;


import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Operator utility class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BOpUtility {

    /**
     * Return all variables recursively present whether in the operator tree or
     * on {@link IConstraint}s attached to operators.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<IVariable<?>> getSpannedVariables(final BOp op) {

        return new Striterator(getArgumentVariables(op)).append(new Striterator(op
                .annotations().values().iterator()).addFilter(new Filter() {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean isValid(Object arg0) {
                return arg0 instanceof BOp;
            }
        }));

    }

    /**
     * Return the variables from the operator's arguments.
     * 
     * @param op
     *            The operator.
     *            
     * @return An iterator visiting its {@link IVariable} arguments.
     */
    @SuppressWarnings("unchecked")
    static public Iterator<IVariable<?>> getArgumentVariables(final BOp op) {

        return new Striterator(Arrays.asList(op.args()).iterator())
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean isValid(final Object arg0) {
                        return arg0 instanceof IVariable<?>;
                    }
                });

    }

    /**
     * The #of arguments to this operation which are variables. This method does
     * not report on variables in child nodes nor on variables in attached
     * {@link IConstraint}, etc.
     */
    static public int getArgumentVariableCount(final BOp op) {
        int nvars = 0;
        final Iterator<BOp> itr = op.args().iterator();
        while(itr.hasNext()) {
            final BOp arg = itr.next();
            if (arg instanceof IVariable<?>)
                nvars++;
        }
        return nvars;
    }
    
}
