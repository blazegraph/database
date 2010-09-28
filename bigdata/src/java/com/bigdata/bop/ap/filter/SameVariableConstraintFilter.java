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
 * Created on Sep 28, 2010
 */

package com.bigdata.bop.ap.filter;

import java.util.Iterator;
import java.util.Map;

import com.bigdata.bop.BOp;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Filterator;

/**
 * Operator imposing a {@link SameVariableConstraint} filter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SameVariableConstraintFilter extends BOpFilterBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOpFilterBase.Annotations {

        /**
         * The constraint to be imposed (required).
         */
        String FILTER = SameVariableConstraintFilter.class.getName()
                + ".filter";

    }

    /**
     * @param op
     */
    public SameVariableConstraintFilter(SameVariableConstraintFilter op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public SameVariableConstraintFilter(BOp[] args,
            Map<String, Object> annotations) {
        super(args, annotations);
    }

    @Override
    final protected Iterator filterOnce(Iterator src, final Object context) {

        final SameVariableConstraint filter = (SameVariableConstraint) getRequiredProperty(Annotations.FILTER);

        return new Filterator(src, context, new FilterImpl(filter));

    }

    static private class FilterImpl extends Filter {

        private static final long serialVersionUID = 1L;

        private final SameVariableConstraint filter;

        public FilterImpl(final SameVariableConstraint filter) {
            this.filter = filter;
        }

        @Override
        protected boolean isValid(Object obj) {

            return filter.accept(obj);

        }

    }

}
