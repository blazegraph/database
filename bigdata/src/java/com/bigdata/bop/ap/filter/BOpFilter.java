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
import com.bigdata.btree.ITupleIterator;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Filterator;

/**
 * Used to filter for objects which satisfy some criteria.
 * <p>
 * <strong>WARNING : DO NOT USE THIS CLASS ON {@link ITupleIterator}s - use
 * {@link BOpTupleFilter} instead.</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BOpFilter extends BOpFilterBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

//    /**
//     * Deserialization.
//     */
//    public BOpFilter() {
//        super();
//    }
    
    /**
     * Deep copy.
     * 
     * @param op
     */
    public BOpFilter(BOpFilter op) {
        super(op);
    }

    /**
     * Shallow copy.
     * 
     * @param args
     * @param annotations
     */
    public BOpFilter(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    @Override
    final protected Iterator filterOnce(Iterator src, final Object context) {
        
        return new Filterator(src, context, new FilterImpl());
        
    }

    /**
     * Return <code>true</code> iff the object should be accepted.
     * 
     * @param obj
     *            The object.
     */
    protected abstract boolean isValid(Object obj);

    private class FilterImpl extends Filter {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isValid(Object obj) {
            return BOpFilter.this.isValid(obj);
        }

    }

}
