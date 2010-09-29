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
import com.bigdata.bop.BOpBase;

import cutthecrap.utils.striterators.IFilter;

/**
 * Base class for operators which apply striterator patterns for access paths.
 * <p>
 * The striterator pattern is enacted slightly differently here. The filter
 * chain is formed by stacking {@link BOpFilterBase}s as child operands. Each
 * operand specified to a {@link BOpFilterBase} must be a {@link BOpFilterBase} and
 * layers on another filter, so you can stack filters as nested operands or as a
 * sequence of operands.
 * <p>
 * 
 * @todo state as newState() and/or as annotation?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BOpFilterBase extends BOpBase implements IFilter {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations {

    }

    /**
     * Deep copy.
     * @param op
     */
    public BOpFilterBase(BOpFilterBase op) {
        super(op);
    }

    /**
     * Shallow copy.
     * @param args
     * @param annotations
     */
    public BOpFilterBase(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    final public Iterator filter(Iterator src, final Object context) {

        // wrap source with each additional filter from the filter chain.
        for (BOp arg : args()) {
        
            src = ((BOpFilterBase) arg).filter(src, context);
            
        }

        // wrap src with _this_ filter.
        src = filterOnce(src, context);
        
        return src;
        
    }

    /**
     * Wrap the source iterator with <i>this</i> filter.
     * 
     * @param src
     *            The source iterator.
     * @param context
     *            The iterator evaluation context.
     * 
     * @return The wrapped iterator.
     */
    abstract protected Iterator filterOnce(Iterator src, final Object context);

}
