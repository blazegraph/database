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

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Resolverator;

/**
 * Striterator resolver pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BOpResolver extends BOpFilterBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param op
     */
    public BOpResolver(BOpFilterBase op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public BOpResolver(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    @Override
    final protected Iterator filterOnce(Iterator src, Object context) {

        return new Resolverator(src, context, new ResolverImpl());

    }

    /**
     * Resolve the object.
     * 
     * @param obj
     *            The object.
     * @return The resolved object.
     */
    abstract protected Object resolve(Object obj);

    private class ResolverImpl extends Resolver {

        private static final long serialVersionUID = 1L;

        @Override
        protected Object resolve(Object obj) {
            return BOpResolver.this.resolve(obj);
        }

    }

}
