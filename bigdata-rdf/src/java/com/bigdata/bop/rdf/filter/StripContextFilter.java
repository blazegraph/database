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

package com.bigdata.bop.rdf.filter;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.ap.filter.BOpResolver;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;

/**
 * Strips the context information from an {@link SPO}. This is used in default
 * graph access paths. It operators on {@link ISPO}s so it must be applied using
 * {@link IPredicate.Annotations#ACCESS_PATH_FILTER}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StripContextFilter extends BOpResolver {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 
     * A global instance.
     */
    public static final transient StripContextFilter INSTANCE = new StripContextFilter(
            BOpBase.NOARGS, BOpBase.NOANNS);

    /**
     * @param op
     */
    public StripContextFilter(StripContextFilter op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public StripContextFilter(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Strips the context position off of a visited {@link ISPO}.
     */
    @Override
    protected Object resolve(final Object obj) {

        final ISPO tmp = (ISPO) obj;

        return new SPO(tmp.s(), tmp.p(), tmp.o());

    }

}
