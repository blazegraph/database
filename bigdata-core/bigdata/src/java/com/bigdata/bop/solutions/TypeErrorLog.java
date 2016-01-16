/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Nov 17, 2011
 */

package com.bigdata.bop.solutions;

import org.apache.log4j.Logger;

import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.engine.BOpStats;

/**
 * A utility class for logging type errors.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TypeErrorLog {

    private final static transient Logger log = Logger
            .getLogger(TypeErrorLog.class);

    /**
     * Logs a type error.
     * 
     * @param t
     *            The cause.
     * @param expr
     *            The expression which resulted in a type error.
     * @param stats
     *            Used to report the type errors as a side-effect on
     *            {@link BOpStats#typeErrors}.
     */
    static public void handleTypeError(
            // final Logger log,
            final Throwable t,//
            final IValueExpression<?> expr,//
            final BOpStats stats//
            ) {

        stats.typeErrors.increment();
        
        if (log.isInfoEnabled())
            log.info("Type error: expr=" + expr + ", cause=" + t);

    }

}
