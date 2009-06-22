/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 22, 2009
 */

package com.bigdata.relation.rule.eval;

import org.apache.log4j.Logger;

/**
 * Class defines the log on which rule execution statistics are written. This
 * covers both query evaluation and mutation operations such as fixed point
 * closure of a rule set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleLog {

    protected static final transient Logger log = Logger
            .getLogger(RuleLog.class);

    /**
     * Log rule execution statistics.
     * 
     * @param stats
     *            The rule execution statistics.
     */
    static public void log(final RuleStats stats) {

        if (log.isInfoEnabled()) {

            log.info(stats.toString());

        }

    }

}
