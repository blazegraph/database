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
 * Created on Apr 6, 2009
 */

package com.bigdata.counters.query;

import java.util.regex.Pattern;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.PeriodEnum;

/**
 * Interface for selecting counters.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICounterSelector {

    /**
     * Selects and returns a collection of performance counters with history.
     * 
     * @param depth
     *            When non-ZERO, this specifies a constraint on the maximum
     *            depth for a counter may can be selected.
     * @param pattern
     *            A regular expression which must be satisified (optional).
     * @param fromTime
     *            The inclusive lower bound in milliseconds of the performance
     *            counter timestamps which will be selected.
     * @param toTime
     *            The exclusive upper bound in milliseconds of the performance
     *            counter timestamps which will be selected.
     * @param period
     *            The unit of aggregation for the selected performance counters.
     * 
     * @return The selected performance counters.
     */
    ICounter[] selectCounters(int depth, Pattern pattern, long fromTime,
            long toTime, PeriodEnum period);

}
