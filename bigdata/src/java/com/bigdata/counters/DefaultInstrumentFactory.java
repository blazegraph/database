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
 * Created on Apr 20, 2008
 */

package com.bigdata.counters;

import com.bigdata.counters.ICounterSet.IInstrumentFactory;

/**
 * Used to read in {@link CounterSet} XML, aggregating data into
 * {@link HistoryInstrument}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultInstrumentFactory implements IInstrumentFactory {

    /**
     * Instance supports overwrite, so the ring buffer will eventually overwrite
     * old values as new values arrive. Minutes are migrated onto hours and
     * hours are migrated onto days. Up to 30 days of data will be retained.
     */
    public static final DefaultInstrumentFactory OVERWRITE_60M = new DefaultInstrumentFactory(
            60/* slots */, PeriodEnum.Minutes, true/* overwrite */);

    public static final DefaultInstrumentFactory NO_OVERWRITE_60M = new DefaultInstrumentFactory(
            60/* slots */, PeriodEnum.Minutes, false/* overwrite */);

    private final int nslots;

    private final PeriodEnum period;

    private final boolean overwrite;

    /**
     * 
     * @param nslots
     *            The #of units of the period to be retained by the
     *            {@link History}.
     * @param period
     *            The period in which those units are measured.
     * @param overwrite
     *            If the {@link History} may serve as a ring buffer.
     */
    public DefaultInstrumentFactory(final int nslots, final PeriodEnum period,
            final boolean overwrite) {

        if (nslots <= 0)
            throw new IllegalArgumentException();

        if (period == null)
            throw new IllegalArgumentException();

        this.nslots = nslots;

        this.period = period;

        this.overwrite = overwrite;

    }

    public IInstrument newInstance(final Class type) {

        if (type == null)
            throw new IllegalArgumentException();

        if (type == Double.class || type == Float.class) {

            final History<Double> minutes = new History<Double>(
                    new Double[nslots], period.getPeriodMillis(), overwrite);

            if (overwrite) {

                // 24 hours in a day
                final History<Double> hours = new History<Double>(24, minutes);

                // 30 days of history
//                final History<Double> days = 
                new History<Double>(30, hours);

            }

            return new HistoryInstrument<Double>(minutes);

        } else if (type == Long.class || type == Integer.class) {

            final History<Long> minutes = new History<Long>(
                    new Long[nslots], period.getPeriodMillis(), overwrite);

            if (overwrite) {

                // 24 hours in a day
                final History<Long> hours = new History<Long>(24, minutes);

                // 30 days of history
//                final History<Long> days = 
                new History<Long>(30, hours);

            }

            return new HistoryInstrument<Long>(minutes);

        } else if (type == String.class) {

            return new StringInstrument();

        } else {

            throw new UnsupportedOperationException("type: " + type);

        }
        
    }

    static class StringInstrument extends Instrument<String> {
      
        public void sample() {}
        
    };
    
}
