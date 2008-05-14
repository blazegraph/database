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
 * Created on Apr 18, 2008
 */

package com.bigdata.counters;

/**
 * A view of a sample period for some {@link History}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 */
public interface IHistoryEntry<T> {
    
    /**
     * The timestamp of the last sample to fall into this period. This timestamp
     * will always be GTE {@link #getStartOfSamplePeriod()} and LE
     * {@link #getEndOfSamplePeriod()}.
     */
    public long lastModified();

//    /**
//     * The earliest possible timestamp for which a sample would be reported
//     * within this {@link IHistoryEntry} (inclusive lower bound).
//     */
//    public long getStartOfSamplePeriod();
//    
//    /**
//     * The last possible timestamp for which a sample would be reported within
//     * this {@link IHistoryEntry} (exclusive upper bound).
//     */
//    public long getEndOfSamplePeriod();
    
    /**
     * The average of the samples in this period.
     */
    public T getValue();

    /**
     * The #of samples in this period.
     */
    public int getCount();

    /**
     * The total of the samples in this period.
     */
    public T getTotal();
    
}