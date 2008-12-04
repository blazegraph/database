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
 * Created on Dec 4, 2008
 */

package com.bigdata.cache;

/**
 * Interface for values that can self-report the last timestamp when they were
 * accessed. This interface may be used to automatically clear values from a
 * {@link HardReferenceQueue} that have not been touched within some timeout.
 * This can be used to automatically make resources in a weak value cache
 * available for garbage collection once they exceed a certain age by simply
 * clearing their reference in the {@link HardReferenceQueue}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IValueAge {

    /**
     * Invoked when a value is touched. The value must reset the timestamp that
     * it will report. That timestamp MUST be obtained using
     * {@link System#nanoTime()} as the age of the value will be judged by
     * comparison to the current value reported by {@link System#nanoTime()}.
     */
    public void touch();
    
    /**
     * Report the timestamp associated with the value. That timestamp MUST be
     * the value of {@link System#nanoTime()} when {@link #touch()} was last
     * invoked.
     */
    public long timestamp();
    
}
