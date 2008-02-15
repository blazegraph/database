/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on May 8, 2007
 */

package com.bigdata.btree;

/**
 * An interface for a counter. Mutable btrees expose mutable counters while
 * read-only btrees do not. Like the mutable {@link BTree}, the mutable counter
 * is NOT thread-safe. An unpartitioned index exposes a single counter for the
 * entire index. A partitioned index exposes a counter per index partition and
 * the partition identified forms the high int32 for the counter.
 * 
 * @see IIndex#getCounter()
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICounter {

    /**
     * The current value of the counter (initially zero).
     */
    public long get();
    
    /**
     * Increment the current value of the counter and then return its value
     * (atomic).
     */
    public long incrementAndGet();
    
}
