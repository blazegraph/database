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
 * Created on May 11, 2010
 */

package com.bigdata.counters;

import java.util.concurrent.atomic.AtomicLong;

import org.cliffc.high_scale_lib.ConcurrentAutoTable;

/**
 * An alias for the high-scale-lib counter implementation. {@link CAT}s are
 * useful when a counter is extremely hot for updates and adaptively expand
 * their internal state to minimize thread contention. However, they can do more
 * work than an {@link AtomicLong} on read since they must scan an internal
 * table to aggregate updates from various threads.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CAT extends ConcurrentAutoTable {

}
