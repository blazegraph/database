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
 * Created on Oct 10, 2008
 */

package com.bigdata.sparse;

import com.bigdata.sparse.TPS.TPV;

/**
 * Various constants that may be used with the {@link SparseRowStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRowStoreConstants {

    /**
     * The minimum value for a timestamp. This may be used as the <i>fromTime</i>
     * to accept {@link TPV}s beginning with the earliest values on record in
     * the store.
     */
    public static final long MIN_TIMESTAMP = 1L;

    /**
     * The maximum value for a timestamp. This may be used as the <i>toTime</i>
     * to accept {@link TPV}s up to the most current on record in the store.
     */
    public static final long MAX_TIMESTAMP = Long.MAX_VALUE;

    /**
     * A special value that may be used as the <i>toTime</i> to read
     * <em>only</em> the current values for a given logical row.
     * <p>
     * Note: This value MAY NOT be specified for the <i>fromTime</i>. The
     * <i>fromTime</i> is always the first timestamp that will be accepted by a
     * read.
     * <p>
     * Normally you will specify {@link #MIN_TIMESTAMP} as the <i>fromTime</i>
     * when you specify {@link #CURRENT_ROW} as the <i>toTime</i>. This has the
     * effect of returning the most current bindings for all properties in the
     * logical row.
     * <p>
     * If you specify a different <i>fromTime</i> timestamp then any property
     * not bound since the given <i>fromTime</i> WILL NOT have a binding in the
     * returned property set. This is useful when you are storing timeseries
     * data as you can efficiently select a range of the timeseries.
     */
    public static final long CURRENT_ROW = Long.MIN_VALUE;

    /**
     * A value which indicates that the timestamp will be assigned by the server -
     * unique timestamps are NOT guarenteed with this constant. If the server
     * assigns the same timestamp to two different writes on the same property
     * values then only one of those property values will be persisted in the
     * index - the other will be <em>overwritten</em> since it will have
     * exactly the same key in the index. For this reason, you SHOULD use
     * {@link #AUTO_TIMESTAMP_UNIQUE} unless you have a compelling reason and a
     * robust design that is safe in the face of such possible overwrites.
     * 
     * @see #AUTO_TIMESTAMP_UNIQUE
     */
    public static final long AUTO_TIMESTAMP = -1L;

    /**
     * A value which indicates that a unique timestamp will be assigned by the
     * server. You should use this value rather than {@link #AUTO_TIMESTAMP}
     * unless you have a compelling design that is robust in the face of
     * possible overwrite by other callers resulting in the assignment of
     * the same timestamp to two different writes.
     * 
     * @see #AUTO_TIMESTAMP
     */
    public static final long AUTO_TIMESTAMP_UNIQUE = -2L;

}
