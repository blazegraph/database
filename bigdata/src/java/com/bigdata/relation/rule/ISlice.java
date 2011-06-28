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
 * Created on Sep 24, 2008
 */

package com.bigdata.relation.rule;

import java.io.Serializable;

import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Indicates the first solution to be returned to the caller (offset) and the
 * #of solutions to be returned (limit).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISlice extends Serializable {

    /**
     * The first solution to be returned to the caller. A value of ZERO (0)
     * indicates that all solutions should be returned.
     */
    public long getOffset();
    
    /**
     * The maximum #of solutions to be returned to the caller. A value of
     * {@link Long#MAX_VALUE} indicates that there is no limit.
     * 
     * @todo modify to be consistent with
     *       {@link IAccessPath#iterator(long, long, int)} where a limit of ZERO
     *       (0L) is interpreted as NO limit and a limit of
     *       {@link Long#MAX_VALUE} is interpreted as ZERO (0L) (that is, also
     *       no limit).
     */
    public long getLimit();

    /**
     * The index of the last solution that we will generate (OFFSET + LIMIT). If
     * OFFSET + LIMIT would be greater than {@link Long#MAX_VALUE}, then use
     * {@link Long#MAX_VALUE} instead.
     */
    public long getLast();
    
}
