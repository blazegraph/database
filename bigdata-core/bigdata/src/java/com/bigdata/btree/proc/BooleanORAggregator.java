/*

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
 * Created on Apr 9, 2009
 */

package com.bigdata.btree.proc;

import com.bigdata.service.Split;

/**
 * Combines together boolean values using a logical <code>OR</code>. The
 * {@link #getResult() result} will be <code>true</code> if any of the
 * component results was <code>true</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BooleanORAggregator implements IResultHandler<Boolean, Boolean> {

    /**
     * Note: The public methods are synchronized so that changes to this field
     * state will be visible.
     */
    private boolean flag;

    public BooleanORAggregator() {
        
        flag = false;
        
    }
    
	@Override
	synchronized public void aggregate(final Boolean result, final Split split) {

        flag |= result.booleanValue();

    }

	@Override
    synchronized public Boolean getResult() {

        return flag;

    }

}
