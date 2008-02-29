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
 * Created on Feb 29, 2008
 */

package com.bigdata.resources;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IJoinHandler;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultJoinHandler implements IJoinHandler {

    private int minimumEntryCount;
    
    /**
     * 
     */
    private static final long serialVersionUID = -1252376380720720744L;

    /**
     * De-serialization ctor. 
     */
    public DefaultJoinHandler() {
        super();
    }

    public DefaultJoinHandler(int minimumEntryCount) {
        
        setMinimumEntryCount(minimumEntryCount);
        
    }
    
    /**
     * Return <code>true</code> iff the range count of the index is less than
     * the {@link #getMinimumEntryCount()}.
     */
    public boolean shouldJoin(IIndex ndx) {

        final long rangeCount = ndx.rangeCount(null, null);
        
        return rangeCount < getMinimumEntryCount();
        
    }

    /**
     * The minimum #of index entries before the index partition becomes eligible
     * to be joined.
     */
    public int getMinimumEntryCount() {
        return minimumEntryCount;
    }

    public void setMinimumEntryCount(int minimumEntryCount) {
        this.minimumEntryCount = minimumEntryCount;
    }

}
