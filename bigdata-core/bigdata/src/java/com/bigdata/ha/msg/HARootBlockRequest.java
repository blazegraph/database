/**

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
package com.bigdata.ha.msg;

import java.io.Serializable;
import java.util.UUID;

public class HARootBlockRequest implements IHARootBlockRequest,
        Serializable {

    private static final long serialVersionUID = 1L;

    private final UUID storeUUID;
    private final boolean isNonBlocking;

    /**
     * Create a non-blocking request (this is the historical behavior).
     * 
     * @param storeUUID
     *            The store UUID (optional).
     */
    public HARootBlockRequest(final UUID storeUUID) {

        this(storeUUID, true/* isNonBlocking */);

    }
 
    /**
     * 
     * @param storeUUID
     *            The store UUID (optional).
     * @param isNonBlocking
     *            <code>true</code> iff the request should be non-blocking.
     */
    public HARootBlockRequest(final UUID storeUUID,final boolean isNonBlocking) {

        // Note: Optional.
        this.storeUUID = storeUUID;
        
        this.isNonBlocking = isNonBlocking;

    }

    @Override
    public UUID getStoreUUID() {

        return storeUUID;

    }

    @Override
    public boolean isNonBlocking() {

        return isNonBlocking;
        
    }
    
}
