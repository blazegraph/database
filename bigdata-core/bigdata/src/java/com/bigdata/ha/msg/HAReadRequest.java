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

import com.bigdata.rawstore.IRawStore;

public class HAReadRequest implements IHAReadRequest,
        Serializable {

    private static final long serialVersionUID = 1L;

    private final long quorumToken;
    private final UUID storeUUID;
    private final long addr;

    public HAReadRequest(final long quorumToken, final UUID storeUUID, final long addr) {

        // Note: Optional.
//        if (storeUUID == null)
//            throw new IllegalArgumentException();
        
        if (addr == IRawStore.NULL)
            throw new IllegalArgumentException();
        
        this.quorumToken = quorumToken;
        this.storeUUID = storeUUID;
        this.addr = addr;
        
    }

    @Override
    public long getQuorumToken() {

        return quorumToken;
        
    }

    @Override
    public UUID getStoreUUID() {

        return storeUUID;
        
    }

    @Override
    public long getAddr() {

        return addr;
        
    }

}
