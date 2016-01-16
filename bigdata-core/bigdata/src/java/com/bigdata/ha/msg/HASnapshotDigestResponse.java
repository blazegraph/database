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

import com.bigdata.util.BytesUtil;

public class HASnapshotDigestResponse implements IHASnapshotDigestResponse, Serializable {

    private static final long serialVersionUID = 1L;

    private final long commitCounter;
    private final byte[] digest;
    
    public HASnapshotDigestResponse(final long commitCounter, final byte[] digest) {

        this.commitCounter = commitCounter;
        
        this.digest = digest;
        
    }
    
    @Override
    public long getCommitCounter() {
        
        return commitCounter;
        
    }

    @Override
    public byte[] getDigest() {

        return digest;
        
    }

    @Override
    public String toString() {

        return super.toString() + "{commitCounter=" + getCommitCounter()
                + ", digest=" + BytesUtil.toHexString(getDigest()) + "}";

    }

}
