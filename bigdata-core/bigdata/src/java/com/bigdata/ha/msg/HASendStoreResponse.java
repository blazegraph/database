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

import java.nio.ByteBuffer;

import com.bigdata.io.ChecksumUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.util.BytesUtil;

public class HASendStoreResponse implements IHASendStoreResponse {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final private byte[] rootBlock0;
    final private byte[] rootBlock1;
    final long byteCount;
    final long blockCount;

    public HASendStoreResponse(final IRootBlockView rootBlock0,
            final IRootBlockView rootBlock1, final long byteCount,
            final long blockCount) {

        if (rootBlock0 == null)
            throw new IllegalArgumentException();

        if (rootBlock1 == null)
            throw new IllegalArgumentException();

        this.rootBlock0 = BytesUtil.toArray(rootBlock0.asReadOnlyBuffer());

        this.rootBlock1 = BytesUtil.toArray(rootBlock1.asReadOnlyBuffer());

        this.byteCount = byteCount;

        this.blockCount = blockCount;

    }

    @Override
    public IRootBlockView getRootBlock0() {

        return new RootBlockView(true, ByteBuffer.wrap(rootBlock0),
                new ChecksumUtility());

    }

    @Override
    public IRootBlockView getRootBlock1() {

        return new RootBlockView(false/* rootBlock0 */,
                ByteBuffer.wrap(rootBlock1), new ChecksumUtility());

    }

    @Override
    public long getByteCount() {

        return byteCount;
        
    }
    
    @Override
    public long getBlockCount() {
        
        return blockCount;
        
    }
    
    public String toString() {

        return getClass() + "{rootBlock0=" + getRootBlock0() + ", rootBlock1="
                + getRootBlock1() + ", bytesSent=" + getByteCount()
                + ", blocksSent=" + getBlockCount() + "}";

    }

}
