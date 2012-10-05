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
package com.bigdata.ha.msg;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.BytesUtil;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.util.ChecksumUtility;

public class HA2PhasePrepareMessage implements IHA2PhasePrepareMessage, Serializable {

    private static final long serialVersionUID = 1L;
    
    private final boolean isRootBlock0;
    private final byte[] rootBlock;
    private final long timeout;
    private final TimeUnit unit;

    public HA2PhasePrepareMessage(final IRootBlockView rootBlock,
            final long timeout, final TimeUnit unit) {
        
        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (timeout < 0L)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        this.isRootBlock0 = rootBlock.isRootBlock0();
        
        /*
         * Convert to a byte[].
         * 
         * Note: This does NOT preserve the isRootBlock0 flag!
         */
        this.rootBlock = BytesUtil.getBytes(rootBlock.asReadOnlyBuffer());
        
        this.timeout = timeout;
        
        this.unit = unit;
        
    }

    @Override
    public boolean isRootBlock0() {
        return isRootBlock0;
    }

    @Override
    public IRootBlockView getRootBlock() {

        return new RootBlockView(isRootBlock0, ByteBuffer.wrap(rootBlock),
                new ChecksumUtility());

    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public TimeUnit getUnit() {
        return unit;
    }

}
