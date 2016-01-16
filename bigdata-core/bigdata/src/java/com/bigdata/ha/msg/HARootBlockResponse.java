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
import java.nio.ByteBuffer;

import com.bigdata.io.ChecksumUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.util.BytesUtil;

public class HARootBlockResponse implements IHARootBlockResponse, Serializable {

    private static final long serialVersionUID = 1L;

    final private boolean isRootBlock0;
    final private byte[] data;
    
    public HARootBlockResponse(final IRootBlockView rootBlock) {
        
        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        this.isRootBlock0 = rootBlock.isRootBlock0();
        
        this.data = BytesUtil.toArray(rootBlock.asReadOnlyBuffer());
        
    }
    
    @Override
    public IRootBlockView getRootBlock() {

        return new RootBlockView(isRootBlock0, ByteBuffer.wrap(data),
                new ChecksumUtility());

    }

}
