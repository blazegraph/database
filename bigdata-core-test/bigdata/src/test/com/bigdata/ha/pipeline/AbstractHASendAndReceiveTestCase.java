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
package com.bigdata.ha.pipeline;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.ha.msg.HAMessageWrapper;
import com.bigdata.ha.msg.HASendState;
import com.bigdata.ha.msg.HAWriteMessageBase;
import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessageBase;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.TestCase3;

public class AbstractHASendAndReceiveTestCase extends TestCase3 {

    public AbstractHASendAndReceiveTestCase() {
    }

    public AbstractHASendAndReceiveTestCase(final String name) {
        super(name);
    }

    protected ChecksumUtility chk;

    @Override
    protected void setUp() throws Exception {

        super.setUp();

        chk = new ChecksumUtility();
    }
    
    @Override
    protected void tearDown() throws Exception {

        super.tearDown();
        
        chk = null;
        
    }
    
    protected HAMessageWrapper newHAWriteMessage(final int sze,
            final ByteBuffer tst) {

        return newHAWriteMessage(sze, chk.checksum(tst));

    }

    protected HAMessageWrapper newHAWriteMessage(final int sze, final int chksum) {

        final IHASyncRequest req = null;

        final IHASendState snd = new HASendState(messageId++, originalSenderId,
                senderId, token, replicationFactor);

        final IHAWriteMessageBase msg = new HAWriteMessageBase(sze, chksum);

        return new HAMessageWrapper(req, snd, msg);

    }

    private long messageId = 1;
    private UUID originalSenderId = UUID.randomUUID();
    private UUID senderId = UUID.randomUUID();
    private long token = 1;
    private int replicationFactor = 3;
    
}
