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

import java.io.IOException;
import java.util.UUID;

import com.bigdata.io.SerializerUtil;

import junit.framework.TestCase2;

public class TestHASendState extends TestCase2 {

    public TestHASendState() {
    }

    public TestHASendState(String name) {
        super(name);
    }

    public void test_roundTrip() throws IOException {
        
        final long messageId = 5;
        final UUID originalSenderUUID = UUID.randomUUID();
        final UUID senderId = UUID.randomUUID();
        final long quorumToken = 12;
        final int replicationFactor = 3;

        final HASendState expected = new HASendState(messageId,
                originalSenderUUID, senderId, quorumToken, replicationFactor);
        
        final byte[] b = SerializerUtil.serialize(expected);
        
        final HASendState actual = (HASendState) SerializerUtil.deserialize(b);
        
        assertEquals(expected, actual);

    }

    public void test_getMarker_decode() throws IOException {

        final long messageId = 5;
        final UUID originalSenderUUID = UUID.randomUUID();
        final UUID senderId = UUID.randomUUID();
        final long quorumToken = 12;
        final int replicationFactor = 3;

        final HASendState expected = new HASendState(messageId,
                originalSenderUUID, senderId, quorumToken, replicationFactor);

        final byte[] b = expected.getMarker();

        final HASendState actual = (HASendState) HASendState.decode(b);

        assertEquals(expected, actual);

    }

}
