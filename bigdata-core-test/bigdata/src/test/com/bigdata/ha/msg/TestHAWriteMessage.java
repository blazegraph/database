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

import junit.framework.TestCase;

import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.util.BytesUtil;

public class TestHAWriteMessage extends TestCase {

	/**
	 * Simple test to verify HAWriteMessage serialization
	 */
    public void testSerialization() throws IOException, ClassNotFoundException {

        final IHAWriteMessage msg1 = new HAWriteMessage(
                UUID.randomUUID(),// store UUID
                12L,// commitCounter
                13L,// commitTime
                14L,// sequence
                15,// size
                16,// checksum
                StoreTypeEnum.RW,//
                17L,// quorumToken
                18L,// fileExtent
                19L // firstOffset
                );

        final byte[] ser1 = serialized(msg1);

        final IHAWriteMessage msg2 = (IHAWriteMessage) SerializerUtil
                .deserialize(ser1);

        assertTrue(msg1.equals(msg2));

        // now confirm serialized byte equivalence in case we just messed up
        // equals
        final byte[] ser2 = serialized(msg2);

        assertTrue(BytesUtil.bytesEqual(ser1, ser2));

//        System.err.println("msg1: " + msg1);
//        System.err.println("msg2: " + msg2);

    }
	
	/**
	 * Utility to return byte[] serialization of the HAWriteMessage
	 */
    private byte[] serialized(final IHAWriteMessage msg) {

	    return SerializerUtil.serialize(msg);
	}

}
