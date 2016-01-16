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
/*
 * Created on Feb 12, 2012
 */

package com.bigdata.bop.engine;

import java.util.UUID;

import com.bigdata.io.SerializerUtil;

import junit.framework.TestCase2;

/**
 * Test suite for {@link StartOpMessage}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStartOpMessage extends TestCase2 {

    /**
     * 
     */
    public TestStartOpMessage() {
    }

    /**
     * @param name
     */
    public TestStartOpMessage(String name) {
        super(name);
    }

    public void test_serialization() {
        
        final IStartOpMessage expected = new StartOpMessage(
                UUID.randomUUID()/* queryId */, 12/* opId */,
                8/* partitionId */, UUID.randomUUID()/* serviceId */, 7/* nmessages */);
        
        doSerializationTest(expected);
        
    }
    
    private static void doSerializationTest(final IStartOpMessage expected) {

        final IStartOpMessage actual = (IStartOpMessage) SerializerUtil
                .deserialize(SerializerUtil.serialize(expected));

        assertSameOp(expected, actual);
        
    }

    private static void assertSameOp(final IStartOpMessage expected,
            final IStartOpMessage actual) {

        assertEquals("queryId", expected.getQueryId(), actual.getQueryId());
        
        assertEquals("serviceId", expected.getServiceId(),
                actual.getServiceId());
        
        assertEquals("bopId", expected.getBOpId(), actual.getBOpId());
        
        assertEquals("partitionId", expected.getPartitionId(),
                actual.getPartitionId());
        
        assertEquals("messageReadyCount", expected.getChunkMessageCount(),
                actual.getChunkMessageCount());

    }

}
