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
 * Test suite for {@link HaltOpMessage}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHaltOpMessage extends TestCase2 {

    /**
     * 
     */
    public TestHaltOpMessage() {
    }

    /**
     * @param name
     */
    public TestHaltOpMessage(String name) {
        super(name);
    }

    public void test_serialization() {
        
        final IHaltOpMessage expected = new HaltOpMessage(
                UUID.randomUUID()/* queryId */, 12/* opId */,
                8/* partitionId */, UUID.randomUUID()/* serviceId */, //
                null,// cause
                5,//sinkMessagesOut
                3,//altSinkMessagesOut
                new BOpStats()// stats
                );
        
        doSerializationTest(expected);
        
    }

    public void test_serialization_throwable() {
        
        final IHaltOpMessage expected = new HaltOpMessage(
                UUID.randomUUID()/* queryId */, 12/* opId */,
                8/* partitionId */, UUID.randomUUID()/* serviceId */, //
                new RuntimeException(),// cause
                5,//sinkMessagesOut
                3,//altSinkMessagesOut
                new BOpStats()// stats
                );
        
        doSerializationTest(expected);
        
    }

    private static void doSerializationTest(final IHaltOpMessage expected) {

        final IHaltOpMessage actual = (IHaltOpMessage) SerializerUtil
                .deserialize(SerializerUtil.serialize(expected));

        assertSameOp(expected, actual);
        
    }

    private static void assertSameOp(final IHaltOpMessage expected,
            final IHaltOpMessage actual) {

        assertEquals("queryId", expected.getQueryId(), actual.getQueryId());
        
        assertEquals("serviceId", expected.getServiceId(),
                actual.getServiceId());
        
        assertEquals("bopId", expected.getBOpId(), actual.getBOpId());
        
        assertEquals("partitionId", expected.getPartitionId(),
                actual.getPartitionId());

        /*
         * Note: Exceptions do not implement equals() so this will fail.
         */
//        assertEquals("cause", expected.getCause(), actual.getCause());

        assertEquals("sinkMessagesOut", expected.getSinkMessagesOut(),
                actual.getSinkMessagesOut());

        assertEquals("altSinkMessagesOut", expected.getAltSinkMessagesOut(),
                actual.getAltSinkMessagesOut());
        
        /*
         * Note: BOpStats does not implement equals() so this will fail.
         */
//        assertEquals("stats", expected.getStats(), actual.getStats());
        
    }

}
