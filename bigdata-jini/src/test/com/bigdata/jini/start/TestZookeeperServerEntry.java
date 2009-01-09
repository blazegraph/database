/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Jan 2, 2009
 */

package com.bigdata.jini.start;

import junit.framework.TestCase2;

import com.bigdata.jini.start.config.ZookeeperServerEntry;

/**
 * Unit tests for the {@link ZookeeperServerEntry}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZookeeperServerEntry extends TestCase2 {

    /**
     * 
     */
    public TestZookeeperServerEntry() {
    }

    /**
     * @param arg0
     */
    public TestZookeeperServerEntry(String arg0) {
        super(arg0);
    }

    public void test_correctRejection() {

        try {
            new ZookeeperServerEntry(0, null);
            fail("Expecting " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

    }

    public void test001() {

        final int expectedId = 0;
        final String expectedHostname = "192.168.1.2";
        final int expectedPeerPort = 233;
        final int expectedLeaderPort = 1992;

        ZookeeperServerEntry entry = new ZookeeperServerEntry(expectedId,
                expectedHostname + ":" + expectedPeerPort + ":"
                        + expectedLeaderPort);

        assertEquals(expectedId, entry.id);
        assertEquals(expectedHostname, entry.hostname);
        assertEquals(expectedPeerPort, entry.peerPort);
        assertEquals(expectedLeaderPort, entry.leaderPort);

        log.info(entry.toString());
        
    }

}
