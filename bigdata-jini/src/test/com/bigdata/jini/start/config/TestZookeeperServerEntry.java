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

package com.bigdata.jini.start.config;

import java.net.UnknownHostException;

import junit.framework.TestCase2;
import net.jini.config.ConfigurationException;

import com.bigdata.net.InetAddressUtil;

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

    /**
     * Test {@link ZookeeperServerEntry} parsing.
     */
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

    /**
     * Unit test for
     * {@link ZookeeperServerConfiguration#getZookeeperServerEntries(String)}
     * 
     * @throws ConfigurationException
     * @throws UnknownHostException 
     */
    public void test002() throws ConfigurationException, UnknownHostException {

        final String[] hosts = new String[] {
                "127.0.0.1",
                "localhost"
        };
        
        final String servers = "1=127.0.0.1:2888:3888, 2=localhost:2888:3888";

        final ZookeeperServerEntry[] a = ZookeeperServerConfiguration
                .getZookeeperServerEntries(servers);

        for (int i = 0; i < a.length; i++) {

            final ZookeeperServerEntry entry = a[i];

            assertEquals(i + 1, entry.id);
            assertEquals(hosts[i], entry.hostname);
            assertEquals(2888, entry.peerPort);
            assertEquals(3888, entry.leaderPort);
            
            InetAddressUtil.getByName(entry.hostname);
            
        }
        
    }

}
