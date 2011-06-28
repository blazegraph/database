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
 * Created on Jan 7, 2009
 */

package com.bigdata.zookeeper;

import java.util.UUID;

import junit.framework.AssertionFailedError;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Test suite for ephemeral znode semantics of children. The basic questions
 * deal with how to maintain order an uniqueness for the ephemeral znodes
 * created for an application process using a {@link ZooKeeper} connection. Note
 * that the {@link ZooKeeper} connection object and its ephemeral znodes are
 * intimately linked. When the {@link ZooKeeper} connection is lost, all
 * ephemeral znodes for that connection are destroyed.
 * <p>
 * Children are inherently unordered in zookeeper. In order to have an order
 * over the children, the {@link CreateMode#EPHEMERAL_SEQUENTIAL} mode must be
 * used. This test suite answers the question whether a {@link ZooKeeper}
 * connection can have more than one direct child for the same znode with (a)
 * the {@link CreateMode#EPHEMERAL} mode; and/or (b) the
 * {@link CreateMode#EPHEMERAL_SEQUENTIAL} mode.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEphemeralSemantics extends AbstractZooTestCase {

    /**
     * 
     */
    public TestEphemeralSemantics() {
    }

    /**
     * @param name
     */
    public TestEphemeralSemantics(String name) {
        super(name);
    }

    /**
     * Can a node have two EPHEMERAL children for the same {@link ZooKeeper}
     * connection (no).
     */
    public void test_EPHEMERAL() throws KeeperException, InterruptedException {

        // a node that is guaranteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        // Create that znode. it will be the parent for this experiment.
        zookeeper.create(zpath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // verify znode was created.
        assertNotNull(zookeeper.exists(zpath, false));

        // create an EPHEMERAL child.
        final String zchild = zookeeper.create(zpath + "/foo", new byte[0],
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // verify znode was created.
        assertNotNull(zookeeper.exists(zchild, false));

        try {
            // re-create an EPHEMERAL child.
            zookeeper.create(zchild, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            // this should not be allowed.
            fail("Recreate of EPHEMERAL znode was allowed");
        } catch (NodeExistsException e) {
            // ignore
        }

    }

    /**
     * Can a node have two {@link CreateMode#EPHEMERAL_SEQUENTIAL} children for
     * the same {@link ZooKeeper} connection (yes).
     * 
     * @todo unit test verifying that a mixture of EPHERMERAL (at most one) and
     *       EPHEMERAL_SEQUENTIAL (zero or more) are allowed.
     */
    public void test_EPHEMERAL_SEQUENTIAL() throws KeeperException,
            InterruptedException {

        // a node that is guaranteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        // Create that znode. it will be the parent for this experiment.
        zookeeper.create(zpath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // verify znode was created.
        assertNotNull(zookeeper.exists(zpath, false));

        // create an EPHEMERAL_SEQUENTIAL child.
        final String zchild1 = zookeeper.create(zpath + "/foo", new byte[0],
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // verify znode was created.
        assertNotNull(zookeeper.exists(zchild1, false));

        final String zchild2;
        try {
            // create another EPHEMERAL_SEQUENTIAL child.
            zchild2 = zookeeper.create(zpath + "/foo", new byte[0],
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (NodeExistsException e) {
            throw new AssertionFailedError(
                    "Create of 2nd EPHEMERAL_SEQUENTIAL znode was not allowed");
        }
        
        // verify the lexical ordering.
        if (zchild1.compareTo(zchild2) >= 0) {
            fail("zchild1=" + zchild1 + " GTE zchild2=" + zchild2);
        }

    }

}
