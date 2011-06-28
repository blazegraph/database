/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 29, 2010
 */

package com.bigdata.quorum.zk;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.zookeeper.AbstractZooTestCase;
import com.bigdata.zookeeper.ZooKeeperAccessor;

/**
 * Test suite for {@link ZKQuorumImpl}. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZkQuorum extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZkQuorum() {
    }

    /**
     * @param name
     */
    public TestZkQuorum(String name) {
        super(name);
    }

    /**
     * Unit test for
     * {@link ZKQuorumImpl#setupQuorum(String, ZooKeeperAccessor, List)}
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void test_setupQuorum() throws KeeperException, InterruptedException {

        // a node that is guaranteed to be unique w/in the test namespace.
        final String logicalServiceId = "/test/" + getName()
                + UUID.randomUUID();

        // Create that znode. it will be the parent for this experiment.
        zookeeper.create(logicalServiceId, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        ZKQuorumImpl.setupQuorum(logicalServiceId, zookeeperAccessor, acl);

        dumpZoo(Level.INFO, "setupQuorum", logicalServiceId);
        
        /*
         * Verify the various child znodes were created.
         */
        final String quorumZPath = logicalServiceId + "/" + ZKQuorum.QUORUM;

        assertNotNull(quorumZPath, zookeeper
                .exists(quorumZPath, false/* watch */));

        assertNotNull(zookeeper.exists(quorumZPath + "/"
                + ZKQuorum.QUORUM_MEMBER, false/* watch */));

        assertNotNull(zookeeper.exists(quorumZPath + "/"
                + ZKQuorum.QUORUM_VOTES, false/* watch */));

        assertNotNull(zookeeper.exists(quorumZPath + "/"
                + ZKQuorum.QUORUM_PIPELINE, false/* watch */));

        assertNotNull(zookeeper.exists(quorumZPath + "/"
                + ZKQuorum.QUORUM_JOINED, false/* watch */));

    }

    /**
     * A simple integration test with <code>k:=1</code>.
     * 
     * @throws KeeperException
     * @throws IOException
     * @throws TimeoutException
     * @throws AsynchronousQuorumCloseException
     */
    public void test_run1() throws InterruptedException, KeeperException,
            IOException, AsynchronousQuorumCloseException, TimeoutException {

        // The service replication factor.
        final int k = 1;

        // The logical service identifier (guaranteed unique in test namespace).
        final String logicalServiceId = "/test/" + getName()
                + UUID.randomUUID();

        // Create that znode. it will be the parent for this experiment.
        zookeeper.create(logicalServiceId, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        
        // The per-client quorum objects.
        final ZKQuorumImpl[] quorums = new ZKQuorumImpl[k];
        final MockQuorumMember[] clients = new MockQuorumMember[k];
        final ZooKeeperAccessor[] accessors = new ZooKeeperAccessor[k];
        final QuorumActor[] actors = new QuorumActor[k];
        final MockServiceRegistrar<Remote> registrar = new MockServiceRegistrar();
        try {

            /*
             * Setup the client quorums.
             */
            for (int i = 0; i < k; i++) {
                accessors[i] = getZooKeeperAccessorWithDistinctSession();
                quorums[i] = new ZKQuorumImpl(k, accessors[i], acl);
                clients[i] = new MockQuorumMember(logicalServiceId, registrar){
                    public Remote newService() {
                        try {
                            return new MockService();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                registrar.put(clients[i].getServiceId(), clients[i].getService());
                quorums[i].start(clients[i]);
                actors[i] = quorums[i].getActor();
            }

            dumpZoo(Level.INFO, "before memberAdd()", logicalServiceId);

            // The serviceId of the 1st client.
            final UUID serviceId = clients[0].getServiceId();

            /*
             * Verify none of the client quorums have any members.
             */
            for (int i = 0; i < k; i++) {
                assertEquals(new UUID[] {}, quorums[i].getMembers());
            }
            // Verify the client is not a member of the quorum.
            assertFalse(clients[0].isMember());

            // tell the client's actor to add it as a quorum member.
            actors[0].memberAdd();
            dumpZoo(Level.INFO, "after memberAdd()", logicalServiceId);
//            log.info("members: "+Arrays.toString(quorums[0].getMembers()));
            
            actors[0].pipelineAdd();
            dumpZoo(Level.INFO, "after pipelineAdd()", logicalServiceId);
//            log.info("pipeline: "+Arrays.toString(quorums[0].getPipeline()));

            actors[0].castVote(1L);
            dumpZoo(Level.INFO, "after castVote(1L)", logicalServiceId);
//            log.info("votes: "+quorums[0].getVotes());
//            log.info("joined: "+Arrays.toString(quorums[0].getJoinedMembers()));

            // Note:serviceJoin should be a NOP since castVote will trigger join.
            actors[0].serviceJoin();
            dumpZoo(Level.INFO, "after serviceJoin()", logicalServiceId);
//            log.info("joined: "+Arrays.toString(quorums[0].getJoinedMembers()));
//            log.info("all: "+quorums[0].toStringAtomic());

            // wait for the quorum to meet.
            final long token = quorums[0].awaitQuorum(5, TimeUnit.SECONDS);
            if (log.isInfoEnabled())
                log.info("token=" + token);

            // verify that the quorum met.
            assertTrue(quorums[0].isQuorumMet());
            
            /*
             * Verify that the action was noticed by all quorum members.
             */
            for (int i = 0; i < k; i++) {
                assertEquals("client#" + i, new UUID[] { serviceId },
                        quorums[i].getMembers());
            }

            // The client should now be a quorum member.
            for (int i = 0; i < k; i++) {
                assertTrue("client#" + i, clients[i].isMember());
            }

        } finally {
            log.info("Tearing down harness.");
            for (int i = 0; i < k; i++) {
                if (quorums[i] != null)
                    quorums[i].terminate();
                if (accessors[i] != null) {
                    accessors[i].close();
                }
            }
        }

    }
    
}
