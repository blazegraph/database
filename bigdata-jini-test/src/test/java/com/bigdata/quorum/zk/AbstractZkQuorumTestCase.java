/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum.zk;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.zookeeper.AbstractZooTestCase;

/**
 * Abstract base class for testing using a {@link MockQuorumFixture}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractZkQuorumTestCase extends AbstractZooTestCase {

    public AbstractZkQuorumTestCase() {
        
    }

    public AbstractZkQuorumTestCase(String name) {
        super(name);
    }

    /**
     * The service replication factor (this mus be set in {@link #setupUp()}).
     */
    protected int k;

    // The logical service identifier (guaranteed unique in test namespace).
    final String logicalServiceId = "/test/" + getName()
            + UUID.randomUUID();

    // The per-client quorum objects.
    ZKQuorumImpl[] quorums;
    MockQuorumMember[] clients;
    ZooKeeper[] accessors;
    QuorumActor[] actors;
    final MockServiceRegistrar<Remote> registrar = new MockServiceRegistrar();
    
    @Override
    public void setUp() throws Exception {

        super.setUp();
        
        if(log.isInfoEnabled())
            log.info(": " + getName());

        if (k == 0)
            throw new AssertionError("k is not set");
        
        // Create that znode. it will be the parent for this experiment.
        zookeeper.create(logicalServiceId, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        
        quorums = new ZKQuorumImpl[k];
        
        clients = new MockQuorumMember[k];

        accessors = new ZooKeeper[k];
        
        actors  = new QuorumActor[k];

        /*
         * Setup the client quorums.
         */
        for (int i = 0; i < k; i++) {
            accessors[i] = getZooKeeperAccessorWithDistinctSession();
            final ZooKeeper zk = accessors[i];
            quorums[i] = new ZKQuorumImpl(k);//, accessors[i], acl);
            clients[i] = new MockQuorumMember(logicalServiceId, registrar) {
                public Remote newService() {
                    try {
                        return new MockService();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public ZooKeeper getZooKeeper() {
                    return zk;
                }

                @Override
                public List getACL() {
                    return acl;
                }
            };
            registrar.put(clients[i].getServiceId(), clients[i].getService());
            quorums[i].start(clients[i]);
            actors[i] = quorums[i].getActor();
        }
    
    }

    @Override
    public void tearDown() throws Exception {
        if(log.isInfoEnabled())
            log.info(": " + getName());
        for (int i = 0; i < k; i++) {
            if (quorums[i] != null)
                quorums[i].terminate();
            if (accessors[i] != null) {
                accessors[i].close();
            }
        }
        super.tearDown();
    }

//    protected void assertCondition(final Runnable cond, final long timeout,
//            final TimeUnit units) {
//
//        AbstractQuorumTestCase.assertCondition(cond, timeout, units);
//
//    }
//
//    protected void assertCondition(final Runnable cond) {
//
//        AbstractQuorumTestCase.assertCondition(cond, 5, TimeUnit.SECONDS);
//
//    }

}
