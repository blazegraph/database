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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum.zk;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumWatcher;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.zookeeper.AbstractZooTestCase;
import com.bigdata.zookeeper.ZooKeeperAccessor;

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
    ZooKeeperAccessor[] accessors;
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

        accessors = new ZooKeeperAccessor[k];
        
        actors  = new QuorumActor[k];

        /*
         * Setup the client quorums.
         */
        for (int i = 0; i < k; i++) {
            accessors[i] = getZooKeeperAccessorWithDistinctSession();
            quorums[i] = new ZKQuorumImpl(k, accessors[i], acl);
            clients[i] = new MockQuorumMember(logicalServiceId, registrar) {
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

    /**
     * Wait up to a timeout until some condition succeeds.
     * <p>
     * Whenever more than one {@link AbstractQuorum} is under test there will be
     * concurrent indeterminism concerning the precise ordering and timing as
     * updates propagate from the {@link AbstractQuorum} which takes some action
     * (castVote(), pipelineAdd(), etc.) to the other quorums attached to the
     * same {@link MockQuorumFixture}. This uncertainty about the ordering and
     * timing state changes is not dissimilar from the uncertainty we face in a
     * real distributed system.
     * <p>
     * While there are times when this uncertainty does not affect the behavior
     * of the tests, there are other times when we must have a guarantee that a
     * specific vote order or pipeline order was established. For those cases,
     * this method may be used to await an arbitrary condition. This method
     * simply retries until the condition becomes true, sleeping a little after
     * each failure.
     * <p>
     * Actions executed in the main thread of the unit test will directly update
     * the internal state of the {@link MockQuorumFixture}, which is shared
     * across the {@link MockQuorum}s. However, uncertainty about ordering can
     * arise as a result of the interleaving of the actions taken by the
     * {@link QuorumWatcher}s in response to both top-level actions and actions
     * taken by other {@link QuorumWatcher}s. For example, the vote order or the
     * pipeline order are fully determined based on sequence such as the
     * following:
     * 
     * <pre>
     * actor0.pipelineAdd();
     * actor2.pipelineAdd();
     * actor1.pipelineAdd();
     * </pre>
     * 
     * When in doubt, or when a unit test displays stochastic behavior, you can
     * use this method to wait until the quorum state has been correctly
     * replicated to the {@link Quorum}s under test.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * @param timeout
     *            The timeout.
     * @param unit
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     */
    public void assertCondition(final Runnable cond, final long timeout,
            final TimeUnit units) {
        final long begin = System.nanoTime();
        long nanos = units.toNanos(timeout);
        // remaining -= (now - begin) [aka elapsed]
        nanos -= System.nanoTime() - begin;
        while (true) {
            try {
                // try the condition
                cond.run();
                // success.
                return;
            } catch (AssertionFailedError e) {
                nanos -= System.nanoTime() - begin;
                if (nanos < 0) {
                    // Timeout - rethrow the failed assertion.
                    throw e;
                }
            }
            // sleep and retry.
            try {
                // sleep up to 10ms or nanos, which ever is less.
                Thread
                        .sleep(Math.min(TimeUnit.NANOSECONDS.toMillis(nanos),
                                10));
            } catch (InterruptedException e1) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Waits up to 5 seconds for the condition to succeed.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     * 
     * @see #assertCondition(Runnable, long, TimeUnit)
     */
    public void assertCondition(final Runnable cond) {

        assertCondition(cond, 5, TimeUnit.SECONDS);

    }
    
}
