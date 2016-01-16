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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;

import com.bigdata.io.TestCase3;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum.MockQuorumActor;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;

/**
 * Abstract base class for testing using a {@link MockQuorumFixture}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorumTestCase extends TestCase3 {

    public AbstractQuorumTestCase() {
        
    }

    public AbstractQuorumTestCase(String name) {
        super(name);
    }

    /** The service replication factor (this must be set in {@link #setupUp()}). */
    protected int k;

    /** The per-client quorum objects. */
    protected MockQuorum[] quorums;

    /** The clients. */
    protected MockQuorumMember[] clients;

    /**
     * The per-client quorum actor objects. The unit tests send events on the
     * behalf of the clients using these actor objects.
     */
    protected MockQuorumActor[] actors;

    /** The mock shared quorum state object. */
    protected MockQuorumFixture fixture;
    
    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        if(log.isInfoEnabled())
            log.info(getName());

        if (k == 0)
            throw new AssertionError("k is not set");
        
        quorums = new MockQuorum[k];
        
        clients = new MockQuorumMember[k];

        actors  = new MockQuorumActor[k];

        fixture = new MockQuorumFixture();

        fixture.start(); 

        /*
         * Setup the client quorums.
         */
        final String logicalServiceId = "testLogicalService1";
        for (int i = 0; i < k; i++) {
            quorums[i] = new MockQuorum(k,fixture);
            clients[i] = new MockQuorumMember(logicalServiceId,fixture);
            quorums[i].start(clients[i]);
            actors [i] = quorums[i].getActor();
        }
    
    }

    protected void tearDown() throws Exception {
        if(log.isInfoEnabled())
            log.info(getName());
        if (quorums != null) {
            for (int i = 0; i < k; i++) {
                if (quorums[i] != null) {
                    quorums[i].terminate();
                    quorums[i] = null;
                }
            }
        }
        if (fixture != null) {
            fixture.terminate();
        }
        quorums = null;
        clients = null;
        actors  = null;
        fixture = null;
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
    static public void assertCondition(final Runnable cond,
            final long timeout, final TimeUnit units) {

        TestCase3.assertCondition(cond, timeout, units);
        
//        final long begin = System.nanoTime();
//        final long nanos = units.toNanos(timeout);
//        long remaining = nanos;
//        // remaining = nanos - (now - begin) [aka elapsed]
//        remaining = nanos - (System.nanoTime() - begin);
//        while (true) {
//            try {
//                // try the condition
//                cond.run();
//                // success.
//                return;
//            } catch (final AssertionFailedError e) {
//                remaining = nanos - (System.nanoTime() - begin);
//                if (remaining < 0) {
//                    // Timeout - rethrow the failed assertion.
//                    throw e;
//                }
//                // Sleep up to 10ms or the remaining nanos, which ever is less.
//                final int millis = (int) Math.min(
//                        TimeUnit.NANOSECONDS.toMillis(remaining), 10);
//                if (millis > 0) {
//                    // sleep and retry.
//                    try {
//                        Thread.sleep(millis);
//                    } catch (InterruptedException e1) {
//                        // propagate the interrupt.
//                        Thread.currentThread().interrupt();
//                        return;
//                    }
//                    remaining = nanos - (System.nanoTime() - begin);
//                    if (remaining < 0) {
//                        // Timeout - rethrow the failed assertion.
//                        throw e;
//                    }
//                }
//            }
//        }
    }

//    /**
//     * Waits up to 5 seconds for the condition to succeed.
//     * 
//     * @param cond
//     *            The condition, which must throw an
//     *            {@link AssertionFailedError} if it does not succeed.
//     * 
//     * @throws AssertionFailedError
//     *             if the condition does not succeed within the timeout.
//     * 
//     * @see #assertCondition(Runnable, long, TimeUnit)
//     */
//    static public void assertCondition(final Runnable cond) {
//        
//        assertCondition(cond, 5, TimeUnit.SECONDS);
//        
//    }

    /**
     * Helper method provides nice rendering of a votes snapshot.
     * <p>
     * Note: The snapshot uses a {@link UUID}[] rather than a collection for
     * each <code>lastCommitTime</code> key. However, by default toString() for
     * an array does not provide a nice rendering.
     * 
     * @param votes
     *            The votes.
     * @return The human readable representation.
     */
    public static String toString(final Map<Long, UUID[]> votes) {

        // put things into a ordered Collection. toString() for the Collection is nice.
        final Map<Long, LinkedHashSet<UUID>> m = new LinkedHashMap<Long, LinkedHashSet<UUID>>();
        
        for(Map.Entry<Long,UUID[]> e : votes.entrySet()) {
            
            final Long commitTime = e.getKey();

            final UUID[] a = e.getValue();

            LinkedHashSet<UUID> votesForCommitTime = m.get(commitTime);

            if(votesForCommitTime == null) {

                votesForCommitTime = new LinkedHashSet<UUID>();

                m.put(commitTime, votesForCommitTime);
                
            }
            
            for (UUID uuid : a) {
            
                votesForCommitTime.add(uuid);

            }
            
        }
        
        return m.toString();
        
    }
    
}
