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
package com.bigdata.journal.jini.ha;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.FutureTaskInvariantMon;
import com.bigdata.ha.HAGlue;

/**
 * Test suite for {@link FutureTaskInvariantMon}. The test suite submits tasks
 * that establish specific invariants and then verifies that the violation of
 * those invariants is correctly detected. This is acheived using the
 * {@link InvariantTask} test helper class. That task only return
 * <code>true</code> iff the monitored invariant was violated.
 * 
 * @author Martyn Cutcher
 * @see FutureTaskInvariantMon
 * @see InvariantTask
 */
public class TestHA3InvariantListener extends AbstractHA3JournalServerTestCase {

    private static final long DEFAULT_TIMEOUT_MILLIS = 5000;
    
    public TestHA3InvariantListener() {

    }

    public TestHA3InvariantListener(final String name) {
        super(name);
    }

    public void testQuorumBreakInvariant() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        awaitMetQuorum();
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);

        // submit task, will be interrupted when quorum breaks!
        final Future<Boolean> res = serverA.submit(
                new InvariantTask.QuorumMet(), true/* asyncFuture */);

        simpleTransaction();

        serverB.destroy();

        // The task should note serverB leaving the service and interrupt the
        // thread
        assertTrue(res.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    }

    public void testMonitorServiceInvariant() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        awaitMetQuorum();
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitNSSAndHAReady(serverC);

        // submit task, will be interrupted when serviceB shutdsdown
        final Future<Boolean> res = serverA
                .submit(new InvariantTask.ServiceJoined(getServiceBId()), true/* asyncFuture */);

        simpleTransaction();

        this.shutdownB();

        // The task should note serverB leaving the service and interrupt the
        // thread
        assertTrue(res.get(2000, TimeUnit.MILLISECONDS));
    }

    public void testMonitorServiceNoChangeInvariant() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        awaitMetQuorum();
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitNSSAndHAReady(serverC);

        // submit task, will not be interrupted since serviceB will NOT shutdown
        final Future<Boolean> res = serverA
                .submit(new InvariantTask.ServiceJoined(getServiceBId()), true/* asyncFuture */);

        simpleTransaction();

        this.shutdownC();

        // Should expect a timeout exception
        try {
            res.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            fail("Timeout expected");
        } catch (TimeoutException te) {
            // expected
        }
    }

    /**
     * This test attempts to check that a services haStatus has changed.
     * 
     * Is this necessary? Or possible?
     * 
     * The internal haStatus is modified on quorum break or quorum meet or
     * service join/break.
     * 
     * The QuorumListener will observe the serviceLeave. Is there a case for
     * observing a service join? Perhaps a more general pattern is to define
     * invariants state which servcie MUST be present and which MUST NOT. Then
     * we could break on specific servcie joining?
     * 
     * Additionally, the invariant class could provide lists of required joined
     * and required not-joined services. If these were ConcurrenSkipLists then
     * they could be side-effected by the task and the listener would interrupt
     * if any incompatible events were observed.
     * 
     */
    public void testMonitorServerCJoinsInvariant() throws Exception {

        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
//        awaitNSSAndHAReady(serverA); 

        // submit task, will be interrupted once serverc has joined
        final Future<Boolean> res = serverA
                .submit(new InvariantTask.ServiceDoesntJoin(getServiceCId()),
                        true/* asyncFuture */);

        final HAGlue serverB = startB();

        awaitMetQuorum();
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);

        simpleTransaction();

        final HAGlue serverC = startC();
        awaitNSSAndHAReady(serverC);

        // Invariant task should be interrupted by server C joining
        assertTrue(res.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    }

    public void testMonitorServerCDoesntJoinInvariant() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        awaitMetQuorum();
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);

        // submit task, will be interrupted if serverC joins
        final Future<Boolean> res = serverA
                .submit(new InvariantTask.ServiceDoesntJoin(getServiceCId()),
                        true/* asyncFuture */);

        simpleTransaction();

        // Should expect a timeout exception since C never joins
        try {
            res.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            fail("Timeout expected");
        } catch (TimeoutException te) {
            // expected
        }
    }

    /**
     * This test just observes A leaving because the quorum is broken by B
     * shutting down
     */
    public void testMonitorServerALeaves() throws Exception {
        // only start 2 services to ensure logs are maintained
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        awaitMetQuorum();
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);

        // submit task, will be interrupted once serverA leaves
        final Future<Boolean> res = serverA
                .submit(new InvariantTask.ServiceJoined(getServiceAId()), true/* asyncFuture */);

        simpleTransaction();

        shutdownB();

        // Invariant task should be interrupted when server A leaves after
        // quorum break
        assertTrue(res.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    }

}
