/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.msg.HAGlobalWriteLockRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.IHAGlobalWriteLockRequest;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Test suite {@link HAGlue#globalWriteLock(IHAGlobalWriteLockRequest)}.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHAJournalServerGlobalWriteLock extends AbstractHA3JournalServerTestCase {

    public TestHAJournalServerGlobalWriteLock() {
    }

    public TestHAJournalServerGlobalWriteLock(String name) {
        super(name);
    }
    
    /**
     * Unit test verifies that we can request, obtain, and hold the global write
     * lock.  The test verifies that writes are suspended while the global write
     * lock is held and verifies that we can release the global write lock and
     * that writes resume once it has been released.
     * 
     * TODO Variant in which we allow the lock to timeout by itself.
     * 
     * TODO Variant in which we verify that two threads can not both obtain the
     * lock at the same time.
     */
    public void testGlobalWriteLock() throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Verify ONE (1) commit point.
         */
        final IRootBlockView rootBlock1 = serverA.getRootBlock(
                new HARootBlockRequest(null/* storeUUID */)).getRootBlock();

        final long lastCommitCounter1 = rootBlock1.getCommitCounter();

        assertEquals(1L, lastCommitCounter1);

        // Figure out which service is the quorum leader.
        final HAGlue leader = quorum.getClient().getLeader(token);

        // Acquire the global write lock.
        final Future<Void> ft = leader
                .globalWriteLock(new HAGlobalWriteLockRequest(//
                        2000,// lockWaitTimeout
                        TimeUnit.MILLISECONDS,// lockWaitUnits
                        5000,// lockHoldTimeout
                        TimeUnit.MILLISECONDS// lockHoldUnits
                ));
        
        // Lock was not acquired.
        assertFalse(ft.isDone());

        final RemoteRepository repo = getRemoteRepository(leader);

        final FutureTask<Void> ft2 = new FutureTask<Void>(new Callable<Void>(){
            public Void call() throws Exception {
                final StringBuilder sb = new StringBuilder();
                sb.append("DROP ALL;\n");
                sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
                sb.append("INSERT DATA {\n");
                sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
                sb.append("  dc:creator \"A.N.Other\" .\n");
                sb.append("}\n");
                
                final String updateStr = sb.toString();
                
                // Verify quorum is still valid.
                quorum.assertQuorum(token);

                repo.prepareUpdate(updateStr).evaluate();
                // Done.
                return null;
            }
        });

        /*
         * Verify write is not processed while lock is held.
         */
        {
            executorService.submit(ft2);
            try {
                ft2.get(2000, TimeUnit.MILLISECONDS);
                if (!ft.isDone())
                    fail("Write accepted while global lock is held.");
            } catch (TimeoutException ex) {
                // Expected - fall through.
            }
        }
        
        // Verify nothing was committed.
        assertEquals(
                1L,
                serverA.getRootBlock(
                        new HARootBlockRequest(null/* storeUUID */))
                        .getRootBlock().getCommitCounter());

        // Release the lock.
        ft.cancel(true/*mayInterruptIfRunning*/);

        /*
         * Verify writes are now accepted and result in a new commit point.
         */
        {
            try {
                ft2.get(2000, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                fail("Write not accepted after global lock was released.");
            }
        }

        // Verify new commit point now exists.
        assertEquals(
                2L,
                serverA.getRootBlock(
                        new HARootBlockRequest(null/* storeUUID */))
                        .getRootBlock().getCommitCounter());

        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

    }

}
