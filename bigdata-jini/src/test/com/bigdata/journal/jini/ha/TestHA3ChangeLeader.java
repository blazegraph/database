/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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
package com.bigdata.journal.jini.ha;

import java.util.concurrent.TimeUnit;

import com.bigdata.ha.HAGlue;

public class TestHA3ChangeLeader extends AbstractHA3JournalServerTestCase  {
	
    /**
     * We have seen problems with updates when the leader changes, this test reconstructs
     * this simple scenario, with and update transaction, change of leader and then a 
     * second update transaction.
     * 
     * @throws Exception
     */
    public void testStartABC_ChangeLeader() throws Exception {

        // Start 3 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token1 = awaitFullyMetQuorum();

        // await pipeline
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] { serverA, serverB,
                serverC });

        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

		/*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        simpleTransaction();
        
        shutdownA();

        final long token2 = awaitNextQuorumMeet(token1);
       	        
        simpleTransaction();

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverB, serverC });
       
    }
}
