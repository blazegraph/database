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
 * Created on Nov 1, 2006
 */

package com.bigdata.service;

import junit.framework.TestCase;

import com.bigdata.journal.IsolationEnum;
import com.bigdata.service.OldTransactionServer;

/**
 * Test suite for semantics of the {@link OldTransactionServer}.
 * 
 * @todo The test suite has to cover the basic API, including correct generation
 *       of abort, prepare, and commit notices, but also the tracking of ground
 *       states and correct generation of GC notices.
 * 
 * @todo Test correct rejection for the API, e.g., out of sequence openSegment,
 *       abort, or commit requests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTransactionServer extends TestCase {

    /**
     * 
     */
    public TestTransactionServer() {
    }

    /**
     * @param arg0
     */
    public TestTransactionServer(String arg0) {
        super(arg0);
    }

    public void test_ctor() {
        
        new OldTransactionServer();
        
    }

    /**
     * Test verifies some correctness for determination of transaction ground
     * states and garbage collection of committed transactions for a ground
     * state once there are no more active transactions for that ground state.
     * 
     * @todo The test is not actually verifying anything.
     * 
     * @todo Do variant where an abort is used to trigger GC (GC only needs to
     *       wait for all transactions emerging from a ground state to complete
     *       (vs commit), and only is triggered for transactions that commit (vs
     *       abort)).
     */
    public void test001() {
        
        OldTransactionServer server = new MyTransactionServer();
        
        /* groundState is t0.
         * 
         * @todo This is bootstrapping a ground state, which is a bit kludgy.
         */
        final long t0 = server.startTx(IsolationEnum.ReadWrite);
        server.commitTx(t0);

        // @todo verify groundState for new transactions is t0.
        final long t1 = server.startTx(IsolationEnum.ReadWrite);
        final long t2 = server.startTx(IsolationEnum.ReadWrite);
        server.commitTx(t1);
        // @todo verify groundState for new transactions is t1.
        final long t3 = server.startTx(IsolationEnum.ReadWrite);
        server.commitTx(t2);
        /*
         * @todo verify groundState for new transactions is t2.
         * 
         * @todo verify that we receive GC notice for {t1,t2} and that t0 is
         * no longer an active groundState.
         */
        final long t4 = server.startTx(IsolationEnum.ReadWrite);
        server.commitTx(t3);
        /*
         * @todo verify groundState for new transactions is t4.
         * 
         * @todo verify that we receive GC notice for {t3}.
         */
        server.commitTx(t4);
        /*
         * @todo verify groundState for new transactions is t4.
         * 
         * @todo verify that we receive GC notice for {t4}.
         */

        // @todo shutdown / disconnect from the service under test.
        
    }

    /**
     * @todo Define the notion of the initial ground state for testing purposes.
     * 
     * @todo Rather than subclassing we could define an API for listeners to the
     *       transaction service and write tests based on events received by
     *       that API, but I am not sure that this makes much of a difference.
     *       
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MyTransactionServer extends OldTransactionServer {

        public MyTransactionServer() {
            super();
        }
        
        protected void notifyAbort(int segment, long ts) {
            super.notifyAbort(segment, ts);
        }

        protected void notifyPrepare(int segment, long ts) {
            super.notifyPrepare(segment, ts);
        }

        protected void notifyCommit(int segment, long ts) {
            super.notifyCommit(segment, ts);
        }

        protected void notifyGC(int segment, long ts) {
            super.notifyGC(segment, ts);
        }

    }
    
}
