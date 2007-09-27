/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
