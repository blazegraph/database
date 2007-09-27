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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates all tests into something approximately increasing dependency
 * order. Most of the tests that are aggregated are proxied test suites and will
 * therefore run with the configuration of the test class running that suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see AbstractTestCase
 * @see ProxyTestCase
 */

public class TestJournalBasics extends TestCase {

    public TestJournalBasics() {
        super();
    }

    public TestJournalBasics(String arg0) {
        super(arg0);
    }

    /**
     * Aggregates the test suites into something approximating increasing
     * dependency. This is designed to run as a <em>proxy test suite</em> in
     * which all tests are run using a common configuration and a delegatation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("Core Journal Test Suite");

        // tests of creation, lookup, use, commit of named indices.
        suite.addTestSuite( TestNamedIndices.class );
        
        // tests the ability to recover and find historical commit records.
        suite.addTestSuite( TestCommitHistory.class );
        
        /*
         * tests of transaction support.
         */
        
        // tests of transitions in the transaction RunState state machine.
        suite.addTest( AbstractTestTxRunState.suite() );
        // @todo update these tests of the tx-journal integration.
        suite.addTestSuite( TestTxJournalProtocol.class );
        // tests of read-write transactions and isolation.
        suite.addTestSuite( TestTx.class );
        // tests of read-only transactions.
        suite.addTestSuite( TestReadOnlyTx.class );
        // @todo tests of read-committed transactions.
        suite.addTestSuite( TestReadCommittedTx.class );
        // todo tests of write-write conflict resolution.
        suite.addTestSuite(TestConflictResolution.class);
        
        /*
         * @todo tests of batch api and group commit mechanisms for very high
         * volume updates. These tests might be more relevent to the data server
         * since group commit can be achieved by transparently collecting small
         * non-conflicting updates into a transaction that succeeds or fails all
         * updates in the group. in this model transactions are local and
         * updates do not have atomicity across journals. another alternative is
         * to use a pulse to commit a global group update transaction with a
         * frequency that trades off the size of the commit groups against
         * latency. the advantage of the latter approach is that it can be
         * combined with normal transaction processing in a trivial manner and I
         * am not sure whether or not that is true of the former approach.
         */
        
        /*
         * @todo stress tests of concurrent transactions parameterized so that
         * we can also test overflow handling and scale up. Scale out testing
         * may require a refactor of the clients to lookup services. This could
         * be moved out of the per-journal strategy test suite and performed
         * only for the target strategy, e.g., Direct. Measure throughput rates
         * and compare TPS and write/read rates with other systems.
         * 
         * @todo we could add tests based on known transaction processing
         * benchmarks here as well.
         */
        suite.addTestSuite(StressTestConcurrent.class);

        return suite;
        
    }

}
