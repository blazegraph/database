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
 * Created on Dec 19, 2006
 */
package com.bigdata.rdf.sail;

import java.util.Random;

/**
 * TestCase to test single writer/mutiple transaction committed readers with
 * SAIL interface.
 * 
 * @author Martyn Cutcher
 */
public class TestMROWTransactionsNoHistory extends TestMROWTransactions {

    public TestMROWTransactionsNoHistory() {
    }

    public TestMROWTransactionsNoHistory(final String arg0) {
        super(arg0);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
        
    /**
     * I do observe problems with the "no-history" version of this test. The
     * RWStore has known issues and a minimum retention time of zero is not
     * supported at this time.
     * 
     * <pre>
     * junit.framework.AssertionFailedError: Test failed: firstCause=java.lang.RuntimeException: java.lang.RuntimeException: Could not load Checkpoint: store=com.bigdata.journal.Journal@327556d1, addrCheckpoint={off=852288,len=220}, retentionMillis=0, nreaderThreads=19, nwriters=100, nreaders=400, indexManager=com.bigdata.journal.Journal@327556d1
     *     at junit.framework.TestCase2.fail(TestCase2.java:90)
     *     at com.bigdata.rdf.sail.TestMROWTransactions.domultiple_csem_transaction2(TestMROWTransactions.java:237)
     *     at com.bigdata.rdf.sail.TestMROWTransactionsNoHistory.test_multiple_csem_transaction_no_history_stress(TestMROWTransactionsNoHistory.java:66)
     *     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     *     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
     *     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     *     at java.lang.reflect.Method.invoke(Method.java:606)
     *     at junit.framework.TestCase.runTest(TestCase.java:154)
     *     at junit.framework.TestCase.runBare(TestCase.java:127)
     *     at junit.framework.TestResult$1.protect(TestResult.java:106)
     *     at junit.framework.TestResult.runProtected(TestResult.java:124)
     *     at junit.framework.TestResult.run(TestResult.java:109)
     *     at junit.framework.TestCase.run(TestCase.java:118)
     *     at junit.framework.TestSuite.runTest(TestSuite.java:208)
     *     at junit.framework.TestSuite.run(TestSuite.java:203)
     *     at org.eclipse.jdt.internal.junit.runner.junit3.JUnit3TestReference.run(JUnit3TestReference.java:130)
     *     at org.eclipse.jdt.internal.junit.runner.TestExecution.run(TestExecution.java:38)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.runTests(RemoteTestRunner.java:467)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.runTests(RemoteTestRunner.java:683)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.run(RemoteTestRunner.java:390)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.main(RemoteTestRunner.java:197)
     * Caused by: java.lang.RuntimeException: java.lang.RuntimeException: Could not load Checkpoint: store=com.bigdata.journal.Journal@327556d1, addrCheckpoint={off=852288,len=220}
     *     at com.bigdata.rdf.lexicon.LexiconRelation.addTerms(LexiconRelation.java:1861)
     *     at com.bigdata.rdf.lexicon.LexiconRelation.addTerms(LexiconRelation.java:1722)
     *     at com.bigdata.rdf.store.AbstractTripleStore.getAccessPath(AbstractTripleStore.java:2868)
     *     at com.bigdata.rdf.sail.BigdataSail$BigdataSailConnection.getStatements(BigdataSail.java:3534)
     *     at com.bigdata.rdf.sail.BigdataSail$BigdataSailConnection.getStatements(BigdataSail.java:3470)
     *     at com.bigdata.rdf.sail.BigdataSail$BigdataSailConnection.getStatements(BigdataSail.java:3433)
     *     at com.bigdata.rdf.sail.TestMROWTransactions$Reader.call(TestMROWTransactions.java:404)
     *     at com.bigdata.rdf.sail.TestMROWTransactions$Reader.call(TestMROWTransactions.java:1)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:262)
     *     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
     *     at java.lang.Thread.run(Thread.java:745)
     * Caused by: java.lang.RuntimeException: Could not load Checkpoint: store=com.bigdata.journal.Journal@327556d1, addrCheckpoint={off=852288,len=220}
     *     at com.bigdata.btree.Checkpoint.loadFromCheckpoint(Checkpoint.java:756)
     *     at com.bigdata.journal.AbstractJournal.getIndexWithCheckpointAddr(AbstractJournal.java:5288)
     *     at com.bigdata.journal.AbstractJournal.getIndexWithCommitRecord(AbstractJournal.java:5135)
     *     at com.bigdata.journal.AbstractJournal.getIndexLocal(AbstractJournal.java:5005)
     *     at com.bigdata.journal.AbstractJournal.getIndex(AbstractJournal.java:4897)
     *     at com.bigdata.journal.Journal.getIndexSources(Journal.java:2656)
     *     at com.bigdata.journal.Journal.getIndex(Journal.java:2892)
     *     at com.bigdata.journal.Journal.getIndex(Journal.java:1)
     *     at com.bigdata.relation.AbstractRelation.getIndex(AbstractRelation.java:238)
     *     at com.bigdata.relation.AbstractRelation.getIndex(AbstractRelation.java:198)
     *     at com.bigdata.relation.AbstractRelation.getIndex(AbstractRelation.java:166)
     *     at com.bigdata.rdf.lexicon.LexiconRelation.getTerm2IdIndex(LexiconRelation.java:984)
     *     at com.bigdata.rdf.lexicon.LexiconRelation.addTerms(LexiconRelation.java:1857)
     *     ... 11 more
     * Caused by: java.lang.RuntimeException: addr=-8196 : cause=com.bigdata.util.ChecksumError: offset=852288,nbytes=224,expected=721420255,actual=-1747893185
     *     at com.bigdata.rwstore.RWStore.getData(RWStore.java:1899)
     *     at com.bigdata.journal.RWStrategy.readFromLocalStore(RWStrategy.java:727)
     *     at com.bigdata.journal.RWStrategy.read(RWStrategy.java:154)
     *     at com.bigdata.journal.AbstractJournal.read(AbstractJournal.java:4043)
     *     at com.bigdata.btree.Checkpoint.load(Checkpoint.java:575)
     *     at com.bigdata.btree.Checkpoint.loadFromCheckpoint(Checkpoint.java:754)
     *     ... 23 more
     * Caused by: com.bigdata.util.ChecksumError: offset=852288,nbytes=224,expected=721420255,actual=-1747893185
     *     at com.bigdata.io.writecache.WriteCacheService._readFromLocalDiskIntoNewHeapByteBuffer(WriteCacheService.java:3706)
     *     at com.bigdata.io.writecache.WriteCacheService._getRecord(WriteCacheService.java:3521)
     *     at com.bigdata.io.writecache.WriteCacheService.access$1(WriteCacheService.java:3493)
     *     at com.bigdata.io.writecache.WriteCacheService$1.compute(WriteCacheService.java:3358)
     *     at com.bigdata.io.writecache.WriteCacheService$1.compute(WriteCacheService.java:1)
     *     at com.bigdata.util.concurrent.Memoizer$1.call(Memoizer.java:77)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:262)
     *     at com.bigdata.util.concurrent.Memoizer.compute(Memoizer.java:92)
     *     at com.bigdata.io.writecache.WriteCacheService.loadRecord(WriteCacheService.java:3463)
     *     at com.bigdata.io.writecache.WriteCacheService.read(WriteCacheService.java:3182)
     *     at com.bigdata.rwstore.RWStore.getData(RWStore.java:1890)
     *     ... 28 more
     * </pre>
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child
     *      does not have persistent identity </a>.
     */
    // Note: This test is disabled since there are known issues when retentionMillis:=0.
    public void _test_multiple_csem_transaction_no_history_stress() throws Exception {

        final Random r = new Random();
        
        for (int i = 0; i < 10; i++) {

            final int nreaderThreads = r.nextInt(19) + 1;
            
            log.warn("Trial: " + i + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(0/* retentionMillis */,
                    nreaderThreads, 100/* nwriters */, 400/* nreaders */, false/* isolatableIndices */);

        }
        
    }

    public void test_multiple_csem_transaction_no_history_stress_readWriteTx()
            throws Exception {

        final Random r = new Random();

        for (int i = 0; i < 10; i++) {

            final int nreaderThreads = r.nextInt(19) + 1;

            log.warn("Trial: " + i + ", nreaderThreads=" + nreaderThreads);

            domultiple_csem_transaction2(0/* retentionMillis */,
                    nreaderThreads, 100/* nwriters */, 400/* nreaders */, true/* isolatableIndices */);

        }

    }

}
