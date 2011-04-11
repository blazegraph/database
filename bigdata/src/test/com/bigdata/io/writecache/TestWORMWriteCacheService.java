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
 * Created on Feb 10, 2010
 */

package com.bigdata.io.writecache;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlueBase;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumPipeline;
import com.bigdata.ha.QuorumPipelineImpl;
import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.TestCase3;
import com.bigdata.io.writecache.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.io.writecache.WriteCache.FileChannelWriteCache;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.AbstractQuorumMember;
import com.bigdata.quorum.AbstractQuorumTestCase;
import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.ChecksumUtility;

/**
 * Test suite for the {@link WriteCacheService} using pure append writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestWriteCacheService.java 2866 2010-05-18 18:36:35Z
 *          thompsonbry $
 * 
 * @todo An occasional error can be observed (at least for WORM) where the
 *       {@link WriteCache} has no bytes written but a non-empty record map.
 *       This would appear to be an problem with the record map maintenance.
 * 
 *       <pre>
 * Caused by: java.lang.AssertionError: Empty cache: com.bigdata.io.WriteCache$FileChannelWriteCache@92dcdb{recordCount=1674,firstOffset=0,releaseBuffer=true,bytesWritten=0,bytesRemaining=1048576}
 *     at com.bigdata.io.WriteCacheService$WriteTask.call(WriteCacheService.java:536)
 *     at com.bigdata.io.WriteCacheService$WriteTask.call(WriteCacheService.java:1)
 *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
 *     at java.util.concurrent.FutureTask.run(FutureTask.java:138)
 *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
 *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
 *     at java.lang.Thread.run(Thread.java:619)
 * </pre>
 */
public class TestWORMWriteCacheService extends TestCase3 {

    protected static final Logger log = Logger.getLogger
    ( TestWORMWriteCacheService.class
      );

    /**
     * 
     */
    public TestWORMWriteCacheService() {
    }

    /**
     * @param name
     */
    public TestWORMWriteCacheService(String name) {
        super(name);
    }

    /**
     * The size of a {@link WriteCache} buffer.
     */
    private static int WRITE_CACHE_BUFFER_CAPACITY = DirectBufferPool.INSTANCE
            .getBufferCapacity();
    
    /*
     * Shared setup values.
     */
    
    /**
     * The #of records to write. 10k is small and the file system cache can
     * often absorb the data immediately. 100k is reasonable.
     */
//    static final int nrecs = 10;
    static final int nrecs = 10000;

    /**
     * The #of records to write for an RW store.
     */
    static final int nrecsRW = nrecs;

    /**
     * The maximum size of a normal record. The database averages 1k per record
     * (WORM) and 4-8k per record (RW).
     */
    static final int maxreclen = Bytes.kilobyte32;

    /**
     * The percentage of records which are larger than the {@link WriteCache}
     * buffer capacity. These records are written using a different code path.
     */
    static final double largeRecordRate = .001;

    /**
     * Mock {@link HAPipelineGlue} implementation.
     */
    static class MockHAPipelineGlue implements HAGlueBase, HAPipelineGlue {

        private final UUID serviceId;

        private final InetSocketAddress addr;

        private final QuorumMember<HAPipelineGlue> member;

        MockHAPipelineGlue(final QuorumMember<HAPipelineGlue> member)
                throws IOException {

            this.serviceId = member.getServiceId();

            this.member = member;

            // chose a random port.
            this.addr = new InetSocketAddress(getPort(0/* suggestedPort */));

        }

        public UUID getServiceId() {
            return serviceId;
        }
        
        public InetSocketAddress getWritePipelineAddr() {
            return addr;
        }

        public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
                throws IOException {

            return ((QuorumPipeline<HAPipelineGlue>) member)
                    .receiveAndReplicate(msg);
            
        }

        /**
         * Note: This is not being invoked due to the implementation of
         * {@link QuorumActor#reorganizePipeline} in {@link MockQuorumFixture}.
         */
        public RunnableFuture<Void> moveToEndOfPipeline() throws IOException {

            final FutureTask<Void> ft = new FutureTask<Void>(
                    new Runnable() {
                        public void run() {

                            // note the current vote (if any).
                            final Long lastCommitTime = member.getQuorum()
                                    .getCastVote(getServiceId());

                            if (member.isPipelineMember()) {

                                // System.err
                                // .println("Will remove self from the pipeline: "
                                // + getServiceId());

                                member.getActor().pipelineRemove();

                                // System.err
                                // .println("Will add self back into the pipeline: "
                                // + getServiceId());

                                member.getActor().pipelineAdd();

                                if (lastCommitTime != null) {

                                    // System.err
                                    // .println("Will cast our vote again: lastCommitTime="
                                    // + +lastCommitTime
                                    // + ", "
                                    // + getServiceId());

                                    member.getActor().castVote(lastCommitTime);

                                }

                            }
                        }
                    }, null/* result */);
            member.getExecutor().execute(ft);
            return ft;

        }
        
    } // class MockHAPipelineGlue

    /**
     * Mock {@link QuorumMember} implements {@link QuorumPipeline}.
     */
    static class MyMockQuorumMember<S extends HAPipelineGlue> extends
            AbstractQuorumMember<S> implements QuorumPipeline<S> {

        private final MockQuorumFixture fixture;
        
        private final QuorumPipelineImpl<S> pipelineImpl;

        private final S serviceImpl;

        /**
         * The #of write cache blocks received by this service. If a service is
         * running as the quorum leader, then it WILL NOT report the write cache
         * blocks which it sends here.
         */
        final AtomicLong nreceived = new AtomicLong();
        
        protected MyMockQuorumMember(final MockQuorumFixture fixture,
                final String logicalServiceId)
                throws IOException {

            super(logicalServiceId, UUID.randomUUID()/* serviceId */);

            this.fixture = fixture;

            this.serviceImpl = (S) new MockHAPipelineGlue(
                    (QuorumMember<HAPipelineGlue>) this);

            addListener(this.pipelineImpl = new QuorumPipelineImpl<S>(this){

                protected void handleReplicatedWrite(final HAWriteMessage msg,
                        final ByteBuffer data) throws Exception {

                    nreceived.incrementAndGet();
                    
                    if (log.isTraceEnabled())
                        log.trace("nreceived=" + nreceived + ", message=" + msg
                                + ", data=" + data);

                    final ChecksumUtility chk = ChecksumUtility.threadChk.get();

                    final int actualChk = chk.checksum(data);

                    if (msg.getChk() != actualChk) {

                        /*
                         * Note: This is how we validate the write pipeline. The
                         * HAReceiveService also has basically the same logic,
                         * so this is not really adding much (if any) value
                         * here. Everyone is using the same ChecksumUtility, so
                         * a correlated failure is possible. If we wanted to go
                         * a little further, we could collect the data in memory
                         * or write it to the disk, and then verify it byte by
                         * byte.
                         */

                        fail("expected=" + msg.getChk() + ", actual="
                                + actualChk + ", msg=" + msg + ", data=" + data);

                    }

                }

            });

        }

        public S getService(final UUID serviceId) {
            
            return (S) fixture.getService(serviceId);
            
        }

        public Executor getExecutor() {
            
            return fixture.getExecutor();
            
        }

        public S getService() {
            
            return serviceImpl;

        }

        public HAReceiveService<HAWriteMessage> getHAReceiveService() {
            
            return pipelineImpl.getHAReceiveService();
            
        }

        public HASendService getHASendService() {
            
            return pipelineImpl.getHASendService();
            
        }

        public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
                throws IOException {
            
            return pipelineImpl.receiveAndReplicate(msg);
            
        }

        public Future<Void> replicate(final HAWriteMessage msg,
                final ByteBuffer b) throws IOException {

            return pipelineImpl.replicate(msg, b);
            
        }

    } // MockQuorumMemberImpl
    
    /**
     * A test which looks for deadlock conditions (one buffer).
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_WORM_1buffer()
            throws InterruptedException, IOException {

        final int nbuffers = 1;
        final boolean useChecksums = false;
        final boolean isHighlyAvailable = false;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate,
                    useChecksums, isHighlyAvailable, StoreTypeEnum.WORM, quorum);
        
        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }

    /**
     * A test which looks for deadlock conditions (one buffer).
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_RW_1buffer()
            throws InterruptedException, IOException {

        final int nbuffers = 1;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = false;
        final boolean isHighlyAvailable = false;

        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {

            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate,
                    useChecksums, isHighlyAvailable, StoreTypeEnum.RW, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }
        
    }

    /**
     * A test which looks for starvation conditions (2 buffers).
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_WORM_2buffers()
            throws InterruptedException, IOException {

        final int nbuffers = 2;
        final boolean useChecksums = false;
        final boolean isHighlyAvailable = false;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                    isHighlyAvailable, StoreTypeEnum.WORM, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }

    /**
     * A test which looks for starvation conditions (2 buffers).
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_RW_2buffers()
            throws InterruptedException, IOException {

        final int nbuffers = 2;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = false;
        final boolean isHighlyAvailable = false;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                    isHighlyAvailable, StoreTypeEnum.RW, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }

    /**
     * A high throughput configuration with record level checksums.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_WORM_6buffers_recordChecksums()
            throws InterruptedException, IOException {

        final int nbuffers = 6;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = false;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                    isHighlyAvailable, StoreTypeEnum.WORM, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }

    /**
     * A high throughput configuration with record level checksums.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_RW_6buffers_recordChecksums()
            throws InterruptedException, IOException {

        final int nbuffers = 6;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = false;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                    isHighlyAvailable, StoreTypeEnum.RW, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }

    /**
     * A high throughput configuration with record level checksums and whole
     * buffer checksums.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_WORM_6buffers_recordChecksums_wholeBufferChecksums()
            throws InterruptedException, IOException {

        final int nbuffers = 6;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = true;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                    isHighlyAvailable, StoreTypeEnum.WORM, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }
    
    /**
     * A high throughput configuration with record level checksums and whole
     * buffer checksums.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_RW_6buffers_recordChecksums_wholeBufferChecksums()
            throws InterruptedException, IOException {

        final int nbuffers = 6;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        final boolean isHighlyAvailable = true;

        // No write pipeline.
        final int k = 1;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor = quorum.getActor();

            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(lastCommitTime);
            fixture.awaitDeque();
                        
            doStressTest(nbuffers, nrecs, maxreclen, largeRecordRate, useChecksums,
                    isHighlyAvailable, StoreTypeEnum.RW, quorum);

        } finally {
            quorum.terminate();
            fixture.terminate();
        }

    }
    
    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, one buffer,
     * and one record written.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_WORM_1record_1buffer_k3_size2()
            throws InterruptedException, IOException {

    	failHATest();
    	
        final int nbuffers = 1;
        final int nrecs = 1;
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();

            actor0.pipelineAdd();
            actor1.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // cast votes.  someone will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();
                        
            // await quorum meets.
            final long token = quorum0.awaitQuorum();
            assertEquals(token, quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.WORM, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());
            
            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {
            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();
        }

    }

    /**
     * FIXME This method is being used to disable some unit tests for the
     * dynamic reorganization of the write pipeline which are causing CI to
     * deadlock. The issue appears to be related to quorum dynamics, especially,
     * and perhaps only, when we have to reorganize the pipeline in order for it
     * to function correctly.  I've disabled these tests for now since the write
     * pipeline is not being actively worked on at the moment and since the mock
     * quorum implementation is itself suspect (it may allow transitions through
     * illegal states which it can not then handle).  There are notes about this
     * in the quorum implementation and how it might have to be restructured to
     * no longer assume that it observes only legal states.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/235
     */
    private void failReorganizePipelineTest() {
        
        fail("Test not run.  See https://sourceforge.net/apps/trac/bigdata/ticket/235");
        
    }
    
    /**
     * FIXME This method is being used to disable the write cache service tests
     * which involve highly available services (k>1).  I've disabled these tests
     * for now since they occasionally result in CI deadlocks. 
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/235
     */
    private void failHATest() {
    	
    	fail("Test not run.  See https://sourceforge.net/apps/trac/bigdata/ticket/235");
    	
    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, one buffer,
     * and one record written where the leader must reorganize the write
     * pipeline when it is elected.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_WORM_1record_1buffer_k3_size2_reorganizePipeline()
            throws InterruptedException, IOException {

        // conditionally fail this test since it deadlocks CI.
        failReorganizePipelineTest();
        
        final int nbuffers = 1;
        final int nrecs = 1;
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();

            /*
             * Setup the pipeline for the actors in an order which does not
             * agree with the vote order so the leader will have to reorganize
             * when it is elected.
             */
            actor1.pipelineAdd();
            actor0.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // Verify the pipeline order.
            assertEquals(new UUID[] {//
                    quorum1.getClient().getServiceId(),//
                    quorum0.getClient().getServiceId() //
                    }, quorum0.getPipeline());

            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();
                        
            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);

            // Verify the pipeline was reorganized into the vote order.
            assertEquals(new UUID[] {//
                    quorum0.getClient().getServiceId(), //
                    quorum1.getClient().getServiceId(),//
                    }, quorum0.getPipeline());

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.WORM, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());
            
            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {
            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();
        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 3 running services, one buffer,
     * and one record written where the leader must reorganize the write
     * pipeline when it is elected.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_WORM_1record_1buffer_k3_size3_reorganizePipeline()
            throws InterruptedException, IOException {

        // conditionally fail this test since it deadlocks CI.
        failReorganizePipelineTest();

        final int nbuffers = 1;
        final int nrecs = 1;
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
            actor2.memberAdd();
            fixture.awaitDeque();

            /*
             * Setup the pipeline for the actors in an order which does not
             * agree with the vote order so the leader will have to reorganize
             * when it is elected.
             */
            actor2.pipelineAdd();
            actor1.pipelineAdd();
            actor0.pipelineAdd();
            fixture.awaitDeque();
            
            // Verify the pipeline order.
            assertEquals(new UUID[] {//
                    quorum2.getClient().getServiceId(),//
                    quorum1.getClient().getServiceId(), //
                    quorum0.getClient().getServiceId(), //
                    }, quorum0.getPipeline());

            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();
                        
            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token, quorum1.awaitQuorum());
            assertEquals(token, quorum2.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            AbstractQuorumTestCase.assertCondition(new Runnable() {
                public void run() {
                    assertEquals(3, quorum0.getJoined().length);
                    assertEquals(3, quorum1.getJoined().length);
                    assertEquals(3, quorum2.getJoined().length);
                }
            });

            /*
             * Verify the pipeline was reorganized into the vote order.
             * 
             * Note: All that is guaranteed is that the pipeline leader will be
             * the first in the vote and join orders. The followers in the
             * pipeline may appear in any order (this is done to allow the
             * pipeline to be optimized for the network topology).
             */
            // verify #of services in the pipeline.
            assertEquals(3, quorum0.getPipeline().length);
            // verify first service in the pipeline is the quorum leader.
            assertEquals(quorum0.getClient().getServiceId(), quorum0
                    .getPipeline()[0]);
//            assertEquals(new UUID[] {//
//                    quorum0.getClient().getServiceId(), //
//                    quorum1.getClient().getServiceId(),//
//                    quorum2.getClient().getServiceId(),//
//                    }, quorum0.getPipeline());

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.WORM, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());
            
            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(3,quorum0.getJoined().length);

        } finally {
            quorum0.terminate();
            quorum1.terminate();
            quorum2.terminate();
            fixture.terminate();
        }

    }

    /**
     * Martyn wrote:
     * 
     * <pre>
     * The simplest way to recreate the deadlock issue is to add a stress test
     * to TestWORMWriteCacheService [this method].
     * 
     * ...
     * 
     * This will create a deadlock after a failure in serviceJoin spewed from
     * the pipeline reorganization:
     * 
     * com.bigdata.quorum.QuorumException: Not a pipeline member : 3b680846-30dc-4013-bc15-47fd1c4b20a5
     *      at com.bigdata.quorum.AbstractQuorum$QuorumActorBase.serviceJoin(AbstractQuorum.java:1251)
     *      at com.bigdata.quorum.AbstractQuorum$QuorumWatcherBase$3.run(AbstractQuorum.java:2249)
     *      at com.bigdata.quorum.AbstractQuorum$QuorumWatcherBase$1.run(AbstractQuorum.java:1927)
     *      at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
     *      at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
     *      at java.lang.Thread.run(Thread.java:680)
     * 
     * As you summarized it appears to be due to an awaited condition which
     * allows an interleaving of the lock such that a concurrent request to
     * serviceJoin gets processed after the service had been removed from the
     * pipeline prior to adding at the end.  Naively increasing lock protection
     * results in immediate deadlock.
     * 
     * I have on occasion seen other failures relating to malformed quorums,
     * but I suspect this is more of the same.
     * </pre>
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_STRESSwriteCacheService_HA_WORM_1record_1buffer_k3_size3_reorganizePipeline()
            throws InterruptedException, IOException {

        // conditionally fail this test since it deadlocks CI.
        failReorganizePipelineTest();

        for (int i = 0; i < 80; i++) {
        
            System.out.println("TEST " + i);

            test_writeCacheService_HA_WORM_1record_1buffer_k3_size3_reorganizePipeline();
            
        }
        
    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, one buffer,
     * and one record written.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_RW_1record_1buffer_k3_size2()
            throws InterruptedException, IOException {

    	failHATest();

    	final int nbuffers = 1;
        final int nrecs = 1;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.RW, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {

            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();
            
        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, one buffer
     * and the default #of records written and the default percentage of large
     * records.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_WORM_1buffer_k3_size2()
            throws InterruptedException, IOException {

    	failHATest();

    	final int nbuffers = 1;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.WORM, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {

            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();

        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, one buffer
     * and the default #of records written and the default percentage of large
     * records.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_RW_1buffer_k3_size2()
            throws InterruptedException, IOException {

    	failHATest();

        final int nbuffers = 1;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);
            
            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.RW, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {

            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();

        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, two buffers
     * and the default #of records written and the default percentage of large
     * records.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_WORM_2buffer_k3_size2()
            throws InterruptedException, IOException {

    	failHATest();

        final int nbuffers = 2;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.WORM, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {

            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();

        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 2 running services, two buffers
     * and the default #of records written and the default percentage of large
     * records.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_RW_2buffer_k3_size2()
            throws InterruptedException, IOException {

    	failHATest();

    	final int nbuffers = 2;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
//        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
//                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
//            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
//            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
//            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
//            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
//            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            assertEquals(2,quorum0.getJoined().length);

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.RW, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(2,quorum0.getJoined().length);

        } finally {

            quorum0.terminate();
            quorum1.terminate();
//            quorum2.terminate();
            fixture.terminate();

        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 3 running services, six buffers
     * and the default #of records written and the default percentage of large
     * records.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_WORM_2buffer_k3_size3()
            throws InterruptedException, IOException {

    	failHATest();

        final int nbuffers = 6;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            assertEquals(token,quorum2.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            AbstractQuorumTestCase.assertCondition(new Runnable() {
                public void run() {
                    assertEquals(3, quorum0.getJoined().length);
                    assertEquals(3, quorum1.getJoined().length);
                    assertEquals(3, quorum2.getJoined().length);
                }
            });

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.WORM, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(3,quorum0.getJoined().length);

        } finally {
            quorum0.terminate();
            quorum1.terminate();
            quorum2.terminate();
            fixture.terminate();
        }

    }

    /**
     * A test of the write pipeline driving from the {@link WriteCacheService}
     * of the leader using a quorum with k := 3, 3 running services, six buffers
     * and the default #of records written and the default percentage of large
     * records.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_writeCacheService_HA_RW_2buffer_k3_size3()
            throws InterruptedException, IOException {

    	failHATest();

        final int nbuffers = 6;
        final int nrecs = nrecsRW;
        /*
         * Note: The RW store breaks large records into multiple allocations,
         * each of which is LTE the size of the write cache so we do not test
         * with large records here.
         */
        final double largeRecordRate = 0d;
        final boolean useChecksums = true;
        // Note: This must be true for the write pipeline.
        final boolean isHighlyAvailable = true;

        final int k = 3;
        final long lastCommitTime = 0L;
        final MockQuorumFixture fixture = new MockQuorumFixture();
        final String logicalServiceId = "logicalService_"+getName();
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum0 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum1= new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        final MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>> quorum2 = new MockQuorum<HAPipelineGlue, MyMockQuorumMember<HAPipelineGlue>>(
                k, fixture);
        try {
            
            fixture.start();
            quorum0.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum1.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));
            quorum2.start(new MyMockQuorumMember<HAPipelineGlue>(fixture,logicalServiceId));

            final QuorumActor<?,?> actor0 = quorum0.getActor();
            final QuorumActor<?,?> actor1 = quorum1.getActor();
            final QuorumActor<?,?> actor2 = quorum2.getActor();

            actor0.memberAdd();
            actor1.memberAdd();
            actor2.memberAdd();
            fixture.awaitDeque();
            
            actor0.pipelineAdd();
            actor1.pipelineAdd();
            actor2.pipelineAdd();
            fixture.awaitDeque();
            
            // actor0 will become the leader.
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();
                        
            // note token and verify expected leader.
            final long token = quorum0.awaitQuorum();
            assertEquals(token,quorum1.awaitQuorum());
            assertEquals(token,quorum2.awaitQuorum());
            quorum0.assertLeader(token);

            // Verify the expected services joined.
            AbstractQuorumTestCase.assertCondition(new Runnable() {
                public void run() {
                    assertEquals(3, quorum0.getJoined().length);
                    assertEquals(3, quorum1.getJoined().length);
                    assertEquals(3, quorum2.getJoined().length);
                }
            });

            final long nsend = doStressTest(nbuffers, nrecs, maxreclen,
                    largeRecordRate, useChecksums, isHighlyAvailable,
                    StoreTypeEnum.RW, quorum0/* leader */);

            // Verify #of cache blocks received by the clients.
            assertEquals(nsend, quorum1.getClient().nreceived.get());

            // Verify still leader, same token.
            quorum0.assertLeader(token);
            
            // Verify the expected services still joined.
            assertEquals(3,quorum0.getJoined().length);
            
        } finally {
            quorum0.terminate();
            quorum1.terminate();
            quorum2.terminate();
            fixture.terminate();
        }

    }

    /**
     * A stress test for the {@link WriteCacheService}.
     * 
     * @param nbuffers
     *            The #of {@link WriteCache} buffers.
     * @param nrecs
     *            The #of records to write.
     * @param maxreclen
     *            The maximum length of a record.
     * @param largeRecordRate
     *            The rate in [0:1) of records which will be larger than the
     *            {@link WriteCache} buffer size.
     * @param useChecksums
     *            When <code>true</code>, use record-level checksums.
     * @param isHighlyAvailable
     *            When <code>true</code>, compute the running checksums of the
     *            {@link WriteCache} as a whole.
     * @return The #of write cache blocks send by the quorum leader to the first
     *         downstream follower.
     *         
     * @throws InterruptedException
     * @throws IOException
     * 
     *             FIXME Test with service leave/joins when the quorum is highly
     *             available. To do this we will have to pass in the quorum[]. A
     *             leader leave causes the quorum to break and is not really
     *             testable here since we do not have any infrastructure to
     *             handle this, but we can test when a follower leaves the
     *             quorum and when it joins. [However, handling a follower join
     *             requires us to synchronize the follower first and we can't
     *             really test that here either, so all we can really test would
     *             be a follower leave, either when the follower did not did not
     *             have a downstream node. If there is downstream node, then the
     *             upstream node from the left follower should reconfigure for
     *             the new downstream node and retransmit the current cache
     *             block and the event should be otherwise unnoticed.]
     */
    protected long doStressTest(final int nbuffers, final int nrecs,
            final int maxreclen, final double largeRecordRate,
            final boolean useChecksums, final boolean isHighlyAvailable,
            final StoreTypeEnum storeType, final Quorum quorum)
            throws InterruptedException, IOException {

        if (log.isInfoEnabled()) {
            log.info("\n====================================================\n"
                    + getName() + ": nbuffers=" + nbuffers + ", nrecs=" + nrecs
                    + ", maxreclen=" + maxreclen + ", largeRecordRate="
                    + largeRecordRate + ", useChecksums=" + useChecksums
                    + ", isHighlyAvailable=" + isHighlyAvailable);
        }
        
        File file = null;
        ReopenFileChannel opener = null;
        WriteCacheService writeCacheService = null;
        try {

            file = File.createTempFile(getName(), "." + storeType + ".tmp");

            opener = new ReopenFileChannel(file, "rw");
            
            final long fileExtent = opener.reopenChannel().size();
            
            writeCacheService = new WriteCacheService(nbuffers, useChecksums,
                    fileExtent, opener, quorum) {

                @Override
                public WriteCache newWriteCache(ByteBuffer buf,
                        boolean useChecksum, boolean bufferHasData,
                        IReopenChannel<? extends Channel> opener)
                        throws InterruptedException {

                    switch (storeType) {
                    case WORM:
                        return new FileChannelWriteCache(0/* baseOffset */,
                                buf, useChecksum, isHighlyAvailable,
                                bufferHasData,
                                (IReopenChannel<FileChannel>) opener);
                    case RW:
                        return new FileChannelScatteredWriteCache(buf,
                                useChecksum, isHighlyAvailable, bufferHasData,
                                (IReopenChannel<FileChannel>) opener, null);
                    default:
                        throw new UnsupportedOperationException();
                    }

                }

            };

            final MockRecord[] records;
            final long firstOffset = 0L;
            final long lastOffset;
            {

                // starting offset.
                long offset = firstOffset;

                // create a bunch of records.
                records = createMockRecords(offset, nrecs, maxreclen,
                        largeRecordRate, useChecksums);

                // the offset of the next possible record.
                offset += records[records.length - 1].offset
                        + records[records.length - 1].nbytes
                        + (useChecksums ? 4 : 0);

                lastOffset = offset;

            }
            
            /*
             * Randomize the record order for the RW mode.
             */
            if (storeType == StoreTypeEnum.RW) {

                final int order[] = getRandomOrder(nrecs);
                
                final MockRecord[] tmp = new MockRecord[nrecs];

                // create a random ordering of the records.
                for (int i = 0; i < nrecs; i++) {
                    tmp[i] = records[order[i]];
                }

                // replace with random ordering of the records.
                for (int i = 0; i < nrecs; i++) {
                    records[i] = tmp[i];
                }

            }

            /*
             * Pre-extend the file to the maximum length.
             * 
             * Note: This is done as a workaround for a JVM bug under at least
             * Windows where IOs with a concurrent file extend can lead to
             * corrupt data. In the WORMStrategy, we handle this using a
             * read/write lock to disallow IOs during a file extend. In this
             * unit test, we shortcut that logic by pre-extending the file.
             */
            {

                assertEquals("fileExtent", 0L, opener.reopenChannel().size());

                opener.truncate(lastOffset);
                
                assertEquals("fileExtent", lastOffset, opener.reopenChannel()
                        .size());
                
            }
            
            /*
             * Write the data onto the cache, which will incrementally write it
             * out onto the backing file.
             */
            // #of cache misses
            int nmiss = 0;
            int nlarge = 0;
            final ChecksumUtility checker = new ChecksumUtility();
            final long begin = System.nanoTime();
            for (int i = 0; i < records.length; i++) {

                final MockRecord rec = records[i];

                if (rec.nbytes > WRITE_CACHE_BUFFER_CAPACITY)
                    nlarge++;
                
                /*
                 * Write the record.
                 * 
                 * Note: The buffer is duplicated in order to prevent a
                 * side-effect on its position().
                 */

                writeCacheService.write(rec.offset, rec.data.asReadOnlyBuffer(),
                        rec.chksum);

                if(log.isTraceEnabled())
                    log.trace("wrote: i=" + i + ", rec=" + rec);

                {
                    // The index of some prior record.
                    final int prior = r.nextInt(i + 1);

                    final MockRecord expected = records[prior];

                    // Read on the cache.
                    ByteBuffer actual = writeCacheService.read(expected.offset);

                    final boolean cacheHit = actual != null;
                    if (actual == null) {
                        // Cache miss - read on the file.
                        nmiss++;
                        if (useChecksums) {
                            // read on file, including checksum field.
                            actual = opener.read(expected.offset,
                                    expected.nbytes + 4);
                            // get the checksum field.
                            final int chkOnDisk = actual
                                    .getInt(expected.nbytes);
                            // trim to the application data record.
                            actual.limit(expected.nbytes);
                            // verify on disk checksum against expected.
                            assertEquals("chkOnDisk", expected.chksum,
                                    chkOnDisk); 
                        } else {
                            // read on the file, no checksum field.
                            actual = opener.read(expected.offset,
                                    expected.nbytes);
                        }

                    }

                    final int actualChecksum = checker.checksum(actual);
                    
                    if(log.isTraceEnabled())
                        log.trace("read : i=" + i + ", prior=" + prior
                            + ", " + (cacheHit ? "hit" : "miss") + ", rec="
                            + expected + ", actualChecksum=" + actualChecksum
                            + ", actual=" + actual);

                    // Verify the data read from cache/disk.
                    assertEquals(expected.data, actual);

                }
                
            }

            // flush the write cache to the backing file.
            log.info("Service flush().");
            writeCacheService.flush(true/*force*/);
            log.info("Service flush() - done.");

            // verify the file size is as expected (we presize the file).
            assertEquals("fileExtent", lastOffset, opener.reopenChannel()
                    .size());
            
            // close the write cache.
            log.info("Service close().");
            // Thread.sleep(0);
            writeCacheService.close();

            // verify the file size is as expected (we presize the file).
            assertEquals("fileExtent", lastOffset, opener.reopenChannel()
                    .size());

            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System
                    .nanoTime()
                    - begin);

            if (log.isInfoEnabled()) {

                // data rate in MB/sec (counts read back as well).
                final double mbPerSec = (((double) lastOffset)
                        / Bytes.megabyte32 / (elapsed / 1000d));

                final NumberFormat fpf = NumberFormat.getNumberInstance();

                fpf.setGroupingUsed(false);

                fpf.setMaximumFractionDigits(2);

                log.info("#miss=" + nmiss + ", nrecs=" + nrecs + ", maxreclen="
                        + maxreclen + ", nlarge=" + nlarge + ", nbuffers="
                        + nbuffers + ", lastOffset=" + lastOffset
                        + ", mbPerSec=" + fpf.format(mbPerSec));
                
                log.info(writeCacheService.getCounters().toString());
                
            }
            
            /*
             * Verify the backing file against the mock data.
             */
            {

                for (int i = 0; i < nrecs; i++) {

                    final MockRecord expected = records[i];

                    final ByteBuffer actual = opener.read(expected.offset,
                            expected.nbytes);

                    final int actualChecksum = checker.checksum(actual);

                    if(log.isDebugEnabled())
                        log.debug("read : i=" + i + ", rec=" + expected
                            + ", actualChecksum=" + actualChecksum
                            + ", actual=" + actual);

                    // Verify the data read from cache/disk.
                    assertEquals(expected.data, actual);

                }

            }
            
            return writeCacheService.getSendCount();

        } finally {
            if (writeCacheService != null)
                try {
                    writeCacheService.close();
                } catch (InterruptedException e) {
                    log.error(e, e);
                }
            if (opener != null) {
                opener.destroy();
            }
        }
        
    }
    
    /*
     * Test helpers
     */
            
    /**
     * A random number generated - the seed is NOT fixed.
     */
    protected final Random r = new Random();

    /**
     * Returns random data that will fit in N bytes. N is chosen randomly in
     * 1:256.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
     *         random length and having random contents.
     */
    public ByteBuffer getRandomData() {

        final int nbytes = r.nextInt(256) + 1;

        return getRandomData(nbytes);

    }

    /**
     * Returns random data that will fit in <i>nbytes</i>.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
     *         having random contents.
     */
    public ByteBuffer getRandomData(final int nbytes) {

        final byte[] bytes = new byte[nbytes];

        r.nextBytes(bytes);

        return ByteBuffer.wrap(bytes);

    }

//    /**
//     * Simple implementation for unit tests.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     */
//    static private class WORMWriteCacheImpl extends
//            WriteCache.FileChannelWriteCache {
//
//        public WORMWriteCacheImpl(final long baseOffset, final ByteBuffer buf,
//                final boolean useChecksum,
//                final boolean isHighlyAvailable,
//                final boolean bufferHasData,
//                final IReopenChannel<FileChannel> opener)
//                throws InterruptedException {
//
//            super(baseOffset, buf, useChecksum, isHighlyAvailable,
//                    bufferHasData, opener);
//
//        }
//
//        @Override
//        protected boolean writeOnChannel(final ByteBuffer data,
//                final long firstOffset,
//                final Map<Long, RecordMetadata> recordMapIsIgnored,
//                final long nanos) throws InterruptedException, IOException {
//
//            final long begin = System.nanoTime();
//
//            final int dpos = data.position();
//
//            final int nbytes = data.remaining();
//
//            final int nwrites = FileChannelUtility.writeAll(opener, data,
//                    firstOffset);
//
//            final WriteCacheCounters counters = this.counters.get();
//            counters.nwrite += nwrites;
//            counters.bytesWritten += nbytes;
//            counters.elapsedWriteNanos += (System.nanoTime() - begin);
//
//            if (log.isInfoEnabled())
//                log.info("wroteOnDisk: dpos=" + dpos + ", nbytes=" + nbytes
//                        + ", firstOffset=" + firstOffset + ", nrecords="
//                        + recordMapIsIgnored.size());
//
//            return true;
//
//        }
//
//    }

    /**
     * An allocation to be written at some offset on a backing channel.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class MockRecord {
        
        /** The file offset at which the record will be written. */
        final long offset;

        /** The data (bytes from the position to the limit). */
        final ByteBuffer data;

        /**
         * The #of bytes in the application portion of the record (this DOES NOT
         * include the optional checksum field at the end of the record).
         */
        final int nbytes;
        
        /** The {@link Adler32} checksum of the data. */
        final int chksum;

        public String toString() {

            return getClass().getSimpleName() + "{offset=" + offset
                    + ",nbytes=" + nbytes + ",chksum=" + chksum + ",data="
                    + data + "}";
            
        }
        
        /**
         * 
         * @param offset
         *            The file offset.
         * @param data
         *            The data (bytes between the position and the limit).
         * @param chksum
         *            The checksum of the data.
         */
        public MockRecord(final long offset, final ByteBuffer data, final int chksum) {

            this.offset = offset;

            this.data = data.asReadOnlyBuffer();
            
            this.nbytes = data.limit();
            
            this.chksum = chksum;

        }

    }

    /**
     * Create and return an array of {@link MockRecord}s. The records will be
     * assigned to a dense region in the file, beginning with the given file
     * offset.
     * 
     * @param firstOffset
     *            The file offset of the first record.
     * @param nrecs
     *            The #of records to create.
     * @param maxreclen
     *            The maximum length of a record. Records will be in [1:m] bytes
     *            long and will contain random bytes.
     * @param largeRecordRate
     *            The rate in [0:1) of records which will be larger than the
     *            {@link WriteCache} buffer size.
     * @param useChecksums
     *            when <code>true</code> record level checksums are in use and
     *            the actual size of the record on the disk is 4 bytes larger
     *            than the application record size due to the checksum field
     *            stored at the end of the record.
     * 
     * @return The {@link MockRecord}s.
     */
    private MockRecord[] createMockRecords(final long firstOffset,
            final int nrecs, final int maxreclen, final double largeRecordRate,
            final boolean useChecksums) {

        final MockRecord[] a = new MockRecord[nrecs];

        long offset = firstOffset;

        final ChecksumUtility checker = new ChecksumUtility();

        for (int i = 0; i < nrecs; i++) {

            final int nbytes;
            final byte[] bytes;
            
            if (r.nextDouble() < largeRecordRate) {

                /*
                 * large record.
                 */
                nbytes = WRITE_CACHE_BUFFER_CAPACITY * (r.nextInt(3) + 1);

                bytes = new byte[nbytes];
    
            } else {

                nbytes = r.nextInt(maxreclen) + 1;

                bytes = new byte[nbytes];

            }

            r.nextBytes(bytes);

            // Create a record with random data for that offset.
            a[i] = new MockRecord(offset, ByteBuffer.wrap(bytes), checker
                    .checksum(bytes));

            // Update the current offset.
            offset += a[i].nbytes + (useChecksums ? 4 : 0);

        }

        return a;
        
    }

    /**
     * Simple implementation for a {@link RandomAccessFile} with hook for
     * deleting the test file.
     */
    private class ReopenFileChannel implements IReopenChannel<FileChannel> {

        final private File file;

        private final String mode;

        private volatile RandomAccessFile raf;

        private AtomicInteger nrepen = new AtomicInteger();
        
        public ReopenFileChannel(final File file, final String mode)
                throws IOException {

            this.file = file;

            this.mode = mode;

            reopenChannel();

        }

        public String toString() {

            return file.toString();

        }

        public void truncate(final long extent) throws IOException {

            reopenChannel();

            raf.setLength(extent);
            
            raf.getChannel().force(true/* metaData */);

        }

        /**
         * Hook used by the unit tests to destroy their test files.
         */
        public void destroy() {
            try {
                raf.close();
            } catch (IOException e) {
                log.error(e, e);
            }
            if (!file.delete())
                log.warn("Could not delete file: " + file);
        }

        /**
         * Read some data out of the file.
         * 
         * @param off
         *            The offset of the record.
         * @param nbytes
         *            The #of bytes to be read.
         * @return The record.
         */
        public ByteBuffer read(final long off, final int nbytes)
                throws IOException {

            final ByteBuffer tmp = ByteBuffer.allocate(nbytes);
            
            FileChannelUtility.readAll(this, tmp, off);
            
            // flip for reading.
            tmp.flip();
            
            return tmp;
            
        }
        
        synchronized public FileChannel reopenChannel() throws IOException {

            if (raf != null && raf.getChannel().isOpen()) {

                /*
                 * The channel is still open. If you are allowing concurrent
                 * reads on the channel, then this could indicate that two
                 * readers each found the channel closed and that one was able
                 * to re-open the channel before the other such that the channel
                 * was open again by the time the 2nd reader got here.
                 */

                return raf.getChannel();

            }

            // open the file.
            this.raf = new RandomAccessFile(file, mode);

            /*
             * @todo As far as I can tell, this test suite should not cause the
             * reopener to run more than once (in the constructor). See the note
             * at the top of the class concerning a ClosedByInterruptException
             * in FileChannelUtility#writeAll(). This may be enabled to see the
             * reopen of the file channel by the reader, but that does not
             * really add much information since we do not know who is
             * generating the interrupt. However, you can sometimes see that the
             * file is reopened more than once.
             */
            final int nreopen = nrepen.incrementAndGet();
            if (nreopen > 1 && false) {
                log.error("(Re-)opened file: " + file, new RuntimeException(
                        "nreopen=" + nreopen + ", test=" + getName()));
            }

            return raf.getChannel();

        }

    };

    /*
     * HA pipeline tests.
     */

    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    private static int getPort(final int suggestedPort) throws IOException {

        ServerSocket openSocket;
        try {
            openSocket = new ServerSocket(suggestedPort);
        } catch (BindException ex) {
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        }

        final int port = openSocket.getLocalPort();

        openSocket.close();

        if (suggestedPort != 0 && port != suggestedPort) {

            log.warn("suggestedPort is busy: suggestedPort=" + suggestedPort
                    + ", using port=" + port + " instead");

        }

        return port;

    }

}
