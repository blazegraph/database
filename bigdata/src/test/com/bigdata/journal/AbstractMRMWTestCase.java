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
 * Created on Feb 20, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IMRMW;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for MRMW (Multiple Readers, Multiple Writers) support.
 * <p>
 * Supporting MRMW is easy for a fully buffered implementation since it need
 * only use a read-only view for readers and serialize the assignment of
 * addresses to written records. If the implementation is not fully buffered,
 * e.g., {@link DiskOnlyStrategy}, then it needs to serialize reads that are
 * not buffered. The exception as always is the {@link MappedBufferStrategy} -
 * since this uses the nio {@link MappedByteBuffer} it supports concurrent
 * readers using the same approach as a fully buffered strategy even though data
 * may not always reside in memory.
 * 
 * @todo Support {@link ExperimentDriver} (also in {@link AbstractMROWTestCase})
 * 
 * @see IMRMW
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractMRMWTestCase
    extends AbstractBufferStrategyTestCase
    implements IComparisonTest
    {

    /**
     * 
     */
    public AbstractMRMWTestCase() {
    }

    /**
     * @param name
     */
    public AbstractMRMWTestCase(String name) {

        super(name);
        
    }

    /**
     * Additional properties understood by this test.
     */
    public static class TestOptions extends Options {

        /**
         * The timeout for the test.
         */
        public static final String TIMEOUT = "timeout";

        /**
         * The #of trials to run.
         */
        public static final String NTRIALS = "ntrials";
        
        /**
         * The #of concurrent clients to run. 20 is a reasonable value for
         * testing concurrency. 1 means one reader or writer (you can decide
         * which it will be using {@link #PERCENT_READERS}.
         */
        public static final String NCLIENTS = "nclients";

        /**
         * The percentage of trials that are tasked as {@link ReaderTask}s
         * rather than {@link WriterTask}s. Zero (0.0) means only writers. One
         * (1.0) means only readers. .8d is a common mixure reflecting that
         * there are typically more read operations than write operations.
         * <p>
         * Note: There is NO validation of correctness unless you are running
         * BOTH writers and readers!
         */
        public static final String PERCENT_READERS = "percentReaders";
        
        /**
         * The maximum length of the records used in the test.
         */
        public static final String RECLEN = "reclen";

        /**
         * The #of write operations in each trial that is elected to be a
         * {@link WriterTask}.
         */
        public static final String NWRITES = "nwrites";

        /**
         * The #of read operations in each trial that is elected to be a
         * {@link ReaderTask}.
         */
        public static final String NREADS = "nreads";
    
    }

    /**
     * Setup and run a test.
     * 
     * @param properties
     *            There are no "optional" properties - you must make sure that
     *            each property has a defined value.
     */
    public Result doComparisonTest(Properties properties) throws Exception {

        final long timeout = Long.parseLong(properties.getProperty(TestOptions.TIMEOUT));

        final int ntrials = Integer.parseInt(properties.getProperty(TestOptions.NTRIALS));

        final int nclients = Integer.parseInt(properties.getProperty(TestOptions.NCLIENTS));

        final double percentReaders = Double.parseDouble(properties.getProperty(TestOptions.PERCENT_READERS));
        
        final int reclen = Integer.parseInt(properties.getProperty(TestOptions.RECLEN));

        final int nwritesPerTask = Integer.parseInt(properties.getProperty(TestOptions.NWRITES));
        
        final int nreadsPerTask = Integer.parseInt(properties.getProperty(TestOptions.NREADS));

        Journal journal = new Journal(properties);

        Result result = doMRMWTest(journal, timeout, ntrials, nclients,
                percentReaders, reclen, nwritesPerTask, nreadsPerTask);

        return result;

    }

    public void setUpComparisonTest() throws Exception {
        
    }

    public void tearDownComparisonTest() throws Exception {
        
    }

    /**
     * Correctness/stress test verifies that the implementation supports
     * Multiple Readers, Multiple Writers
     */
    public void testMRMW() throws Exception {

        IBufferStrategy store = ((Journal)getStore()).getBufferStrategy();

        final long timeout = 5;
        
        final int ntrials = 10000;

        final int nclients = 20;
        
        final double percentReaders = .7d;
        
        final int reclen = 128;
        
        final int nwrites = 100;

        final int nreads = 100;
        
        doMRMWTest(store, timeout, ntrials, nclients, percentReaders, reclen, nwrites, nreads);
        
    }

    /**
     * A correctness/stress/performance test with a pool of concurrent clients
     * designed to verify MRMW operations. If the store passes these tests, then
     * {@link StressTestConcurrent} is designed to reveal concurrency problems
     * in the higher level data structures (transaction process and especially
     * the indices).
     * 
     * @param store
     *            The store.
     * 
     * @param timeout
     *            The timeout (seconds).
     * 
     * @param ntrials
     *            The #of distinct client trials to execute.
     * 
     * @param nclients
     *            The #of concurrent clients. Each client will be either a
     *            {@link WriterTask} or a {@link ReaderTask}.
     * 
     * @param percentReaders
     *            The percent of the clients that will be readers (vs writers)
     *            [0.0:1.0]. When <code>1.0</code>, only readers will be
     *            created. When <code>0.0</code> only writers will be created.
     * 
     * @param reclen
     *            The length of the random byte[] records used in the
     *            operations.
     * 
     * @param nwritesPerTask
     *            The #of records to write per {@link WriterTask}.
     * 
     * @param nreadsPerTask
     *            The #of records to read per {@link ReaderTask}.
     */
    static public Result doMRMWTest(IRawStore store, long timeout, int ntrials,
            int nclients, double percentReaders, int reclen,
            int nwritesPerTask, int nreadsPerTask) throws Exception {

        if (percentReaders < 0 || percentReaders > 1)
            throw new IllegalArgumentException();
        
        // Provides ground truth for the store under test.
        GroundTruth groundTruth = new GroundTruth();
        
        /*
         * Pre-write 5000 records so that readers have something to choose from
         * when they start reading.
         */
        {

            final int nprewrites = 5000;
            
            new WriterTask(groundTruth, store, reclen, nprewrites).call();
            
            System.err.println("Pre-wrote " + nprewrites + " records");
            
        }
        
        // Used to execute concurrent clients.
        ExecutorService clientService = Executors.newFixedThreadPool(
                nclients, DaemonThreadFactory.defaultThreadFactory());

        // Setup client task queue.
        Collection<Callable<Integer>> tasks = new LinkedList<Callable<Integer>>(); 
        
        {
            
            Random r = new Random();

            int nreaders = 0;
            int nwriters = 0;
            
            for (int i = 0; i < ntrials; i++) {

                final Callable<Integer> task;
                
                if (r.nextDouble() < percentReaders) {

                    task = new ReaderTask(groundTruth, store, nreadsPerTask);
                    
                    nreaders++;
                    
                } else {
                    
                    task = new WriterTask(groundTruth, store, reclen, nwritesPerTask);
                    
                    nwriters++;
                    
                }
                    
                tasks.add( task );

            }
            
            
            // used to format percentages.
            final NumberFormat percentFormat = NumberFormat.getPercentInstance();

            System.err.println("#clients=" + nclients + ", #readers="
                    + nreaders + ", #writers=" + nwriters + ", readerToWriterRatio="
                    + percentFormat.format(((double) nreaders / ntrials )));
            
        }
        
        /*
         * Run M trials for N clients.
         */
        
        final long begin = System.currentTimeMillis();
        
        // start clients.
        
        System.err.println("Starting clients.");
        
        List<Future<Integer>> results = clientService.invokeAll(tasks, timeout,
                TimeUnit.SECONDS);

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("Halting clients.");
        
        // force the clients to terminate.
        clientService.shutdownNow();

        if (!clientService.awaitTermination(1, TimeUnit.SECONDS)) {

            /*
             * Note: if readers do not terminate within the timeout then an
             * IOException MAY be reported by disk-backed stores if the store is
             * closed while readers are still attempting to resolve records on
             * disk.
             * 
             * See FileChannel#write(ByteBuffer,long).
             * 
             * One consequence is that the channel is NO LONGER AVAILABLE as it
             * was closed when a write() on the channel was interrupted!
             */
            System.err.println("Some clients(s) did not terminate.");

        } else {
        
            System.err.println("Clients halted.");
            
        }
        
        // #of records actually written.
        final int nwritten = groundTruth.getRecordCount();

        // #of records that were verified.
        final int nverified = groundTruth.getVerifiedCount();
        
        Iterator<Future<Integer>> itr = results.iterator();
        
        int nsuccess = 0; // #of trials that successfully committed.
        int ncancelled = 0; // #of trials that did not complete in time.
        int nerr = 0;
        Throwable[] errors = new Throwable[ntrials];
        
        while(itr.hasNext()) {

            Future<Integer> future = itr.next();
            
            if(future.isCancelled()) {
                
                ncancelled++;
                
                continue;
                
            }

            try {

                future.get(); // ignore the return (#of records read/written).
                
                nsuccess++;
                
            } catch(ExecutionException ex ) {

                Throwable cause = ex.getCause();
                
                if (cause != null
                        && cause.getCause() != null
                        && cause.getCause().getCause() != null
                        && cause.getCause().getCause() instanceof ClosedChannelException) {

                    /*
                     * Note: This is not an error. It is just the behavior of
                     * the channel when we cancelled the running tasks.
                     */
                    
                    continue;
                    
                }
                
                if(nerr<10) {
                    
                    // show the first N stack traces.
                    ex.printStackTrace(System.err);
                    
                } else if(nerr<500) {

                    System.err.println("Not expecting: "+ex);

                } else {
                    
                    System.err.print('X');
                    
                }
                
                errors[nerr++] = cause;
                
            }
            
        }

        // used to format bytes.
        final NumberFormat bytesFormat = NumberFormat.getNumberInstance();
        bytesFormat.setGroupingUsed(true);

        long seconds = TimeUnit.SECONDS.convert(elapsed, TimeUnit.MILLISECONDS);
        
        long bytesWrittenPerSecond = groundTruth.bytesWritten.get()
                / (seconds == 0 ? 1 : seconds);

        long bytesVerifiedPerSecond = groundTruth.bytesVerified.get()
                / (seconds == 0 ? 1 : seconds);
        
//        System.err.println("#clients=" + nclients + ", ntrials=" + ntrials
//                + ", nsuccess=" + nsuccess + ", ncancelled=" + ncancelled
//                + ", nerrors=" + nerr + ", elapsed=" + elapsed
//                + "ms");
//        
//        System.err.println("nwritten=" + nwritten + ", bytesWritten="
//                + bytesFormat.format(groundTruth.bytesWritten.get()) + ", bytes/sec="
//                + bytesFormat.format(bytesWrittenPerSecond));
//
//        System.err.println("nverified=" + nverified + ", bytesVerified="
//                + bytesFormat.format(groundTruth.bytesVerified.get()) + ", bytes/sec="
//                + bytesFormat.format(bytesVerifiedPerSecond));
        
        Result ret = new Result();
        
        // these are the results.
        ret.put("nsuccess",""+nsuccess);
        ret.put("ncancelled",""+ncancelled);
        ret.put("nerrors", ""+nerr);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("nwritten", ""+nwritten);
        ret.put("bytesWritten", ""+groundTruth.bytesWritten.get());
        ret.put("bytesWrittenPerSec", ""+bytesWrittenPerSecond);
        ret.put("nread", ""+nverified);
        ret.put("bytesVerified", ""+groundTruth.bytesVerified.get());
        ret.put("bytesVerifiedPerSec", ""+bytesVerifiedPerSecond);
        
        System.err.println(ret.toString(true/*newline*/));
        
        store.closeAndDelete();

        return ret;

    }

    /**
     * A ground truth record as generated by a {@link WriterTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Record {
        public final long addr;
        public final byte[] data;
        public Record(long addr, byte[] data) {
            assert addr != 0L;
            assert data != null;
            this.addr = addr;
            this.data = data;
        }
    };

    /**
     * Class maintains ground truth for the store. Each {@link WriterTask}
     * stores the {@link Record}s that it writes here. The {@link ReaderTask}s
     * select and verify random ground truth {@link Record}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class GroundTruth {
        
        final Random r = new Random();

        /**
         * The ground truth data written so far.
         * 
         * Note: {@link Vector} is thread-safe.
         */
        private Vector<Record> records = new Vector<Record>(10000,10000);
        
        private AtomicLong bytesWritten = new AtomicLong(0L);
        
        private AtomicLong bytesVerified = new AtomicLong(0L);
        
        private AtomicInteger nverified = new AtomicInteger(0);
        
        /**
         * The #of records written so far.
         */
        public int getRecordCount() {
            
            return records.size();
            
        }

        public long getBytesWritten() {
        
            return bytesWritten.get();
            
        }
        
        public long getBytesVerified() {
            
            return bytesVerified.get();
            
        }

        public int getVerifiedCount() {
            
            return nverified.get();
            
        }
        
        /**
         * Used by a {@link ReaderTask} to notify us that it has verified
         * a ground truth record.
         */
        public void verifiedRecord(Record record) {
            
            nverified.incrementAndGet();
            
            bytesVerified.addAndGet(record.data.length);
            
        }
        
        /**
         * Records a new ground truth record.
         * 
         * @param record
         *            The record.
         */
        public void add(Record record) {

            records.add(record);
         
            bytesWritten.addAndGet(record.data.length);
            
        }
        
        /**
         * Return a randomly choosen ground truth record. 
         */
        public Record getRandomGroundTruthRecord() {

            if(r.nextInt(100)>95) {
            
                /*
                 * 5% of the time we choose the most recently written record to
                 * see if we can trip up read back on a record that might not be
                 * "fully" written and recoverable yet.
                 */
                
                return records.lastElement();
                
            }
            
            int index = r.nextInt(records.size());
            
            return records.get( index );
            
        }

        /**
         * Returns random data that will fit in N bytes. N is choosen randomly in
         * 1:<i>reclen</i>.
         * 
         * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code> of
         *         random length and having random contents.
         */
        public ByteBuffer getRandomData(int reclen) {
            
            final int nbytes = r.nextInt(reclen) + 1;
            
            byte[] bytes = new byte[nbytes];
            
            r.nextBytes(bytes);
            
            return ByteBuffer.wrap(bytes);
            
        }
        
    }
    
    /**
     * Run a writer.
     * <p>
     * The writer uses a {@link GroundTruth} object to expose state to the
     * readers so that they can perform reads on written records and so that the
     * can validate those reads against ground truth.
     */
    public static class WriterTask implements Callable<Integer> {

        private final GroundTruth groundTruth;
        private final IRawStore store;
        private final int reclen;
        private final int nwrites;

        /**
         * 
         * @param groundTruth
         *            Used to store ground truth for written records.
         * @param store
         *            The store on which records will be written.
         * @param reclen
         *            The maximum record length to write - random length records
         *            of up to this length will be written.
         * @param nwrites
         *            The #of records to write.
         */
        public WriterTask(GroundTruth groundTruth, IRawStore store,
                int reclen, int nwrites) {

            this.groundTruth = groundTruth;
            
            this.store = store;
            
            this.reclen = reclen;
            
            this.nwrites = nwrites;
            
        }

        /**
         * Writes N records.
         * 
         * @return The #of records written.
         */
        public Integer call() throws Exception {

            try {

                for (int i = 0; i < nwrites; i++) {

                    write();
                
                }
                
            } catch(Throwable t) {
                
                log.warn(t.getMessage());

                throw new RuntimeException( t );
                
            }
            
            return nwrites;
        
        }
        
        /**
         * Write a random record and record it in {@link #records}.
         */
        public void write() {

            ByteBuffer data = groundTruth.getRandomData(reclen);
            
            final long addr = store.write(data);

            groundTruth.add(new Record(addr, data.array()));

        }
        
    }
    
    /**
     * Run a reader. The reader will verify {@link GroundTruth} records already
     * written on the backing store.
     */
    public static class ReaderTask implements Callable<Integer> {

        private final GroundTruth groundTruth;
        private final IRawStore store;
        private final int nreads;
        
        final Random r = new Random();
        
        /**
         * 
         * @param groundTruth The ground truth records.
         * @param store The backing store.
         * @param nwrites #of reads to perform.
         */
        public ReaderTask(GroundTruth groundTruth, IRawStore store, int nreads) {

            this.groundTruth = groundTruth;
            
            this.store = store;
            
            this.nreads = nreads;
            
        }

        /**
         * Executes random reads and validates against ground truth.
         * 
         * @return The #of records read and validated.
         */
        public Integer call() throws Exception {
            
            // Random reads.

            int nverified = 0;

            try {

                for (int i = 0; i < nreads; i++) {

                    if(read()) {

                        nverified++;
                    
                    }
                
                }
            
            } catch(Throwable t) {
                
                log.warn(t.getMessage());
                
                throw new RuntimeException( t );
                
            }

            return nverified;
            
        }
        
        /**
         * Read and verify a random record.
         * 
         * @return
         */
        public boolean read() {
        
            Record record = groundTruth.getRandomGroundTruthRecord();

            ByteBuffer buf;
            
            try {

                if (r.nextInt(100) > 30) {

                    buf = store.read(record.addr);

                } else {

                    buf = ByteBuffer.allocate(store
                            .getByteCount(record.addr));

                    buf = store.read(record.addr);

                }
                
            } catch (IllegalArgumentException ex) {

                System.err.println("Could not read: "
                        + store.toString(record.addr) + ": cause=" + ex);

                throw ex;

            }
            
            assertEquals(record.data, buf);
            
            groundTruth.verifiedRecord(record);

            return true;
        
        }
        
    }

    /**
     * Correctness/stress/performance test for MRMW behavior.
     */
    public static void main(String[] args) throws Exception {
                
        Properties properties = new Properties();
        
        // timeout in seconds.
        properties.setProperty(TestOptions.TIMEOUT,"10");
        
        // You must increase the timeout to do 100k trials.
        properties.setProperty(TestOptions.NTRIALS, "100000");

        properties.setProperty(TestOptions.NCLIENTS, "20");
        
        properties.setProperty(TestOptions.PERCENT_READERS,".8");

        properties.setProperty(TestOptions.RECLEN, "1024");

        properties.setProperty(TestOptions.NWRITES,"100");

        properties.setProperty(TestOptions.NREADS,"100");
        
//      properties.setProperty(Options.USE_DIRECT_BUFFERS,"true");
        
//      properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());

//      properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

//      properties.setProperty(Options.BUFFER_MODE, BufferMode.Mapped.toString());
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        new StressTestMRMW().doComparisonTest(properties);

    }

    /**
     * Concrete instance for running stress tests and comparisons.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StressTestMRMW extends AbstractMRMWTestCase {

        protected BufferMode getBufferMode() {
        
            throw new UnsupportedOperationException();
            
        }
            
    }
    
    /**
     * Experiment generation utility class.
     * <p>
     * The follow result summary is for 20 clients with 80% read tasks
     * (9/27/07). The timeout was 30 and the store files grew up to ~160M (for
     * transient).
     * </p>
     * 
     * <pre>
     *   bufferMode   readBytes/s writeBytes/s    read    write
     *   Transient     22,372,313  5,666,728 
     *   Direct        18,560,143  4,823,312      83%      85%
     *   Disk          11,315,786  3,001,315      51%      53%
     * </pre>
     * 
     * <p>
     * The big puzzle here is why Disk is so much slower than Direct. You can
     * see that Direct runs at 83% (reads) or 85% (writes) of Transient while
     * Disk runs at a significantly slow percentage of the Transient
     * performance. This is odd since both methods are writing on the disk! If
     * reads are dragging down performance, then maybe read cache for the
     * {@link DiskOnlyStrategy} would be useful? Note that this class uses
     * random selection of addresses to read back so this will defeat any cache
     * once the #of bytes/records written grows large enough.
     * </p>
     * 
     * @todo Compare the results above with the performance of index writes
     *       using a suitable stress test or application. The question is
     *       whether the slower performance of the Disk mode is reflected there
     *       as well. There are various reasons why it might not, including
     *       memory usage, heap churn, and access patterns all of which are
     *       different once indices are introduced. For example, this test does
     *       random reads but indices buffer nodes and leaves and pay a high
     *       cost for (de-)serialization. Also, less available memory means more
     *       frequent garbage collection and the indices do more object
     *       allocation (measure the heap churn for both this test and one with
     *       indices to get a sense of this and see if there are ways to reduce
     *       the allocation for the indices and thereby reduce the need to do
     *       GCs).
     * 
     * @todo we do not need to test forceOnCommit here since we are never doing
     *       a commit during this stress test. However, useDirectBuffers is an
     *       interesting variable that can be substituted. Change this by
     *       further refactoring of the generator classes to support better
     *       parameterization.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class GenerateExperiment extends ExperimentDriver {
        
        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(String[] args) throws Exception {
            
            // this is the test to be run.
            String className = StressTestMRMW.class.getName();
            
            Map<String,String> defaultProperties = new HashMap<String,String>();

            // force delete of the files on close of the journal under test.
            defaultProperties.put(Options.CREATE_TEMP_FILE,"true");

            // avoids journal overflow when running out to 60 seconds.
            defaultProperties.put(Options.MAXIMUM_EXTENT, ""+Bytes.megabyte32*400);

            /* 
             * Set defaults for each condition.
             */
            
            defaultProperties.put(TestOptions.TIMEOUT,"30");

            defaultProperties.put(TestOptions.NTRIALS,"100000");

//            defaultProperties.put(TestOptions.NCLIENTS, "20");
            
            defaultProperties.put(TestOptions.PERCENT_READERS,".8");

            defaultProperties.put(TestOptions.RECLEN, "1024");

            defaultProperties.put(TestOptions.NWRITES,"100");

            defaultProperties.put(TestOptions.NREADS,"100");
            
            List<Condition>conditions = new ArrayList<Condition>();

//            conditions.addAll(BasicExperimentConditions.getBasicConditions(
//                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
//                            "1") }));
//
//            conditions.addAll(BasicExperimentConditions.getBasicConditions(
//                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
//                            "2") }));
//
//            conditions.addAll(BasicExperimentConditions.getBasicConditions(
//                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
//                            "10") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "20") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "20"), new NV(TestOptions.PERCENT_READERS,"0.0"),
                            new NV(TestOptions.TIMEOUT,"10") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "20"), new NV(TestOptions.PERCENT_READERS,"1.0"),
                            new NV(TestOptions.TIMEOUT,"10")}));

//            conditions.addAll(BasicExperimentConditions.getBasicConditions(
//                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
//                            "100") }));
//
//            conditions.addAll(BasicExperimentConditions.getBasicConditions(
//                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
//                            "200") }));
            
            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }
        
    }
    
}
