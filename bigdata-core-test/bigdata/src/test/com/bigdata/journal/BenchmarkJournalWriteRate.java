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
 * Created on Oct 15, 2006
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Properties;
import java.util.UUID;

import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestSuite;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.testutil.ExperimentDriver;
import com.bigdata.util.Bytes;

/**
 * <p>
 * A benchmark that computes the raw platform write rate for pure sequential IO,
 * the journal's write rate, and both unisolated and isolated index write rates.
 * The overhead of the journal can be estimated by comparing the pure sequential
 * writes on the file system with the write rates when using each of the journal
 * modes. Likewise, the overhead of the index can be estimated in comparison to
 * the journal write rate, and the overehead of (large) transactions can be
 * estimated in comparison to the unisolated index write rate (this test does
 * not estimate the overhead for small transactions for a variety of reasons).
 * </p>
 * <p>
 * The results from this benchmark can be used to compare the performance of the
 * different {@link IBufferMode} implementations. The theoretical maximum for a
 * platform is the sustained write rate for the disk subsystem on which the
 * journal file is located - this can be obtained with a bit of research on your
 * disk drives, e.g., using <a href="http://www.storagereview.com/">
 * storagereview.com </a>. It is generally achieved by
 * {@link BenchmarkBlockBasedOptimium}
 * </p>
 * <p>
 * Note: you should run these tests multiple times to make sure that you are
 * getting valid numbers for your platform. You should also compare the data
 * with the expected disk maximum write rate for your platform. You can monitor
 * your platform using "perfmon" on Windows or "vmstat" on Un*x. It is important
 * that your system has not swapped out parts of the JVM or the benchmark will
 * run poorly (this can be a problem with a memory-limited Windows platform).
 * </p>
 * <p>
 * Analysis: The Transient mode achieves 30x the raw write rate when compared to
 * any of the disk-backed modes (1,282 MB/sec vs 40 MB/sec). However, the index
 * write rates are essentially constant across the buffer modes (roughly
 * 20MB/sec for unisolated writes, which is ~50% of the journal write rate when
 * backed by disk, and 9MB/sec for isolated writes, or ~25% of the journal write
 * rate when backed by disk). The limiting factor for index writes is the btree
 * code itself (it tends to be key search). The limiting factor for the isolated
 * index writes is that the transaction write set overflows onto disk, so we
 * wind up doing much more IO for a large transaction (however small
 * transactions have very little overhead when compared to unisolated index
 * writes). The disk-only buffer mode does a little better than the
 * fully-buffered modes for the isolated writes - presumably since (a) the
 * monotonically increasing keys defeat the index node and leaf cache; and (b)
 * the disk-only mode is able to make more RAM available to the JVM since it
 * does not maintain the large in memory buffer.
 * </p>
 * 
 * @see src/architecture/performance.xls.
 * 
 * @todo Test the impact of an AIO strategy.
 * 
 * FIXME Use the {@link ExperimentDriver} and compare the various buffer modes
 * and other variables and the write rates for the {@link IRawStore} vs
 * unisolated indices. Checkout the disk queue under the performance monitor and
 * make sure that we are driving the disk as hard as possible.
 * 
 * @todo Quantify impact of the disk-only mode write cache.
 * 
 * @todo Note that the target performance environment requires multiple journals
 *       and multiple read-optimized databases. Do not over-optimize for a
 *       single writer. Write benchmarks for write absorption rates for
 *       concurrent writers with and without concurrent readers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BenchmarkJournalWriteRate extends TestCase2 {

    /**
     * 
     */
    public BenchmarkJournalWriteRate() {
    }

    /**
     * @param name
     */
    public BenchmarkJournalWriteRate(String name) {
        super(name);
    }

    /**
     * Sets the initial extent for the test.
     */
    public Properties getProperties() {
        
        Properties properties = super.getProperties();

        properties.setProperty(Options.INITIAL_EXTENT,""+getInitialExtent());

        properties.setProperty(Options.BUFFER_MODE, getBufferMode().toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        // Note: Forcing writes is generally MUCH slower. 
//        properties.setProperty(Options.FORCE_WRITES, ForceEnum.ForceMetadata.toString());

        properties.setProperty(Options.DELETE_ON_EXIT,"true");
        
        return properties;
        
    }

    /**
     * The tests are performed with a record size of 4k, but you can vary the
     * "record size" to be larger or smaller. 4k was choosen as being
     * representative of the expected size of a node or leaf of a btree since
     * that is the primary kind of object that we write on the journal.
     */
    protected int getRecordSize() {
    
        return Bytes.kilobyte32 * 4;
        
    }

    /**
     * The tests are performed with a 100M journal.
     */
    protected long getInitialExtent() {
        
        return 100*Bytes.megabyte;
        
    }

    abstract protected BufferMode getBufferMode();
    
    protected String getFilename() {
    
        return "benchmark-"+getBufferMode()+"-"+getName()+".jnl";
        
    }
    
    /**
     * The branching factor used by the unisolated btree on the journal and by
     * the isolated btree iff a transaction is used to isolated the write set.
     * <p>
     * Note: A higher branching factor can be choosen for this test since the
     * btree writes use monotoically increasing keys.
     */
    protected int getBranchingFactor() {
        
//        return 16;
        return 256;
        
    }
    
    Journal journal;
    
    void deleteFile() {
        
        try {
            
            File file = new File(getFilename());
            
            if ( file.exists() && ! file.delete()) {
                
                System.err.println("Warning: could not delete: " + file.getAbsolutePath());
                
            }
            
        } catch (Throwable t) {
            
            System.err.println("Warning: " + t);
            
        }

    }
    
    public void setUp() throws IOException {
        
        System.err.println("------------------\n");
        
        deleteFile();
        
        journal = new Journal( getProperties() );
        
    }
    
    public void tearDown() throws IOException {
        
        try {

            journal.destroy();
            
        }

        catch( IllegalStateException ex ) {
            
            System.err.println("Could not close the journal: "+ex);
            
        }
        
    }

    static NumberFormat cf;
    static NumberFormat fpf;
    
    static {

        cf = NumberFormat.getNumberInstance();

        cf.setGroupingUsed(true);

        fpf = NumberFormat.getNumberInstance();

        fpf.setGroupingUsed(false);

        fpf.setMaximumFractionDigits(2);

    }

    public void testRawRecordWriteRate() throws IOException {

        doRawRecordWriteRateTest(getRecordSize());

    }
    
    /**
     * Test the index write rate using an index that does NOT support
     * transactional isolation using 32 bit integer keys and 128 byte values for
     * the index entries.
     */
    public void testNonIsolatableIndexWriteRate() throws IOException {
        
        // register named index that does NOT support isolation.
        String name = "abc";

        final BTree btree;
        {
            
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(getBranchingFactor());

            btree = BTree.create(journal, metadata);
            
        }
        
        journal.registerIndex(name, btree);

        journal.commit();

        // NOT isolated.
        long tx = 0L;

        // run test.
        doIndexWriteRateTest(name, tx, 128);
        
    }

    /**
     * Test the index write rate using an index that supports transactional
     * isolation but without transactional isolation using 32 bit integer keys
     * and 128 byte values for the index entries.
     */
    public void testUnisolatedIndexWriteRate() throws IOException {
        
        // register named index that can support isolation.
        String name = "abc";

        final BTree btree;
        {
            
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(getBranchingFactor());

            metadata.setIsolatable(true);
            
            btree = BTree.create(journal, metadata);
            
        }

        journal.registerIndex(name, btree );

        journal.commit();

        // NOT isolated.
        long tx = 0L;

        // run test.
        doIndexWriteRateTest(name, tx, 128);

    }

    /**
     * Test the index write rate for a fully isolated transaction using 32 bit
     * integer keys and 128 byte values for the index entries.
     */
    public void testIsolatedIndexWriteRate() throws IOException {

        // register named index that can support isolation.
        String name = "abc";

        final BTree btree;
        {
            
            IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBranchingFactor(getBranchingFactor());

            metadata.setIsolatable(true);
            
            btree = BTree.create(journal, metadata);
            
        }

        journal.registerIndex(name, btree );

        journal.commit();

        // fully isolated transaction.
        long tx = journal.newTx(ITx.UNISOLATED);

        // run test.
        doIndexWriteRateTest(name, tx, 128);
        
    }
    
    /**
     * Writes N records of the given size such that the journal will be filled
     * to capacity using the {@link IRawStore} interface (unisolated raw writes
     * not using an index).
     * 
     * @param writeSize
     *            The size of the object to be written.
     * 
     * @return The elapsed time for the test.
     */
    public long doRawRecordWriteRateTest(int writeSize) {

        Journal store = journal;
        
        System.err.println("Begin: bufferMode="+journal.getBufferStrategy().getBufferMode());

        final long begin = System.currentTimeMillis();
        
        final int nwrites = (int) journal.getBufferStrategy().getUserExtent()
                / writeSize;
        
        System.err.println("writeSize=" + writeSize + ", nwrites=" + nwrites);
        
        // the buffer is reused on each write. 
        ByteBuffer data = ByteBuffer.allocate/*Direct*/(writeSize);

        for( int i=0; i<nwrites; i++ ) {

            data.put(0,(byte)i); // at least one non-zero byte.

            data.position( 0 );
            
            data.limit( writeSize );
            
            store.write(data);
            
        }

        final long elapsed = System.currentTimeMillis() - begin;
        
        final long bytesWritten = journal.getBufferStrategy().getNextOffset();
        
        // MB/sec.
        final double writeRate = (bytesWritten/(double)Bytes.megabyte) / (elapsed / 1000d);
        
        System.err.println("Elapsed: " + elapsed + "(ms), bufferMode="
                + journal.getBufferStrategy().getBufferMode() + ", recordSize="
                + cf.format(writeSize) + ", nwrites=" + cf.format(nwrites)
                + ", writeRate=" + fpf.format(writeRate) + "MB/sec");

        return elapsed;
        
    }

    /**
     * Writes N records of the given size such that the journal will be filled
     * to "near" capacity using either an isolated or unisolated {@link BTree}
     * to absorb the writes. The records are written in key order, so this is
     * the best cast for sequential key writes. The test ends before the journal
     * would overflow in order to measure only the cost of writes without buffer
     * extension handling.
     * <p>
     * Note that for transactional writes, the writes are buffered in memory and
     * then on disk, validated against the buffered writes, and finally
     * transferred to the unisolated index on the journal. Short transactions
     * are therefore very fast, but large transactions will be significantly
     * slower than the corresponding unisolated writes since there is several
     * times more IO for large transactions (write on tx buffer, read tx buffer
     * and validate against the unisolated index, read tx buffer and write on
     * the unisolated index). However, there is also logic to defeat validation
     * when no concurrent writes have occurred, so the worst case will not be
     * demonstrated by a single write process.
     * 
     * @param name
     *            The name of the index on which the writes will be performed.
     *            The named index MUST have been registered by the caller and
     *            that registration MUST have been committed.
     * 
     * @param tx
     *            The transaction identifier -or- 0L if the writes will not be
     *            isolated by a transaction.
     * 
     * @param valueSize
     *            The size in bytes of the value to be written under each key.
     * 
     * @return The elapsed time for the test.
     */
    public long doIndexWriteRateTest(String name, long tx, int valueSize) {

        IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);
        
        // @todo rewrite as a Task submitted to the journal using that timestamp.
        IIndex ndx = (tx == 0 ? journal.getIndex(name)
                : journal.getLocalTransactionManager().getTx(tx).getIndex(name));

        System.err.println("Begin: index write rate, isolated="
                + (tx == 0 ? "no" : "yes") + ", isolatable="
                + ndx.getIndexMetadata().isIsolatable() + ", bufferMode="
                + journal.getBufferStrategy().getBufferMode());

        // target percentage full to avoid journal overflow.
        final double percentFull = .90;

        // #of entries to insert into the index.
        final int nwrites = (int) (journal.getBufferStrategy().getExtent()
                * percentFull / valueSize);

        final long begin;

        {

            begin = System.currentTimeMillis();

            for (int i = 0; i < nwrites; i++) {

                // key[] is new on each insert; keys are monotonically
                // increasing.
                final byte[] key = keyBuilder.reset().append(i).getKey();

                // value[] is new on each insert.
                final byte[] value = new byte[valueSize];

                value[0] = (byte) i; // at least one non-zero byte.

                ndx.insert(key, value);

            }
            
        }

        if (tx == 0L) {
            
            /*
             * Force to stable store when not using isolation (the transaction
             * does this anyway so this makes things more fair).
             */
            
            final long beginCommit = System.currentTimeMillis();
            
            final long elapsedWrite = beginCommit - begin;

            journal.commit();

            final long elapsedCommit = System.currentTimeMillis() - beginCommit;

            System.err.println("Write  : "+elapsedWrite+"(ms)");
            System.err.println("Commit : "+elapsedCommit+"(ms)");

        } else {

            /*
             * @todo track active vs validation vs commit time for transactions
             * so that I can report them here.
             */

//            ITx t = journal.getTx(tx);
//            
//            final long beginPrepare = System.currentTimeMillis();            
//
//            final long elapsedWrite =  beginPrepare - begin;
//
//            t.prepare(journal.nextTimestamp());
//
//            final long beginCommit = System.currentTimeMillis();
//            
//            final long elapsedPrepare = beginCommit - beginPrepare;

            final long beginCommit = System.currentTimeMillis();
            
            final long elapsedWrite = beginCommit - begin;
            
            journal.commit(tx);

            final long elapsedCommit = System.currentTimeMillis() - beginCommit;

            System.err.println("Write  : "+elapsedWrite+"(ms)");
//            System.err.println("Prepare: "+elapsedPrepare+"(ms)");
            System.err.println("Commit : "+elapsedCommit+"(ms)");
            
        }
        
        final long elapsed = System.currentTimeMillis() - begin;

        // The unisolated btree on which the data were actually written.
        final BTree btree = (BTree)journal.getIndex(name);
        
        final long nodesWritten = btree.getBtreeCounters().getNodesWritten();
        
        final long leavesWritten = btree.getBtreeCounters().getLeavesWritten();
        
        final long bytesWrittenByBTree = btree.getBtreeCounters().getBytesWritten();
        
        final long bytesWritten = journal.getBufferStrategy().getNextOffset();

        System.err.println("bytesWritten: btree="+bytesWrittenByBTree+", journal="+bytesWritten);
        
        System.err.println("btree counters: "+btree.getBtreeCounters());

        final long recordsWritten = (nodesWritten + leavesWritten);
        
        final double averageRecordSize = bytesWrittenByBTree / (double)recordsWritten;
        
        // MB/sec.
        final double writeRate = (bytesWritten/(double)Bytes.megabyte) / (elapsed / 1000d);
        
        System.err.println("Elapsed: " + elapsed + "(ms), bufferMode="
                + journal.getBufferStrategy().getBufferMode() + ", valueSize="
                + cf.format(valueSize) + ", ninserts=" + cf.format(nwrites)
                + ", nrecordsWritten=" + recordsWritten
                + ", averageRecordSize=" + fpf.format(averageRecordSize)
                + ", branchingFactor="+btree.getBranchingFactor()
                + ", writeRate=" + fpf.format(writeRate) + "MB/sec");
        
        return elapsed;
        
    }

    public static class BenchmarkTransientJournal extends BenchmarkJournalWriteRate {

        @Override
        protected BufferMode getBufferMode() {return BufferMode.Transient;}

    }
    
    public static class BenchmarkDirectJournal extends BenchmarkJournalWriteRate {

        @Override
        protected BufferMode getBufferMode() {return BufferMode.Direct;}

    }
    
    public static class BenchmarkMappedJournal extends BenchmarkJournalWriteRate {

        @Override
        protected BufferMode getBufferMode() {return BufferMode.Mapped;}

    }
    
    public static class BenchmarkDiskJournal extends BenchmarkJournalWriteRate {

        @Override
        protected BufferMode getBufferMode() {return BufferMode.Disk;}
        
    }

    public static class BenchmarkDiskRWJournal extends BenchmarkJournalWriteRate {

        @Override
        protected BufferMode getBufferMode() {return BufferMode.DiskRW;}
        
    }

    /**
     * <p>
     * Does N writes of M size data blocks on a pre-extended file using pure
     * sequential IO. Small writes may be used to estimate the maximum
     * throughput for large numbers of small writes. Large writes may be used to
     * estimate the absolute maximum throughput for your platform (OS + disk
     * system).
     * </p>
     * <p>
     * Note: This test is conducted without the use of a journal. It is bundled
     * in the same source code file so that we can compare the journal
     * performance with the raw IO performance of the platform.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract static class AbstractBenchmarkOptimium extends TestCase2 {

        /**
         * The file is named for the test class.
         * <p>
         * Note: contents are not actually a journal.
         */
        protected String getFilename() {
        
            return getClass().getSimpleName()+".bin";
            
        }
        
        /**
         * Override to specify the record size.
         */
        abstract public int getRecordSize();

        /**
         * 100M
         */
        protected int getInitialExtent() {
            
            return Bytes.megabyte32 * 100;
            
        }
        
        RandomAccessFile raf;

        void deleteFile() {
            
            try {
                
                File file = new File(getFilename());
                
                if ( file.exists() && ! file.delete()) {
                    
                    System.err.println("Warning: could not delete: " + file.getAbsolutePath());
                    
                }
                
            } catch (Throwable t) {
                
                System.err.println("Warning: " + t);
                
            }

        }
        
        public void setUp() throws IOException {
            
            System.err.println("------------------\n");
            
            deleteFile();

            // Note: This makes it MUCH slower.
            boolean forceWrites = false;
            
            raf = new RandomAccessFile(getFilename(),"rw"+(forceWrites?"d":""));
            
        }
        
        public void tearDown() throws IOException {
            
            raf.getChannel().force(false);
            
            raf.close();
            
            deleteFile();
            
        }

        public void testNoIsolation() throws IOException {

            doOptimiumWriteRateTest(getRecordSize());
            
        }

        /**
         * 
         * @param recordSize
         * @throws IOException
         */
        public void doOptimiumWriteRateTest(int recordSize) throws IOException {

            final long begin = System.currentTimeMillis();

            final int dataSize = getRecordSize();
            
            final long initialExtent = getInitialExtent();
            
            final int nwrites = (int) initialExtent / dataSize;
            
            System.err.println("Begin: optimum write rate test: #writes="
                    + nwrites + ", dataSize=" + dataSize);
           
            raf.setLength(initialExtent);
            
            ByteBuffer data = ByteBuffer.allocateDirect(dataSize);
            
            long pos = 0;
            
            for( int i=0; i<nwrites; i++ ) {
            
                data.put(0,(byte)i); // at least one non-zero byte.

                data.position( 0 );
                
                data.limit( dataSize );
                
                raf.getChannel().write(data,pos);
                
                pos += dataSize;
                
            }

            final long elapsed = System.currentTimeMillis() - begin;
            
            final long bytesWritten = raf.length();

            // MB/sec.
            final double writeRate = (bytesWritten/(double)Bytes.megabyte) / (elapsed / 1000d);
            
            System.err.println("Elapsed: " + elapsed
                    + "(ms), non-journal optimum, recordSize="
                    + cf.format(dataSize) + ", nwrites=" + cf.format(nwrites)
                    + ", writeRate=" + fpf.format(writeRate) + "MB/sec");
            
        }

    }
    
    /**
     * <p>
     * Writes the same amount of data, using <code>128</code> byte records on
     * a pre-extended file using pure sequential IO. This case should produce
     * the optimium throughput to disk for small IOs.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BenchmarkSmallRecordOptimium extends AbstractBenchmarkOptimium {

        /**
         * 128 bytes.
         */
        public int getRecordSize() {
            
            return 128;
            
        }

    }
    
    /**
     * <p>
     * Writes the same amount of data using large blocks on a pre-extended file
     * using pure sequential IO. This case should produce the "best-case"
     * optimium throughput to disk <i>for block-oriented IO</i>. In order for
     * the journal to approach this best case scenario, you need to be writing
     * large blocks. Note that the btree does exactly this, but the limiting
     * factor for throughput is the write on the btree data structures (mostly
     * key search) rather than the writes on the journal and their consequent
     * IO.
     * </p>
     * <p>
     * Note: This overrides several methods in the base class in order to
     * conduct a test without the use of a journal.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BenchmarkBlockBasedOptimium extends AbstractBenchmarkOptimium {
        
        /**
         * 8k
         */
        public int getRecordSize() {
            
            return Bytes.kilobyte32 * 8;
            
        }

    }
    
    /**
     * <p>
     * Writes the same amount of data using a single nio "write buffer"
     * operation on a pre-extended file. The buffer is a direct buffer, so it is
     * allocated in the OS memory. The write should be pure sequential IO. This
     * case should produce the "best-case" optimium throughput to disk <i>for
     * sustained IO</i>. The journal SHOULD NOT be able approach this best case
     * scenario. Comparison to this case should reveal the overhead of the
     * journal, Java, and block-oriented IO when compare to sustained sequential
     * data transfer from RAM to disk. Since block-based IO is, in fact, better,
     * one can only presume that the nio library has some problem with very
     * large writes.
     * </p>
     * <p>
     * Note: This overrides several methods in the base class in order to
     * conduct a test without the use of a journal.
     * </p>
     * <p>
     * Note: I have seen block-based IO perform better in cases where system
     * resources were low (the disk was nearly full).
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BenchmarkSustainedTransferOptimium extends AbstractBenchmarkOptimium {

        /**
         * The entire extent in one sustained write.
         */
        public int getRecordSize() {
            
            return (int) getInitialExtent();
            
        }

    }
    
    /**
     * Runs the tests that have not been commented out :-)
     * <p>
     * Note: Running all benchmarks together can challange the VM by running low
     * on heap, native memory given over to direct buffers - and things can actually
     * slow down with more memory.
     */
    public static Test suite() {
        
        TestSuite suite = new TestSuite("Benchmark Journal Write Rates");
        
        suite.addTestSuite( BenchmarkTransientJournal.class );
//        suite.addTestSuite( BenchmarkDirectJournal.class );
//        suite.addTestSuite( BenchmarkMappedJournal.class );
        suite.addTestSuite( BenchmarkDiskJournal.class );
        suite.addTestSuite( BenchmarkDiskRWJournal.class );
        suite.addTestSuite( BenchmarkSmallRecordOptimium.class );
        suite.addTestSuite( BenchmarkBlockBasedOptimium.class );
        suite.addTestSuite( BenchmarkSustainedTransferOptimium.class );

        return suite;
        
    }
    
    /**
     * Main routine can be used for running the test under a performance
     * analyzer.
     * 
     * @param args
     *            Not used.
     * 
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        
        BenchmarkTransientJournal test = new BenchmarkTransientJournal();
        
        test.setUp();
        
        try {

            /*
             * Choose one test to run. (You must setUp/tearDown for each test).
             */
            test.testNonIsolatableIndexWriteRate();

//          test.testUnisolatedIndexWriteRate();

//            test.testIsolatedIndexWriteRate();

        }
        finally {
            
            test.tearDown();
            
        }
        
    }
    
}
