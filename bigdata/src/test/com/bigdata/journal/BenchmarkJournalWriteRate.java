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
 * Created on Oct 15, 2006
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestSuite;

import com.bigdata.rawstore.Bytes;

/**
 * <p>
 * A benchmark that computes the journal's write rate.
 * </p>
 * This is the most basic benchmark. It compares pure sequential writes on the
 * file system with throughput when using each of the journal modes. This tells
 * us the overhead imposed by the journal.
 * </p>
 * <p>
 * The results from this benchmark can be used to compare the performance of the
 * different {@link IBufferMode} implementations. The theoretical maximum for a
 * platform is the sustained write rate for the disk subsystem on which the
 * journal file is located - this can be obtained with a bit of research on your
 * disk drives, e.g., using <a href="http://www.storagereview.com/">
 * storagereview.com </a>.
 * </p>
 * 
 * <pre>
 *                         
 *              Windows XP 2002 SP2 on Dell Latitude D620.
 *              
 *              #of slots in the journal: 819200
 *              slot size: 128
 *              Elapsed: 1609(ms), bufferMode=transient (memory only)
 *              Elapsed: 2375(ms), block-based optimum (low level disk IO, 8k blocks)
 *              Elapsed: 2485(ms), sustained transfer optimum
 *              Elapsed: 2687(ms), bufferMode=mapped
 *              Elapsed: 4641(ms), slot-based optimum (low level disk IO, 128b slots)
 *              Elapsed: 9328(ms), bufferMode=disk
 *              Elapsed: 9391(ms), bufferMode=direct
 *                         
 * </pre>
 * 
 * <p>
 * Analysis: The direct-mode journal imposes nearly a 2x overhead when compared
 * to per-slot low-level IOs. However, block-oriented modes (block-based optimum
 * and memory-mapped), _significantly_ out perform per-slot IO modes, achieving
 * nearly the performance of the transient mode (direct in-memory buffer without
 * IO).
 * </p>
 * 
 * FIXME Implement and test the impact of an AIO strategy.
 * 
 * @todo Monitor the heap, IO, and CPU using this benchmark : profile as well.
 *       Try on linux with top and vmstat and on Windows with "perfmon".
 * 
 * @todo Note that the target performance environment requires multiple journals
 *       and multiple read-optimized databases. Do not over-optimize for a
 *       single writer. Write benchmarks for write absorption rates for
 *       concurrent writers with and without concurrent readers.
 * 
 * @todo Test for variance in performance. If necessary, run multiple times and
 *       average the results for each buffer mode.
 * 
 * @todo Try with "write through" option on the {@link RandomAccessFile} enabled
 *       for the various journal configurations and the low level "optimum" test
 *       cases. Write through may do quite well in combination with a dirty page
 *       cache (commit list of dirty pages with incremental flush and flush on
 *       commit in any case).
 * 
 * @todo Compare performance with and without transactional isolation. Isolation
 *       is definately slower. Validation is relatively fast. Most of the
 *       additional time appears to be merging the isolated object index down
 *       onto the global object index. (Note that I am seeing lots of variance
 *       in the times for the transient journal. This may be due to whether or
 *       not a full GC is being performed. Also, the transient journal is a
 *       direct buffer, so there may be contention for native heap space.)
 * 
 * @todo Compare performance as a function of the "object size". It appears to
 *       be significantly faster to perform fewer writes of larger objects both
 *       with and without isolation.
 * 
 * FIXME The memory-mapped file can not be deleted after close(). See
 * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038.
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

        properties.setProperty(Options.SEGMENT, "0");
        
        properties.setProperty(Options.CREATE_TEMP_FILE, "true");
        
//        properties.setProperty(Options.DELETE_ON_CLOSE,"true");

        properties.setProperty(Options.DELETE_ON_EXIT,"true");
        
        return properties;
        
    }

    /**
     * The tests are performed with a 100M journal.
     */
    public long getInitialExtent() {return 100*Bytes.megabyte;}

    /**
     * The test are performed with a slot size of 128, but you can vary
     * the "object size" to be larger or smaller.
     */
    public int getSlotSize() {return 128;}

    abstract public BufferMode getBufferMode();
    
    public String getFilename() {
    
        return "benchmark-"+getBufferMode()+"-"+getName()+".jnl";
        
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

            journal.closeAndDelete();
            
//            deleteFile();
            
        }
        catch( IllegalStateException ex ) {
            
            System.err.println("Could not close the journal: "+ex);
            
        }
        
    }

//    /**
//     * FIXME Testing with isolation appears to pose a heavy memory burden during
//     * writes (vs prepare/commit) for some of the buffer modes - track that
//     * down!
//     */
//    public void testWithIsolation() throws IOException {
//
//        Tx tx = new Tx(journal, 0L);
//
//        doJournalWriteRateTest(tx,128);
//
//    }

    public void testNoIsolation() throws IOException {

        doJournalWriteRateTest(128);

    }
    
    /**
     * Run a test.
     * 
     * @param writeSize
     *            The size of the object to be written.
     * 
     * @return The elapsed time for the test.
     */
    public long doJournalWriteRateTest(int writeSize) {

        Journal store = journal;
        
        System.err.println("Begin: bufferMode="+journal._bufferStrategy.getBufferMode());

        final long begin = System.currentTimeMillis();
        
        final int nwrites = (int) journal._bufferStrategy.getExtent()
                / writeSize;
        
        System.err.println("writeSize=" + writeSize + ", nwrites=" + nwrites);
        
        ByteBuffer data = ByteBuffer.allocateDirect(writeSize);

//        int id = 1;
        
        for( int i=0; i<nwrites; i++ ) {
        
            data.put(0,(byte)i); // at least one non-zero byte.

            data.position( 0 );
            
            data.limit( writeSize );
            
            store.write(data);
            
        }

//        final boolean isTx = store instanceof ITx;
//        
//        if( isTx ) {
//
//            ITx tx = (Tx) store;
//            
//            final long beginPrepare = System.currentTimeMillis();            
//
//            final long elapsedWrite =  beginPrepare - begin;
//
//            tx.prepare();
//
//            final long beginCommit = System.currentTimeMillis();
//            
//            final long elapsedPrepare = beginCommit - beginPrepare;
//
//            tx.commit();
//
//            final long elapsedCommit = System.currentTimeMillis() - beginCommit;
//
//            System.err.println("Write  : "+elapsedWrite+"(ms)");
//            System.err.println("Prepare: "+elapsedPrepare+"(ms)");
//            System.err.println("Commit : "+elapsedCommit+"(ms)");
//
//        } else {
//            
//            /*
//             * Force to stable store when not using isolation (the transaction
//             * does this anyway so this makes things more fair).
//             */
//            
//            journal._bufferStrategy.force(0L,true);
//            
//        }
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("Elapsed: " + elapsed + "(ms), bufferMode="
                + journal._bufferStrategy.getBufferMode()
//                + ", isolation="+ isTx
                );

//        store.closeAndDelete(); // done in tearDown.
        
        return elapsed;
        
    }

    public static class BenchmarkTransientJournal extends BenchmarkJournalWriteRate {

        public BufferMode getBufferMode() {return BufferMode.Transient;}

    }
    
    public static class BenchmarkDirectJournal extends BenchmarkJournalWriteRate {

        public BufferMode getBufferMode() {return BufferMode.Direct;}

    }
    
    public static class BenchmarkMappedJournal extends BenchmarkJournalWriteRate {

        public BufferMode getBufferMode() {return BufferMode.Mapped;}

    }
    
    public static class BenchmarkDiskJournal extends BenchmarkJournalWriteRate {

        public BufferMode getBufferMode() {return BufferMode.Disk;}
        
    }

    /**
     * <p>
     * Writes the same amount of data, in the same sized blocks (e.g., object /
     * slot sized blocks), on a pre-extended file using pure sequential IO. This
     * case should produce the optimium throughput to disk. The same size blocks
     * are used so as to not priviledge this case unduely if we assume that we
     * are writing objects whose serialized form normally fits into one slot of
     * a journal. If you are doing a sustained fat grained write, then you
     * should hit the absolute maximum for your platform.
     * </p>
     * <p>
     * Note: This overrides several methods in the base class in order to
     * conduct a test without the use of a journal.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BenchmarkSlotBasedOptimium extends BenchmarkJournalWriteRate {

        // Note: This winds up basically not used.
        public BufferMode getBufferMode() {return BufferMode.Transient;}
        
        // Note: contents are not actually a journal.
        public String getFilename() {return "benchmark-slot-based-optimum.bin";}

        RandomAccessFile raf;

        public void setUp() throws IOException {
            
            deleteFile();

            raf = new RandomAccessFile(getFilename(),"rw");
            
        }
        
        public void tearDown() throws IOException {
            
            raf.getChannel().force(false);
            
            raf.close();
            
            deleteFile();
            
        }

        public void testNoIsolation() throws IOException {

            doOptimiumWriteRateTest();
            
        }

        public void testWithIsolation() throws IOException {/*NOP*/}
        
        public void doOptimiumWriteRateTest() throws IOException {

            final long begin = System.currentTimeMillis();

            final int dataSize = getSlotSize();
            
            final long initialExtent = getInitialExtent();
            
            final int slotLimit = (int) initialExtent / dataSize;
            
            System.err.println("Begin: Slot-based optimum write rate test: #writes="+slotLimit+", dataSize="+dataSize);
           
            raf.setLength(initialExtent);
            
            ByteBuffer data = ByteBuffer.allocateDirect(dataSize);
            
            long pos = 0;
            
            for( int i=0; i<slotLimit; i++ ) {
            
                data.put(0,(byte)i); // at least one non-zero byte.

                data.position( 0 );
                
                data.limit( dataSize );
                
                raf.getChannel().write(data,pos);
                
                pos += dataSize;
                
            }

            final long elapsed = System.currentTimeMillis() - begin;
            
            System.err.println("Elapsed: "+elapsed+"(ms), slot-based optimum");

        }

    }
    
    /**
     * <p>
     * Writes the same amount of data using large blocks on a pre-extended file
     * using pure sequential IO. This case should produce the "best-case"
     * optimium throughput to disk <i>for block-oriented IO</i>. In order for
     * the journal to approach this best case scenario, it needs to combine
     * individual slot writes that are, in fact, on purely successive positions
     * in the channel into larger block write operations.
     * </p>
     * <p>
     * Note: This overrides several methods in the base class in order to
     * conduct a test without the use of a journal.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BenchmarkBlockBasedOptimium extends BenchmarkJournalWriteRate {

        // Note: This winds up basically not used.
        public BufferMode getBufferMode() {return BufferMode.Transient;}
        
        // Note: contents are not actually a journal.
        public String getFilename() {return "benchmark-block-based-optimum.bin";}

        RandomAccessFile raf;

        public void setUp() throws IOException {
            
            deleteFile();

            raf = new RandomAccessFile(getFilename(),"rw");
            
        }
        
        public void tearDown() throws IOException {
            
            raf.getChannel().force(false);
            
            raf.close();
            
            deleteFile();
            
        }

        public void testNoIsolation() throws IOException {

            doOptimiumWriteRateTest();
            
        }

        public void testWithIsolation() throws IOException {/*NOP*/}

        public void doOptimiumWriteRateTest() throws IOException {

            final long begin = System.currentTimeMillis();

            final int dataSize = 8 * Bytes.kilobyte32;
            
            final long initialExtent = getInitialExtent();
            
            final int slotLimit = (int) initialExtent / dataSize;
            
            System.err.println("Begin: Block-based optimum write rate test: #writes="+slotLimit+", dataSize="+dataSize);
           
            raf.setLength(initialExtent);
            
            ByteBuffer data = ByteBuffer.allocateDirect(dataSize);
            
            long pos = 0;
            
            for( int i=0; i<slotLimit; i++ ) {
            
                data.put(0,(byte)i); // at least one non-zero byte.

                data.position( 0 );
                
                data.limit( dataSize );
                
                raf.getChannel().write(data,pos);
                
                pos += dataSize;
                
            }

            // Force to the disk.
            raf.getChannel().force(false);

            final long elapsed = System.currentTimeMillis() - begin;
            
            System.err.println("Elapsed: "+elapsed+"(ms), block-based optimum");

        }

    }
    
    /**
     * <p>
     * Writes the same amount of data using a single nio "write buffer"
     * operation on a pre-extended file. The buffer is a direct buffer, so it is
     * allocated in the OS memory. The write should be pure sequential IO. This
     * case should produce the "best-case" optimium throughput to disk <i>for
     * sustained IO</i>. The journal SHOULD NOT be approach this best case
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
     * FIXME Try with write through to disk option enabled for the channel in
     * the RandomAccessFile constructor. This might improve performance since we
     * are writing through without read back. Try this option also on the
     * block-based optimium test. If successful in that context, then it SHOULD
     * be used on the journal modes with a page cache (direct and disk-only).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BenchmarkSustainedTransferOptimium extends BenchmarkJournalWriteRate {

        // Note: This winds up basically not used.
        public BufferMode getBufferMode() {return BufferMode.Transient;}
        
        // Note: contents are not actually a journal.
        public String getFilename() {return "benchmark-sustained-transfer-optimum.bin";}

        RandomAccessFile raf;

        public void setUp() throws IOException {
            
            deleteFile();

            raf = new RandomAccessFile(getFilename(),"rw");
            
        }
        
        public void tearDown() throws IOException {
            
            raf.getChannel().force(false);
            
            raf.close();
            
            deleteFile();
            
        }

        public void testNoIsolation() throws IOException {

            doOptimiumWriteRateTest();
            
        }

        public void testWithIsolation() throws IOException {/*NOP*/}

        public void doOptimiumWriteRateTest() throws IOException {

            final long initialExtent = getInitialExtent();

            if( initialExtent > Integer.MAX_VALUE ) {
                
                throw new RuntimeException("The initialExtent is too large to be buffered in RAM.");
                
            }
            
            final int dataSize = (int) initialExtent;
            
            System.err.println("Begin: Sustained-tranasfer optimum write rate test: #writes="+1+", dataSize="+dataSize);
           
            raf.setLength(initialExtent);
            
            ByteBuffer data = ByteBuffer.allocateDirect(dataSize);
            
            data.position( 0 );
                
            data.limit( dataSize );
                
            // Note measure only the disk operation time.
            final long begin = System.currentTimeMillis();

            // Write on the disk.
            raf.getChannel().write(data,0L);

            // Force to the disk.
            raf.getChannel().force(false);
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            System.err.println("Elapsed: "+elapsed+"(ms), sustained transfer optimum");

        }

    }
    
    /**
     * Runs the tests that have not been commented out :-)
     * 
     * Note: Running all benchmarks together can challange the VM by running low
     * on heap, native memory given over to direct buffers - and things can actually
     * slow down with more memory.
     */
    public static Test suite() {
        
        TestSuite suite = new TestSuite("Benchmark Journal Write Rates");
        
        suite.addTestSuite( BenchmarkTransientJournal.class );
        suite.addTestSuite( BenchmarkDirectJournal.class );
//        suite.addTestSuite( BenchmarkMappedJournal.class );
        suite.addTestSuite( BenchmarkDiskJournal.class );
        suite.addTestSuite( BenchmarkSlotBasedOptimium.class );
        suite.addTestSuite( BenchmarkBlockBasedOptimium.class );
        suite.addTestSuite( BenchmarkSustainedTransferOptimium.class );

        return suite;
        
    }
    
}
