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
 *      Windows XP 2002 SP2 on Dell Latitude D620.
 *      
 *      #of slots in the journal: 819200
 *      slot size: 128
 *      Elapsed: 1609(ms), bufferMode=transient (memory only)
 *      Elapsed: 2375(ms), block-based optimum (low level disk IO, 8k blocks)
 *      Elapsed: 2687(ms), bufferMode=mapped
 *      Elapsed: 4641(ms), slot-based optimum (low level disk IO, 128b slots)
 *      Elapsed: 9328(ms), bufferMode=disk
 *      Elapsed: 9391(ms), bufferMode=direct
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
 *       cases.
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

        properties.setProperty("initialExtent",""+getInitialExtent());

        properties.setProperty("slotSize",""+getSlotSize());
        
        properties.setProperty("bufferMode", getBufferMode().toString());

        properties.setProperty("segment", "0");
        
        properties.setProperty("file",getFilename());
        
        return properties;
        
    }

    /**
     * The tests are performed with a 100M journal.
     */
    public long getInitialExtent() {return 100*Bytes.megabyte;}

    /**
     * The test are performed with a slot size of 128 - all "objects" are
     * designed to fit within a single slot.
     */
    public int getSlotSize() {return 128;}

    abstract public BufferMode getBufferMode();
    
    public String getFilename() {
    
        return "benchmark-"+getBufferMode()+".jnl";
        
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
        
        deleteFile();
        
        journal = new Journal( getProperties() );
        
    }
    
    public void tearDown() throws IOException {
        
        journal.close();
        
        deleteFile();
        
    }
    
    public void test() throws IOException {

        // Note: This does not work since we have to setUp/tearDown for each
        // pass.  We can make this work if we move away from junit for the
        // benchmark.
        
//        final int limit = 10;
//        
//        long[] elapsed = new long[limit];
//        
//        long total = 0l;
//        
//        // Discard one test.
//        doJournalWriteRateTest();
//
//        // Run N tests.
//        for( int i=0; i<limit; i++ ) {
//            
//            elapsed[ i ] = doJournalWriteRateTest();
//
//            total += elapsed[ i ];
//            
//        }
//        
//        // Report average elapsed time.
//        System.err.println("Results: "+getBufferMode()+" : "+(total/limit));
    
        doJournalWriteRateTest();

    }

    /**
     * Run a test.
     * 
     * @return The elapsed time for the test.
     */
    public long doJournalWriteRateTest() {

        System.err.println("Begin: bufferMode="+journal._bufferStrategy.getBufferMode());

        final long begin = System.currentTimeMillis();
        
        final int slotLimit = journal._bufferStrategy.getSlotLimit();

        final int dataSize = journal._bufferStrategy.getSlotDataSize();
        
        ByteBuffer data = ByteBuffer.allocateDirect(dataSize);

        Tx tx = new Tx(journal,0L);
        
        for( int i=0; i<slotLimit; i++ ) {
        
            data.put(0,(byte)i); // at least one non-zero byte.

            data.position( 0 );
            
            data.limit( dataSize );
            
            journal.write(tx, i, data);
            
        }

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("Elapsed: "+elapsed+"(ms), bufferMode="+journal._bufferStrategy.getBufferMode());

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

        public void test() throws IOException {

            doOptimiumWriteRateTest();
            
        }

        public void doOptimiumWriteRateTest() throws IOException {

            final long begin = System.currentTimeMillis();

            final int dataSize = new SlotMath(getSlotSize()).dataSize;
            
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
     * optimium throughput to disk. In order for the journal to approach this
     * best case scenario, it needs to combine individual slot writes that are,
     * in fact, on purely successive positions in the channel into larger block
     * write operations. 
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

        public void test() throws IOException {

            doOptimiumWriteRateTest();
            
        }

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

            final long elapsed = System.currentTimeMillis() - begin;
            
            System.err.println("Elapsed: "+elapsed+"(ms), block-based optimum");

        }

    }
    
    public static Test suite() {
        
        TestSuite suite = new TestSuite("Benchmark Journal Write Rates");
        
        suite.addTestSuite( BenchmarkTransientJournal.class );
        suite.addTestSuite( BenchmarkDirectJournal.class );
        suite.addTestSuite( BenchmarkMappedJournal.class );
        suite.addTestSuite( BenchmarkDiskJournal.class );
        suite.addTestSuite( BenchmarkSlotBasedOptimium.class );
        suite.addTestSuite( BenchmarkBlockBasedOptimium.class );

        return suite;
        
    }
    
}
