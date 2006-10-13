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
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * An object journal for very fast write absorbtion. The journal supports
 * concurrent fully isolated transactions and is designed to absorb writes
 * destined for a read-optimized database file. Writes are logically appended to
 * the journal to minimize disk head movement.
 * </p>
 * <p>
 * The journal provides for fast migration of committed data to a read-optimized
 * database. Data may be migrated as soon as a transaction commits and only the
 * most recent state for any datum need be migrated. Note that the criteria for
 * migration typically are met before the slots occupied by those objects may be
 * released. This is because a concurrent transaction must be permitted to read
 * from the committed state of the database at the time that the transaction was
 * begun.
 * </p>
 * <p>
 * The journal is a ring buffer divised of slots. The slot size and initial
 * extent are specified when the journal is provisioned. Objects are written
 * onto free slots. Slots are released once no active transaction can read from
 * that slots. In this way, the journal buffers consistent histories.
 * </p>
 * <p>
 * The journal maintains two indices:
 * <ol>
 * <li>An object index from int32 object identifier to a slot allocation; and
 * </li>
 * <li>An allocation index from slot to a slot status record.</li>
 * </ol>
 * These data structures are b+trees. The nodes of the btrees are stored in the
 * journal. These data structures are fully isolated. The roots of the indices
 * are choosen based on the transaction begin time. Changes result in a copy on
 * write. Writes percolate up the index nodes to the root node.
 * </p>
 * <p>
 * Commit processing. The journal also maintains two root blocks. When the
 * journal is wired into memory, incremental writes are absorbed by the journal
 * into a direct buffer and written through to disk. Writes are flushed to disk
 * on commit. Commit processing also updates the root blocks using the Challis
 * algorithm. (The root blocks are updated using an alternating pattern and
 * timestamps are recorded at the head and tail of each root block to detect
 * partial writes.)
 * </p>
 * <p>
 * A journal may be used without a database file as an object database. The
 * design is very efficient for absorbing writes, but read operations are not
 * optimized. Overall performance will degrade as the journal size increases
 * beyond the limits of physical memory and as the object index depth increases.
 * </p>
 * <p>
 * A journal and a database file form a logical segment in the bigdata
 * distributed database architecture. In bigdata, the segment size is generally
 * limited to a few hundred megabytes. At this scale, the journal and database
 * may both be wired into memory for the fastest performance. If memory
 * constraits are tighted, these files may be memory-mapped or used as a
 * disk-base data structures. A single journal may buffer for multiple copies of
 * the same database segment or journals may be chained for redundancy.
 * </p>
 * <p>
 * Very large objects should be handled by specially provisioned database files
 * using large pages and a "never overwrite" strategy.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There is a dependency in a distributed database architecture on
 *       transaction begin time. A very long running transaction could force the
 *       journal to hold onto historical states. If a decision is made to
 *       discard those states and the transaction begins to read from the
 *       journal then the transaction must be rolled back.
 * 
 * @todo Define distributed transaction protocol.
 * 
 * @todo Define distributed protocol for robust startup, operation, and
 *       failover.
 * 
 * @todo Implement forward validation and state-based conflict resolution with
 *       custom merge rules for persistent objects, generic objects, and btree
 *       index nodes.
 * 
 * @todo Divide into API layers. The network facing layer uses a single threaded
 *       nio interface to receive writes into direct buffers. Writes on the
 *       journal are also single-threaded. Buffers of rows are queued by one
 *       thread and then written through to the journal by another. Isolation
 *       occurs in a layer above raw journal writes that maintains the object
 *       and allocation indices. Rows can be written onto slots using simple
 *       math since both the row size and the slot size are known. Objects must
 *       be deserialized during state-based validation if a conflict is
 *       detected.
 * 
 * @todo Refactor btree support for journals.
 * 
 * @todo Flushing to disk on commit could be optional, e.g., if there are
 *       redundent journals then this is not required.
 * 
 * @todo cache index and allocation nodes regardless of the strategy.
 * 
 * @todo cache objects? They are materialized only for state-based validation.
 *       If possible, validation should occur in its own layer so that the basic
 *       store can handle IO using either byte[] or streams (allocated from a
 *       high concurrency queue to minimize stream creation overhead).
 */

public class Journal /*implements IStore*/ {

    final static long DEFAULT_INITIAL_EXTENT = 10 * Bytes.megabyte;
    
    final int slotSize;
    final SlotMath slotMath;
//    final long initialExtent;
//    final File file;
//    final RandomAccessFile raf;
//    final FileLock fileLock;
    
    /**
     * The buffer mode in which the file is opened.
     */
    public BufferMode getBufferMode() {
        
        return _bufferStrategy.getBufferMode();
        
    }
//    final BufferMode bufferMode;
    
//    /**
//     * Optional buffer image of the journal. There are two strategies for using
//     * the buffer. One is as a memory mapped image of the journal, so the OS
//     * controls which pages of the journal are currently materialized in the
//     * buffer and writes on the buffer are written through to disk by the OS.
//     * The other option is a direct buffer. This is the fastest option and the
//     * one where the journal has the most control over IO and latency. When
//     * allocated as a direct buffer, data are written on the buffer before
//     * writing to disk and reads are performed from the buffer.
//     * 
//     * @todo The buffer and the {@link RandomAccessFile} should be encapsulated
//     *       so that the journal is, in the most part, unaware of the specific
//     *       strategy in use and so that the strategy may be changed
//     *       dynamically.
//     */
//    final ByteBuffer directBuffer;
    
    /**
     * Interface for implementations of a buffer strategy as identified by a
     * {@link BufferMode}. This interface is designed to encapsulate the
     * specifics of reading and writing slots and performing operations to make
     * an atomic commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Develop abstraction for {@link BufferMode} implementations of the
     *       journal. The code currently uses a direct buffer without writing
     *       through to disk, even though it sets up the disk file. The mode
     *       should be transparently convertable, e.g., from direct to mapped or
     *       disk, from mapped to disk or disk, and from disk to direct or
     *       mapped iff the size on disk permits. There is also a notional mode
     *       for transient journals. You should not be able to convert from a
     *       restart safe journal to a transient journal, but conversion in the
     *       other direction makes sense. One approach is to define an interface
     *       for journal implementations and then have the {@link Journal}
     *       delegate that interface to the appropriate implementation object.
     *       There are three strategies concerned with restart safe journals and
     *       they all have a file on disk. There are three strategies that use a
     *       buffer - two use a direct buffer in exactly the same manner ({@link BufferMode#Transient}
     *       and {@link BufferMode#Direct}) while the other uses a
     *       memory-mapped buffer. There is also an interaction between locking
     *       the file and using a memory mapped disk image (the two do not work
     *       well together).
     */
    public interface IBufferStrategy {
        
        /**
         * The buffer mode supported by the implementation
         * 
         * @return The implemented buffer mode.
         */
        public BufferMode getBufferMode();

        /**
         * True iff the journal is open.
         */
        public boolean isOpen();
        
        /**
         * The index of the first slot that MUST NOT be addressed (e.g., nslots).
         */
        public int getSlotLimit();
        
        /**
         * The current size of the journal in bytes.
         */
        public long getExtent();
        
        /**
         * Read the first slot for some data version.
         * 
         * @param id
         *            The persistent identifier.
         * @param firstSlot
         *            The first slot for that data version.
         * @param readData
         *            When true, a buffer will be allocated sized to exactly hold
         *            the data version and the data from the first slot will be read
         *            into that buffer. The buffer is returned to the caller.
         * @param slotHeader
         *            The structure into which the header data will be copied.
         * 
         * @return A newly allocated buffer containing the data from the first slot
         *         or <code>null</code> iff <i>readData</i> is false. The
         *         {@link ByteBuffer#position()} will be the #of bytes read from the
         *         slot. The limit will be equal to the position. Data from
         *         remaining slots for this data version should be appended starting
         *         at the current position. You must examine
         *         {@link SlotHeader#nextSlot} to determine if more slots should be
         *         read.
         * 
         * @exception RuntimeException
         *                if the slot is corrupt.
         */

        public ByteBuffer readFirstSlot(long id, int firstSlot, boolean readData,
                SlotHeader slotHeader);
        
        /**
         * Read another slot in a chain of slots for some data version.
         * 
         * @param id
         *            The persistent identifier.
         * @param thisSlot
         *            The slot being read.
         * @param priorSlot
         *            The previous slot read.
         * @param slotsRead
         *            The #of slots read so far in the chain for the data version.
         * @param dst
         *            When non-null, the data from the slot is appended into this
         *            buffer starting at the current position.
         * @return The next slot to be read or {@link #LAST_SLOT_MARKER} iff this
         *         was the last slot in the chain.
         */
        public int readNextSlot(long id,int thisSlot,int priorSlot,int slotsRead,ByteBuffer dst );

        /**
         * Write a slot.
         * 
         * @param thisSlot
         *            The slot index.
         * @param priorSlot
         *            The value to be written into the priorSlot header field.
         * @param nextSlot
         *            The value to be written into the nextSlot header field.
         * @param data
         *            The data to be written on the slot.
         */
        public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data);

    }

    /**
     * The implementation logic for the current {@link BufferMode}.
     * 
     * @todo Support dynamically changing the buffer mode or just require that
     * the journal be closed and opened under a new buffer mode?
     */
    final IBufferStrategy _bufferStrategy;

    /**
     * Abstract base class for {@link IBufferStrategy} implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract static class AbstractBufferStrategy implements IBufferStrategy {
        
        /**
         * The size of the journal root blocks.  There are two root blocks
         * and they are written in an alternating order using the Challis
         * algorithm.
         */
        final int SIZE_JOURNAL_HEADER = 0;

        /**
         * The size of a slot. 
         */
        final int slotSize;
        
        /**
         * The size of the per-slot header.
         */
        final int slotHeaderSize;

        /**
         * The size of the per-slot data region.
         */
        final int slotDataSize;

        AbstractBufferStrategy(SlotMath slotMath) {
            
            if( slotMath == null ) throw new IllegalArgumentException();
            
            this.slotSize = slotMath.slotSize;
            
            this.slotHeaderSize = slotMath.headerSize;
            
            this.slotDataSize = slotMath.dataSize;
            
        }

        /**
         * Throws an exception if the extent is too large for an in-memory
         * buffer.
         * 
         * @param extent The extent.
         */
        void assertNonDiskExtent(long extent) {

            if( extent > Integer.MAX_VALUE ) {
                
                /*
                 * The file image is too large to address with an int32. This
                 * rules out both the use of a direct buffer image and the use
                 * of a memory-mapped file. Therefore, the journal must use a
                 * disk-based strategy.
                 */
               
                throw new RuntimeException(
                        "The extent requires the 'disk' mode: extent=" + extent);
                
            }

        }
        
    }
    
    /**
     * Transient buffer strategy uses a direct buffer but never writes on disk.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see BufferMode#Transient
     * 
     * @todo Refactor implementation from {@link DirectBufferStrategy}.
     */
    abstract public static class TransientBufferStrategy {
        
        final BufferMode bufferMode = BufferMode.Transient;

//        final ByteBuffer directBuffer;

        public BufferMode getBufferMode() {return bufferMode;}
        
    }

    /**
     * Direct buffer strategy uses a direct {@link ByteBuffer} as a write
     * through cache and writes through to disk for persistence.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see BufferMode#Direct
     * 
     * FIXME Add logic to write on the backing file.
     * 
     * @todo Support extension of the journal. This will require removing the
     *       'final' declaration from some fields. Those fields should probably
     *       become private at that time.
     */
    public static class DirectBufferStrategy extends AbstractBufferStrategy {

        final BufferMode bufferMode = BufferMode.Direct;

        /**
         * The index of the first slot that MUST NOT be addressed (e.g.,
         * nslots).
         * 
         * FIXME This must be a long integer for the {@link BufferMode#Disk}
         * strategy. The other strategies all use an int limit. That means that
         * we need to hide the actual field a little more, but we also need to
         * change the APIs to pass a long slot value everywhere.
         */
        final int slotLimit;
        
        final File file;
        final RandomAccessFile raf;
        final FileLock fileLock;
        final ByteBuffer directBuffer;
        final long extent;

        public long getExtent() {
            
            return extent;
            
        }
        
        public BufferMode getBufferMode() {return bufferMode;}

        // FIXME Track open vs closed state.
        public boolean isOpen() {
            
            return true;
            
        }
        
        public int getSlotLimit() {return slotLimit;}

        /**
         * Asserts that the slot index is in the legal range for the journal
         * <code>[0:slotLimit)</code>
         * 
         * @param slot The slot index.
         */
        
        void assertSlot( int slot ) {
            
            if( slot>=0 && slot<slotLimit ) return;
            
            throw new AssertionError("slot=" + slot + " is not in [0:"
                    + slotLimit + ")");
            
        }
        
        DirectBufferStrategy(File file, SlotMath slotMath, long initialExtent)
                throws IOException {
            
            super( slotMath );
            
            if( file == null ) throw new IllegalArgumentException();

            this.file = file;

            /*
             * Note: We do not choose the options for writing synchronously
             * to the underlying storage device since we only need to write
             * through to disk on commit, not on incremental write.
             */

            final String fileMode = "rw";

            final boolean exists = file.exists();

            if( exists ) {

                System.err.println("Opening existing file: "+file);
                
            } else {
                
                System.err.println("Will create file: "+file);
                
            }

            /*
             * Open/create the file.
             */
            this.raf = new RandomAccessFile(file, fileMode);
            
            /*
             * Obtain exclusive lock on the file.
             * 
             * FIXME Once we have a lock, we must be sure to release it.
             */
            this.fileLock = this.raf.getChannel().tryLock();

            if (fileLock == null) {

                throw new RuntimeException("Could not lock file: " + file);

            }

            if( exists ) {
                
                /*
                 * The file already exists.
                 * 
                 * FIXME Check root blocks (magic, timestamps), choose root
                 * block, figure out whether the journal is empty or not, read
                 * constants (slotSize, segmentId), make decision whether to
                 * compact and truncate the journal, read root nodes of indices,
                 * etc. Do this before we read the file image into memory since
                 * that is a potentially lengthy operation?
                 * 
                 * Those operations should probably be factored into our
                 * abstract base class.
                 * 
                 * Note: There should be no processing required on restart since
                 * the intention of transactions that did not commit will not be
                 * visible.
                 */

                this.extent = raf.length();
                
                /*
                 * Verify that we can address this many bytes with this
                 * strategy.
                 */
                assertNonDiskExtent(extent);
                
                    /*
                     * Read the file image into a direct buffer.
                     */
                    
                    this.directBuffer = ByteBuffer.allocateDirect((int) extent );
                    
                    raf.getChannel().read(directBuffer, 0L);

                    // @todo verify/set initial position and limit.

                /*
                 * FIXME Read the root index and allocation nodes.
                 */
                
            } else {

                /*
                 * Create a new journal.
                 */
                
                /*
                 * Set the initial extent.
                 */
                
                this.extent = initialExtent;
                
                /*
                 * Verify that we can address this many bytes with this
                 * strategy.
                 */
                assertNonDiskExtent(extent);
                
                raf.setLength(extent);

//                this.initialExtent = raf.length();

                    /*
                     * Allocate the direct buffer.
                     */
                    
                    this.directBuffer = ByteBuffer.allocateDirect((int) extent);

                    /*
                 * FIXME Format the journal, e.g., write the root blocks and the
                 * root index and allocation nodes.  That logic should be factored
                 * into the abstract base class.
                 */

            }
            
            /*
             * The first slot index that MUST NOT be addressed.
             */
            
            slotLimit = (int) (extent - SIZE_JOURNAL_HEADER) / slotSize;
            
            System.err.println("slotLimit="+slotLimit);

        }
        
        public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data) {
            
            // Position the buffer on the current slot.
            int pos = SIZE_JOURNAL_HEADER + slotSize * thisSlot;
            directBuffer.limit( pos + slotSize );
            directBuffer.position( pos );
            
            // Write the slot header.
            directBuffer.putInt(nextSlot); // nextSlot or -1 iff last
            directBuffer.putInt(priorSlot); // priorSlot or -size iff first

            // Write the slot data, advances data.position().
            directBuffer.put(data);

        }

        public ByteBuffer readFirstSlot(long id, int firstSlot, boolean readData,
                SlotHeader slotHeader) {

            assert slotHeader != null;
            
            final int pos = SIZE_JOURNAL_HEADER + slotSize * firstSlot;
            directBuffer.limit( pos + slotHeaderSize );
            directBuffer.position( pos );
            
            int nextSlot = directBuffer.getInt();
            final int size = -directBuffer.getInt();
            if( size <= 0 ) {
                
                dumpSlot( firstSlot, true ); // FIXME Abstract or drop dumpSlot or make it impl specific.
                throw new RuntimeException("Journal is corrupt: id=" + id
                        + ", firstSlot=" + firstSlot + " reports size=" + size);
                
            }

            // Copy out the header fields.
            slotHeader.nextSlot = nextSlot;
            slotHeader.priorSlot = size;

            if( ! readData ) return null;
            
            // Allocate destination buffer to size.
            ByteBuffer dst = ByteBuffer.allocate(size);
            
            /*
             * We copy no more than the remaining bytes and no more than the data
             * available in the slot.
             */
            
            final int dataSize = slotDataSize;
            
            int thisCopy = (size > dataSize ? dataSize : size);
            
            // Set limit on source for copy.
            directBuffer.limit(directBuffer.position() + thisCopy);
            
            // Copy data from slot.
            dst.put(directBuffer);
            
            return dst;

        }

        public int readNextSlot(long id,int thisSlot,int priorSlot,int slotsRead,ByteBuffer dst ) {

            // Position the buffer on the current slot and set limit for copy.
            final int pos = SIZE_JOURNAL_HEADER + slotSize * thisSlot;
            directBuffer.limit( pos + slotHeaderSize );
            directBuffer.position( pos );
                        
            // read the header.
            final int nextSlot = directBuffer.getInt();
            final int priorSlot2 = directBuffer.getInt();
            if( priorSlot != priorSlot2 ) {
                
                dumpSlot( thisSlot, true );
                throw new RuntimeException("Journal is corrupt:  id=" + id
                        + ", slotsRead=" + slotsRead + ", slot=" + thisSlot
                        + ", expected priorSlot=" + priorSlot
                        + ", actual priorSlot=" + priorSlot2);

            }

            // Copy data from slot.
            if( dst != null ) {
                
                final int size = dst.capacity();
                
                final int remaining = size - dst.position();
                
                // #of bytes to read from this slot (header + data).
                final int thisCopy = (remaining > slotDataSize ? slotDataSize : remaining);

                directBuffer.limit( pos + slotHeaderSize + thisCopy );

                dst.put(directBuffer);
                
            }

            return nextSlot;
            
        }
        
        /**
         * Utility shows the contents of the slot on stderr.
         * 
         * @param slot
         *            The slot.
         * @param showData
         *            When true, the data in the slot will also be dumped.
         * 
         * @todo Abstract, remove dumpSlot, or make it impl specific (more code
         * duplication).
         */
        void dumpSlot(int slot, boolean showData) {
            
            System.err.println("slot="+slot);
            assertSlot( slot );
            
            ByteBuffer view = directBuffer.asReadOnlyBuffer();
            
            int pos = SIZE_JOURNAL_HEADER + slotSize * slot;

            view.limit( pos + slotSize );

            view.position( pos );

            int nextSlot = view.getInt();
            int priorSlot = view.getInt();

            System.err.println("nextSlot="
                    + nextSlot
                    + (nextSlot == -1 ? " (last slot)"
                            : (nextSlot < 0 ? "(error: negative slotId)"
                                    : "(more slots)")));
            System.err.println(priorSlot<0?"size="+(-priorSlot):"priorSlot="+priorSlot);
            
            if( showData ) {
            
                byte[] data = new byte[slotDataSize];

                view.get(data);
                
                System.err.println(Arrays.toString(data));
                
            }
            
        }

    }

    /**
     * Memory-mapped journal strategy.
     * 
     * Note: the use of {@link FileLock} with a memory-mapped file is NOT
     * recommended by the JDK as this combination is not permitted on some
     * platforms.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see BufferMode#Mapped
     * 
     * @todo Implement.
     */
    abstract public static class MappedBufferStrategy {

        final BufferMode bufferMode = BufferMode.Mapped;

//        final File file;
//        final RandomAccessFile raf;
//        //final FileLock fileLock;
//
//        final ByteBuffer mappedBuffer;

        public BufferMode getBufferMode() {return bufferMode;}

    }

    /**
     * Disk-based journal strategy.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see BufferMode#Disk
     * 
     * @todo Implement.
     */
    abstract public static class DiskBufferStrategy {

        final BufferMode bufferMode = BufferMode.Disk;

//        final File file;
//        final RandomAccessFile raf;
//        final FileLock fileLock;

        // @todo Uses a page cache, not a directBufffer.
        
        public BufferMode getBufferMode() {return bufferMode;}

    }

    /**
     * Asserts that the slot index is in the legal range for the journal
     * <code>[0:slotLimit)</code>
     * 
     * @param slot The slot index.
     */
    
    void assertSlot( int slot ) {
        
        if( slot>=0 && slot<slotLimit ) return;
        
        throw new AssertionError("slot=" + slot + " is not in [0:"
                + slotLimit + ")");
        
    }
    
    /**
     * Create or open a journal.
     * 
     * @param properties
     *            <dl>
     *            <dt>file</dt>
     *            <dd>The name of the file. If the file not found and "create"
     *            is true, then a new journal will be created.</dd>
     *            <dt>slotSize</dt>
     *            <dd>The slot size in bytes.</dd>
     *            <dt>initialExtent</dt>
     *            <dd>The initial extent of the journal (bytes). The initial
     *            file size is computed by subtracting off the space required by
     *            the root blocks and dividing by the slot size.</dd>
     *            <dt>create</dt>
     *            <dd>When [create == true] and the named file is not found, a
     *            new journal will be created.</dd>
     *            <dt>buffer</dt>
     *            <dd>Either "direct", "mapped", or "disk".</dd>
     *            </dl>
     * @throws IOException
     * 
     * @todo Refactor as factory for more flexibility?
     * @todo Caller should start migration thread.
     * @todo Caller should provide network interface and distribtued commit
     *       support.
     * 
     * @see BufferMode
     */
    
    public Journal(Properties properties) throws IOException {
        
        int slotSize = 128;
        long initialExtent = DEFAULT_INITIAL_EXTENT;
        String val;

        if( properties == null ) throw new IllegalArgumentException();

        /*
         * "buffer" mode. Note that very large journals MUST use the disk-based
         * mode.
         */

        val = properties.getProperty("buffer");

        if (val == null)
            val = BufferMode.Direct.toString();

        BufferMode bufferMode = BufferMode.parse(val);

        /*
         * "slotSize"
         */

        val = properties.getProperty("slotSize");

        if (val != null) {

            slotSize = Integer.parseInt(val);

            if( slotSize < 32 ) {
                
                throw new RuntimeException("slotSize >= 32");
                
            }
            
        }

        this.slotSize = slotSize;

        /*
         * Helper object for slot-based computations. 
         */
        
        this.slotMath = new SlotMath(slotSize);

        /*
         * "initialExtent"
         */

        val = properties.getProperty("initialExtent");

        if (val != null) {

            initialExtent = Long.parseLong(val);

            if( initialExtent <= Bytes.megabyte ) {
                
                throw new RuntimeException(
                        "The initialExtent must be at least one megabyte("
                                + Bytes.megabyte + ")");
                
            }
            
        }

        switch (bufferMode) {
        
        case Transient: {

            throw new UnsupportedOperationException();
            
        }
        
        case Direct: {

            /*
             * "file"
             */

            val = properties.getProperty("file");

            if (val == null) {
                throw new RuntimeException("Required property: 'file'");
            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            _bufferStrategy = new DirectBufferStrategy(file, slotMath,
                    initialExtent);

            this.slotLimit = _bufferStrategy.getSlotLimit();
            
            /*
             * An index of the free and used slots in the journal.
             * 
             * FIXME This needs to be part of the IBufferStrategy interface
             * since the allocation index needs to be a persistent data
             * structure.
             */
            allocationIndex = new BitSet(_bufferStrategy.getSlotLimit());

            break;
        
        }

        case Mapped: {
            
            throw new UnsupportedOperationException();

        }
        
        case Disk: {

            throw new UnsupportedOperationException();
        
        }
        
        default:
            
            throw new AssertionError();
        
        }
        
    }

    /*
     * Stubs for low level operations reading or writing on the store. These
     * operations all assume that they are running within a single thread. That
     * assumption could be made stronger by attaching the permission to a writer
     * thread using a thread local variable. There also must be a means to lock
     * out the writer during shutdown.
     */
    
    private void assertOpen() {
        
        if( ! _bufferStrategy.isOpen() ) {

            throw new IllegalStateException();

        }
        
    }
    
    /**
     * Insert the data into the store and associate it with the persistent
     * identifier from which the data can be retrieved. If there is an entry for
     * that persistent identifier within the transaction scope, then its data is
     * logically overwritten. The written version of the data will not be
     * visible outside of this transaction until the transaction is committed.
     * 
     * @param tx
     *            The transaction identifier.
     * @param id
     *            The int64 persistent identifier.
     * @param data
     *            The data to be written. The bytes from
     *            {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}
     *            will be written.
     * 
     * @todo Handle transaction isolation.
     * @todo Update the object index.
     * @todo Update the allocation nodes.
     * @todo optimize by tracking position.
     * 
     * @return The first slot on which the object was written.
     */
    public int write(long tx,long id,ByteBuffer data) {
        
        assert( data != null ); // Note: could treat null or zero len as delete

        assertOpen();
        
        // #of bytes of data that fit in a single slot.
        final int dataSize = slotMath.dataSize;

        // #of bytes to be written.
        int remaining = data.remaining();
        
        // Total size of the data in bytes.
        final int nbytes = remaining;

        /*
         * Make sure that there are enough free slots in the journal to write
         * the data and obtain the first slot on which the data will be written.
         */
        final int nslots = slotMath.getSlotCount(remaining);
        final int firstSlot = releaseSlots( tx, nslots );

        int thisSlot = firstSlot;

        // The priorSlot in the chain and -size for the first slot in the chain.
        int priorSlot = -nbytes;

        int nwritten = 0;
        
        while( remaining > 0 ) {

            // verify valid index.
            assertSlot( thisSlot );
            
            int nextSlot;
            
            final int thisCopy;
            
            if( remaining > dataSize ) {

                /*
                 * Marks the current slot as allocated and returns the next free
                 * slot (the slot on which we will write in the _next_ pass).
                 */
                nextSlot = releaseNextSlot(tx);

                // We will write dataSize bytes into the current slot.
                thisCopy = dataSize;

            } else {

                /*
                 * Mark the last slot on which we write as allocated.
                 * 
                 * @todo We need better encapsulation for the allocation logic.
                 */
                allocationIndex.set(thisSlot);
                
                // This is the last slot for this data.
                nextSlot = LAST_SLOT_MARKER; // Marks the end of the chain.

                // We will write [remaining] bytes into the current slot
                thisCopy = remaining;

            }

            // Verify that this slot has been allocated.
            assert( allocationIndex.get(thisSlot) );
            
            // Set limit on data to be written on the slot.
            data.limit( data.position() + thisCopy );

            _bufferStrategy.writeSlot( thisSlot, priorSlot, nextSlot, data );

            // Update #of bytes remaining in data.
            remaining -= thisCopy;

            // The slot on which we just wrote data.
            priorSlot = thisSlot;
            
            // The next slot on which we will write data.
            thisSlot = nextSlot;

            // Update the src position.
            data.position( data.limit() );

            nwritten++;
            
        }

        assert nwritten == nslots;
        
        // Update the object index.
        mapIdToSlot( tx, id, firstSlot);

        return firstSlot;
        
    }
    
    /**
     * Update the object index for the transaction to map id onto slot.
     * 
     * @param tx
     *            The transaction.
     * @param id
     *            The persistent identifier.
     * @param slot
     *            The first slot on which the current version of the identified
     *            data is written within this transaction scope.
     * 
     * @todo This needs to use a persistence capable data structure for the
     *       object index.
     * 
     * @todo This needs to isolate changes to the object index within the
     *       specified transaction.
     */
    void mapIdToSlot( long tx, long id, int slot ) {
        
        assert slot != -1;

        // Update the object index.
        objectIndex.put(id, slot);
        
    }
    
    /**
     * Return the first slot on which the current version of the data is stored.
     * If the current version of the object for the transaction is stored on the
     * journal, then this returns the first slot on which that object is
     * written. If the object is logically deleted on the journal within the
     * scope visible to the transaction, then {@link DELETED} is returned to
     * indicate that the caller MUST NOT resolve the object against the database
     * (since the current version is deleted). Otherwise {@link NOTFOUND} is
     * returned to indicate that the current version is stored on the database.
     * 
     * @param tx
     *            The transaction.
     * @param id
     *            The persistent identifier.
     * @return The slot, -1 if the identifier is not mapped, and -2 if the
     *         identifier is mapped and has been deleted.
     * 
     * @see #NOTFOUND
     * @see #DELETED
     * 
     * @todo Transactions are not isolated.
     * 
     * @todo Consider simply throwning a specific exception for the "DELETED"
     *       case as this indicates an error (a bad reference was provided by
     *       the application). In contrast, the NOTFOUND condition merely
     *       indicates that the current version is not on the journal and that
     *       the application MUST resolve the object against the database (which
     *       will result either in the current version or a "not found"
     *       semantics, indicating a bad reference).
     */
    int getFirstSlot( long tx, long id ) {

        Integer firstSlot = objectIndex.get(id);
        
        if( firstSlot == null ) return NOTFOUND;
        
        int slot = firstSlot.intValue();
        
        if( slot < 0 ) return DELETED;
        
        return slot;
        
    }
    
    /**
     * Indicates that the current data version for the persistent identifier was
     * not found in the journal's object index (-1). An application should test
     * the database when this is returned since the current version MAY exist on
     * the database.
     */
    public static final int NOTFOUND = -1;
    
    /**
     * Indicates that the current data version for the persistent identifier has
     * been deleted. An application MUST NOT attempt to resolve the persistent
     * identifier against the database since that could result in a read of an
     * earlier version.
     */
    public static final int DELETED = -2;

    /**
     * Indicates the last slot in a chain of slots representing a data version
     * (-1).
     * 
     * @see SlotMath#headerSize
     */
    public static final int LAST_SLOT_MARKER = -1;
    
    /**
     * Ensure that at least this many slots are available for writing data in
     * this transaction. If necessary, triggers the logical deletion of
     * historical data versions no longer accessable to any active transaction.
     * If necessary, triggers the extension of the journal.
     * 
     * @param tx
     *            The transaction. Note that slots written in the same
     *            transaction whose data have since been re-written or deleted
     *            are available for overwrite in that transaction (but not in
     *            any other transaction).
     * 
     * @param nslots
     *            The #of slots required.
     * 
     * @return The first free slot. The slot is NOT mark as allocated by this
     *         call.
     * 
     * @see #releaseNextSlot(long)
     * 
     * @todo Ensure that at least N slots are available for overwrite by this
     *       transaction.
     * @todo Allocation by
     *       {@link #releaseNextSlot(long) should be optimized for fast consumption of
     *       the next N free slots.
     * @todo Update the allocation nodes as slots are released.
     * @todo Reuse slots that were written earlier in this transaction and whose
     *       data have since been either updated or deleted.
     */
    int releaseSlots(long tx,int nslots) {

        _nextSlot = nextFreeSlot( tx );
        
        return _nextSlot;
        
//        // first slot found.  this will be our return value.
//        int firstSlot = -1;
//        
//        // #of slots found.  we can not stop until nfree >= nslots.
//        int nfree = 0;
//        
//        /*
//         * True iff wrapped around the journal once already.
//         * 
//         * Note that the BitSet search methods only allow us to specify a
//         * fromIndex, not a fromIndex and a toIndex.  This means that we need
//         * to do a little more work to detect when we have scanned past our
//         * stopping point (and that BitSet will of necessity scan to the end
//         * of the bitmap).
//         */
//        boolean wrapped = false;
//        
//        // starting point for the search.
//        int fromSlot = this.nextSlot;
//        
//        // We do not search past this slot.
//        final int finalSlot = ( fromSlot == 0 ? slotLimit : fromSlot - 1 ); 
//        
//        while( nfree < nslots ) {
//            
//            if( _nextSlot == slotLimit ) {
//                
//                if( wrapped ) {
//                    
//                } else {
//                
//                    // restart search from front of journal.
//                    _nextSlot = 0;
//
//                    wrapped = true;
//                    
//                }
//                
//            }
//            
//            if( _nextSlot == -1 ) {
//                
//                int nrequired = nslots - nfree;
//                
//                _nextSlot = release(fromSlot, nrequired );
//                
//            }
//            
//            nfree++;
//            
//        }
//        
//        return _nextSlot;
        
    }

    /* Use to optimize release of the next N slots, and where N==1.
    private int[] releasedSlots;
    private int nextReleasedSlot;
    */
    
    /**
     * Marks the current slot as "in use" and return the next slot free in the
     * journal.
     * 
     * @param tx
     *            The transaction scope.
     * 
     * @return The next slot.
     * 
     * @exception IllegalStateException
     *                if there are no free slots. Note that
     *                {@link #releaseSlots(long, int)} MUST be invoked as a
     *                pre-condition to guarentee that the necessary free slots
     *                have been released on the journal.
     * 
     * @todo This is not able to recognize slots corresponding to versions that
     *       were written and then logically deleted within the given
     *       transaction scope. The slots for such versions MAY be reused within
     *       that transaction.
     */
    int releaseNextSlot(long tx) {

        // Required to prevent inadvertant extension of the BitSet.
        assertSlot(_nextSlot);
        
        // Mark this slot as in use.
        allocationIndex.set(_nextSlot);

        return nextFreeSlot( tx );
        
    }
    
    /**
     * Returns the next free slot.
     */
    private int nextFreeSlot( long tx )
    {

        // Required to prevent inadvertant extension of the BitSet.
        assertSlot(_nextSlot);

        // Determine whether [_nextSlot] has been consumed.
        if( ! allocationIndex.get(_nextSlot) ) {
            
            /*
             * The current [_nextSlot] has not been allocated and is still
             * available.
             */
            
            return _nextSlot;
            
        }
        
        /*
         * Note: BitSet returns nbits when it can not find the next clear bit
         * but -1 when it can not find the next set bit. This is because the 
         * BitSet is logically infinite, so there is always another clear bit
         * beyond the current position.
         */
        _nextSlot = allocationIndex.nextClearBit(_nextSlot);
        
        if( _nextSlot == slotLimit ) {
            
            /*
             * No more free slots, try wrapping and looking again.
             * 
             * @todo This scans the entire index -- there is definately some
             * wasted effort there. However, the allocation index structure
             * needs to evolve to support persistence and transactional
             * isolation so this is not worth tuning further at this time.
             */
            
            System.err.println("Journal is wrapping around.");
            
            _nextSlot = allocationIndex.nextClearBit( 0 );
            
            if( _nextSlot == slotLimit ) {

                // The journal is full.
                throw new IllegalStateException("Journal is full");
                
            }
            
        }
        
        return _nextSlot;
        
    }

    /**
     * Release slots on the journal beginning at the specified slot index by
     * logical deletion of historical data versions no longer accessable to any
     * active transaction. If necessary, triggers the extension of the journal.
     * 
     * @param fromSlot
     *            The index of the slot where the release operation will begin.
     * 
     * @param minSlots
     *            The minimum #of slots that must be made available by this
     *            operation.
     * 
     * @return The index of the next free slot.
     */
    int release(int fromSlot, int minSlots ) {
       
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * <p>
     * The next slot that is known to be available. Slot indices begin at 0 and
     * run up to {@link Integer#MAX_VALUE}. A -1 is used to indicate an invalid
     * slot index.
     * </p>
     * 
     * @todo This MUST be initialized on startup. The default is valid only for
     *       a new journal.
     */
    int _nextSlot = 0;

    /**
     * The index of the first slot that MUST NOT be addressed ([0:slotLimit-1]
     * is the valid range of slot indices).
     */
    final int slotLimit;

    /**
     * <p>
     * Map from persistent identifier to the first slot on which the identifier
     * was written.
     * </p>
     * <p>
     * Note: A deleted version is internally coded as a negative slot
     * identifier. The negative value is computed as
     * 
     * <pre>
     * negIndex = -(firstSlot + 1)
     * </pre>
     * 
     * The actual slot where the deleted version was written is recovered using:
     * 
     * <pre>
     * firstSlot = (-negIndex - 1);
     * </pre>
     * </p>
     * 
     * @todo This map is not persistent.
     * @todo This map does not differentiate between data written in different
     *       transactions.
     * @todo This map does is not isolated within transaction scope.
     */
    final Map<Long,Integer> objectIndex = new HashMap<Long,Integer>();

    /**
     * Allocation map of free slots in the journal.
     * 
     * @todo This does not differentiate whether a slot was already written in a
     *       given transaction, making it impossible to overwrite slots already
     *       written in the same transaction.
     *       
     *       @todo This map is not persisent.
     */
    final BitSet allocationIndex;
    
    /**
     * <p>
     * Read the data from the store.
     * </p>
     * <p>
     * This resolves the data by looking up the entry in the object index. If an
     * entry is found, it records the first slot onto which a data version was
     * last written. If more than one slot was required to store the data, then
     * the slots are chained together using their headers. All such slots are
     * read into a buffer, which is then returned to the caller.
     * </p>
     * 
     * @param tx
     *            The transaction scope for the read request.
     * @param id
     *            The persistent identifier.
     * 
     * @return The data. The position will be zero (0). The limit will be the
     *         capacity. The buffer will be entirely filled with the read data
     *         version. A <code>null</code> return indicates that the object
     *         was not found in the journal, in which case the application MUST
     *         attempt to resolve the object against the database (i.e., the
     *         object MAY have been migrated onto the database and the version
     *         logically deleted on the journal).
     * 
     * @exception IllegalArgumentException
     *                if the transaction identifier is bad.
     * @exception IllegalArgumentException
     *                if the persistent identifier is bad.
     * 
     * @todo Perhaps the object index should also store the datum size so that
     *       we can right size the buffer? However, this this interface is
     *       single threaded, we can just reuse the same buffer over and over
     *       and size the buffer to the maximum data length that the journal
     *       will accept.
     * 
     * FIXME Reads that fail on the journal must still be resolved against the
     * read-write database. However, there is a difference between "not found"
     * and "deleted". The caller MUST NOT read through to the database if the
     * data were deleted. This semantic distinction MUST be passed back. Since
     * DELETED may be a very common outcome, it should not be modeled as a
     * thrown exception (the overhead is too high) (but NOTFOUND could be
     * modeled as a thrown exception).
     */
    public ByteBuffer read(long tx,long id) {

        assertOpen();

//        final int headerSize = slotMath.headerSize;
//        final int dataSize = slotMath.dataSize;
        
        final int firstSlot = getFirstSlot(tx, id);

        if( firstSlot == NOTFOUND ) {
            
            return null;
            
        }
        
        if( firstSlot == DELETED ) {

            throw new IllegalArgumentException("Deleted: tx="+tx+", id="+id);

        }
        
        // Verify that this slot has been allocated.
        assert( allocationIndex.get(firstSlot) );

        /*
         * Position the buffer on the current slot and permit reading of the
         * header.
         */
//        int pos = SIZE_JOURNAL_HEADER + slotSize * firstSlot;
//        directBuffer.limit( pos + headerSize );
//        directBuffer.position( pos );
//        
//        int nextSlot = directBuffer.getInt();
//        final int size = -directBuffer.getInt();
//        if( size <= 0 ) {
//            
//            dumpSlot( firstSlot, true );
//            throw new RuntimeException("Journal is corrupt: tx=" + tx + ", id="
//                    + id +", slot=" + firstSlot+" reports size="+size );
//            
//        }
//
//        // Allocate destination buffer to size.
//        ByteBuffer dst = ByteBuffer.allocate(size);
//
//        int remaining = size;
//        
//        /*
//         * We copy no more than the remaining bytes and no more than the data
//         * available in the slot.
//         */
//        int thisCopy = (remaining > dataSize ? dataSize : remaining);
//        
//        // Set limit on source for copy.
//        directBuffer.limit(directBuffer.position() + thisCopy);
//        
//        // Copy data from slot.
//        dst.put(directBuffer);
//        
//        remaining -= thisCopy;
//        
//        assert( remaining >= 0 );

        /*
         * Read the first slot, returning a new buffer with the data from that
         * slot and returning the header information as a side effect.
         */
        final ByteBuffer dst = _bufferStrategy.readFirstSlot(id, firstSlot,
                true, _slotHeader);

        // #of bytes in the data version.
        final int size = dst.capacity();
        
//        // #of bytes that still need to be read.
//        int remaining = size - dst.position();
        
        // The next slot to be read.
        int nextSlot = _slotHeader.nextSlot;
        
        // #of slots read. @todo verify that the #of slots read is the lowest
        // number that would contain the expected data length.
        int slotsRead = 1;
        
        // The previous slot read.
        int priorSlot = firstSlot;

        while( nextSlot != LAST_SLOT_MARKER ) {
            
            // The current slot being read.
            final int thisSlot = nextSlot; 

            // Verify that this slot has been allocated.
            assert( allocationIndex.get(thisSlot) );

            nextSlot = _bufferStrategy.readNextSlot(id, thisSlot, priorSlot,
                    slotsRead, dst);
            
//            // #of bytes to read from this slot (header + data).
//            int thisCopy = (remaining > dataSize ? dataSize : remaining);
//
//            // Verify that this slot has been allocated.
//            assert( allocationIndex.get(thisSlot) );
//
//            // Position the buffer on the current slot and set limit for copy.
//            int pos = SIZE_JOURNAL_HEADER + slotSize * thisSlot;
//            directBuffer.limit( pos + headerSize + thisCopy );
//            directBuffer.position( pos );
//                        
//            // read the header.
//            nextSlot = directBuffer.getInt();
//            int priorSlot2 = directBuffer.getInt();
//            if( thisSlot != firstSlot && priorSlot != priorSlot2 ) {
//                
//                dumpSlot( firstSlot, true );
//                throw new RuntimeException("Journal is corrupt: tx=" + tx
//                        + ", id=" + id + ", size=" + size + ", slotsRead="
//                        + slotsRead + ", slot=" + thisSlot
//                        + ", expected priorSlot=" + priorSlot
//                        + ", actual priorSlot=" + priorSlot2);
//
//            }
//
//            // Copy data from slot.
//            dst.put(directBuffer);
//
//            remaining -= thisCopy;
//
//            priorSlot = thisSlot;
//
//            assert (remaining >= 0);

            priorSlot = thisSlot;

            slotsRead++;
            
        }

        if (dst.limit() != size) {

            throw new RuntimeException("Journal is corrupt: tx=" + tx + ", id="
                    + id + ", size=" + size + ", slotsRead=" + slotsRead + ", expected size="
                    + size + ", actual read=" + dst.limit());
            
        }

        dst.position(0);
        
        return dst;
        
    }

    /**
     * A data structure used to get the header fields from a slot.
     * 
     * @see Journal#readFirstSlot(long, int, com.bigdata.journal.Journal.SlotHeader)
     */
    static class SlotHeader {
        /**
         * The prior slot# or -size iff this is the first slot in a chain of
         * slots for some data version.
         */
        int priorSlot;
        /**
         * The next slot# or {@link Journal#LAST_SLOT_MARKER} iff this is the
         * last slot in a chain of slots for some data version.
         */
        int nextSlot;
    }

    /**
     * A single instance is used by {@link #read(long, long)} since the journal
     * is single threaded.
     */
    final SlotHeader _slotHeader = new SlotHeader();
    
    /**
     * <p>
     * Delete the data from the store.
     * </p>
     * <p>
     * This removes the entry in the object index, making the data no longer
     * visible in this transaction scope.
     * </p>
     * 
     * @param tx
     *            The transaction within which the delete request was made.
     * @param id
     *            The persistent identifier.
     * @exception IllegalArgumentException
     *                if the transaction identifier is bad.
     * @exception IllegalArgumentException
     *                if the persistent identifier is bad.
     * 
     * @todo This implementation does not support transactional isolation or the
     *       reuse of slots first allocated within the same transaction. The
     *       slots occupied by the data are immediately marked as "free" in a
     *       global scope.
     */
    public void delete(long tx,long id) {

        assertOpen();

        // Get the object index entry.
        Integer firstSlot = objectIndex.get(id);

        if( firstSlot == null ) {
            
            throw new IllegalArgumentException("Not found: tx="+tx+", id="+id);
            
        }

        // Convert to int.
        final int firstSlot2 = firstSlot.intValue();
        
        if( firstSlot2 < 0 ) {
            
            /*
             * A negative slot index is used to mark objects that have been
             * deleted from the object index but whose slots have not yet been
             * released.  It is an error to double-delete an object.
             */
            
            throw new IllegalStateException("Already deleted: tx=" + tx
                    + ", id=" + id);
            
        }
        
        // Update the index to store the negative slot index.
        objectIndex.put(id, -(firstSlot+1));

    }
    
    /**
     * Deallocates slots storing a logically deleted data version. The data
     * version MUST have been previsously deleted using
     * {@link #delete(long, long)}
     * 
     * @param tx
     *            The transaction.
     * @param id
     *            The persistent identifier.
     * 
     * @todo This does not return anything and there is no logic to invoke this
     *       method at this time. Possible return values of interest are the
     *       first slot that was deallocated, an array or list of the
     *       deallocated slots, or the #of slots that were deallocated.
     * 
     * @todo Write test.
     */
    void deallocateSlots(long tx, long id) {
        
        // Remove the object index entry, recovering its contents. 
        Integer negIndex = objectIndex.remove(id);
        
        if( negIndex == null ) {

            throw new IllegalArgumentException("Not found: tx="+tx+", id="+id);
            
        }

        // Convert to int.
        final int negIndex2 = negIndex.intValue();
        
        if( negIndex2 >= 0 ) {
            
            /*
             * A negative slot index is used to mark objects that have been
             * deleted from the object index but whose slots have not yet been
             * released.
             */
            
            throw new IllegalStateException("Not deleted: tx=" + tx
                    + ", id=" + id);
            
        }
        
        // Convert back to a non-negative slot index.
        final int firstSlot = (-negIndex - 1);

        // read just the header for the first slot in the chain.
        _bufferStrategy.readFirstSlot(id, firstSlot, false, _slotHeader);

        // mark slot as available in the allocation index.
        allocationIndex.clear(firstSlot);
        
        int nextSlot = _slotHeader.nextSlot; 

        int priorSlot = firstSlot;
        
        int slotsRead = 1;
        
        while( nextSlot != LAST_SLOT_MARKER ) {

            final int thisSlot = nextSlot;
            
            nextSlot = _bufferStrategy.readNextSlot(id, thisSlot, priorSlot,
                    slotsRead, null);
            
            priorSlot = thisSlot;
            
            // mark slot as available in the allocation index.
            allocationIndex.clear(thisSlot);
            
            slotsRead++;
            
        }

        System.err.println("Dealloacted " + slotsRead + " slots: tx=" + tx
                + ", id=" + id);
        
    }

    /**
     * Prepare the transaction for a {@link #commit(long tx)}.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public void prepare(long tx) {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Commit the transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @exception IllegalStateException
     *                if the transaction has not been
     *                {@link #prepare(long tx) prepared}.
     */
    public void commit(long tx) {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Abort the transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public void abort(long tx) {
        
        throw new UnsupportedOperationException();
        
    }
    
}
