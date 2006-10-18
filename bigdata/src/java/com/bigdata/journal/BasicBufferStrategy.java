package com.bigdata.journal;

import java.nio.ByteBuffer;

import com.bigdata.journal.Journal.SlotHeader;

/**
 * Implements logic to read from and write on a buffer. This is sufficient
 * for a {@link BufferMode#Transient} implementation or a
 * {@link BufferMode#Mapped} implementation, but the
 * {@link BufferMode#Direct} implementation needs to also implement write
 * through to the disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BasicBufferStrategy extends AbstractBufferStrategy {

    /**
     * A direct buffer containing a write through image of the backing file.
     */
    final ByteBuffer directBuffer;
    
    /**
     * The current length of the backing file in bytes.
     */
    final long extent;

    public long getExtent() {
        
        return extent;
        
    }

    /**
     * The index of the first slot that MUST NOT be addressed (e.g., nslots).
     */
    final int slotLimit;

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

  BasicBufferStrategy(int journalHeaderSize, BufferMode bufferMode,SlotMath slotMath,ByteBuffer buffer) {
      
      super( journalHeaderSize, bufferMode, slotMath );
      
      this.directBuffer = buffer;
      
      this.extent = buffer.capacity();

      /*
       * The first slot index that MUST NOT be addressed.
       * 
       * Note: The same computation occurs in DiskOnlyStrategy and FileMetadata.
       */

      this.slotLimit = (int) (extent - journalHeaderSize) / slotSize;

      System.err.println("slotLimit=" + slotLimit);

  }

  public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data) {
      
      // Position the buffer on the current slot.
      final int pos = journalHeaderSize + slotSize * thisSlot;
      directBuffer.limit( pos + slotSize );
      directBuffer.position( pos );
      
      // Write the slot header.
      directBuffer.putInt(nextSlot); // nextSlot or -1 iff last
      directBuffer.putInt(priorSlot); // priorSlot or -size iff first

      // Write the slot data, advances data.position().
      directBuffer.put(data);

  }

  public ByteBuffer readFirstSlot(int firstSlot, boolean readData,
          SlotHeader slotHeader, ByteBuffer dst ) {

      assert slotHeader != null;
      
      final int pos = journalHeaderSize + slotSize * firstSlot;
      directBuffer.limit( pos + slotHeaderSize );
      directBuffer.position( pos );
      
      int nextSlot = directBuffer.getInt();
      final int size = -directBuffer.getInt();
      if( size <= 0 ) {
          
//          dumpSlot( firstSlot, true );
          throw new RuntimeException("Journal is corrupt" + ": firstSlot="
                    + firstSlot + " reports size=" + size);
          
      }

      // Copy out the header fields.
      slotHeader.nextSlot = nextSlot;
      slotHeader.priorSlot = -size;

      if( ! readData ) return null;
      
      /*
       * Verify that the destination buffer exists and has sufficient
       * remaining capacity.
       */
      if (dst == null || dst.remaining() < size) {

          // Allocate a destination buffer to size.
          dst = ByteBuffer.allocate(size);

      }
      
      /*
       * We copy no more than the remaining bytes and no more than the data
       * available in the slot.
       */
      
      final int thisCopy = (size > slotDataSize ? slotDataSize : size);
      
      /*
       * The source should have its position at the start of the slot data.
       * Now we set the limit on source for copy of that data.
       */
      directBuffer.limit( pos + slotHeaderSize + thisCopy);
      
      // Copy data from slot.
      dst.limit(dst.position() + thisCopy);
      dst.put(directBuffer);
      
      return dst;

  }

  public int readNextSlot(int thisSlot, int priorSlot, int slotsRead,
            int remainingToRead, ByteBuffer dst) {

      // Position the buffer on the current slot and set limit for copy.
      final int pos = journalHeaderSize + slotSize * thisSlot;
      directBuffer.limit( pos + slotHeaderSize );
      directBuffer.position( pos );
                  
      // read the header.
      final int nextSlot = directBuffer.getInt();
      final int priorSlot2 = directBuffer.getInt();
      if( priorSlot != priorSlot2 ) {
          
//          dumpSlot( thisSlot, true );
          throw new RuntimeException("Journal is corrupt"
                  + ": slotsRead=" + slotsRead + ", slot=" + thisSlot
                  + ", expected priorSlot=" + priorSlot
                  + ", actual priorSlot=" + priorSlot2);

      }

      // Copy data from slot.
      if( dst != null ) {

          assert remainingToRead > 0;
          
//          final int size = dst.capacity();
          
//          final int remaining = size - dst.position();
          
          // #of bytes to read from this slot (header + data).
          final int thisCopy = (remainingToRead > slotDataSize ? slotDataSize
                  : remainingToRead);

          directBuffer.limit( pos + slotHeaderSize + thisCopy );

          dst.limit(dst.position() + thisCopy);

          dst.put(directBuffer);
          
      }

      return nextSlot;
      
  }

//  /**
//   * Utility shows the contents of the slot on stderr.
//   * 
//   * @param slot
//   *            The slot.
//   * @param showData
//   *            When true, the data in the slot will also be dumped.
//   * 
//   * @todo Abstract, remove dumpSlot, or make it impl specific (more code
//   * duplication).  There are only two implementations required.  One for
//   * the buffer modes and one for the disk only mode.
//   */
//  void dumpSlot(int slot, boolean showData) {
//      
//      System.err.println("slot="+slot);
//      assertSlot( slot );
//      
//      ByteBuffer view = directBuffer.asReadOnlyBuffer();
//      
//      int pos = journalHeaderSize + slotSize * slot;
//
//      view.limit( pos + slotSize );
//
//      view.position( pos );
//
//      int nextSlot = view.getInt();
//      int priorSlot = view.getInt();
//
//      System.err.println("nextSlot="
//              + nextSlot
//              + (nextSlot == -1 ? " (last slot)"
//                      : (nextSlot < 0 ? "(error: negative slotId)"
//                              : "(more slots)")));
//      System.err.println(priorSlot<0?"size="+(-priorSlot):"priorSlot="+priorSlot);
//      
//      if( showData ) {
//      
//          byte[] data = new byte[slotDataSize];
//
//          view.get(data);
//          
//          System.err.println(Arrays.toString(data));
//          
//      }
//      
//  }

}
