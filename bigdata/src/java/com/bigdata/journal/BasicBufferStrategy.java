package com.bigdata.journal;

import java.nio.ByteBuffer;


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

  public void writeSlot(int slot, ByteBuffer data) {

      assertSlot(slot);
      assert data != null;

      // Position the buffer on the current slot.
      final int pos = journalHeaderSize + slotSize * slot;
      directBuffer.limit( pos + slotSize );
      directBuffer.position( pos );

      // Write the slot data, advances data.position().
      directBuffer.put(data);

  }

  public ByteBuffer readSlot(int slot, ByteBuffer dst ) {

      assertSlot(slot);
      assert dst != null;
      
      final int remaining = dst.remaining();

      assert remaining <= slotSize;
      
      /*
       * Setup the source (limit and position).
       */
      final int pos = journalHeaderSize + slotSize * slot;
      directBuffer.limit( pos + remaining );
      directBuffer.position( pos );
      
      /*
       * Copy data from slot.
       */
//      dst.limit(dst.position() + thisCopy);
      dst.put(directBuffer);
      
      return dst;

  }

//  public int readNextSlot(int thisSlot, int priorSlot, int slotsRead,
//            int remainingToRead, ByteBuffer dst) {
//
//      // Position the buffer on the current slot and set limit for copy.
//      final int pos = journalHeaderSize + slotSize * thisSlot;
//      directBuffer.limit( pos + slotHeaderSize );
//      directBuffer.position( pos );
//                  
//      // read the header.
//      final int nextSlot = directBuffer.getInt();
//      final int priorSlot2 = directBuffer.getInt();
//      if( priorSlot != priorSlot2 ) {
//          
////          dumpSlot( thisSlot, true );
//          throw new RuntimeException("Journal is corrupt"
//                  + ": slotsRead=" + slotsRead + ", slot=" + thisSlot
//                  + ", expected priorSlot=" + priorSlot
//                  + ", actual priorSlot=" + priorSlot2);
//
//      }
//
//      // Copy data from slot.
//      if( dst != null ) {
//
//          assert remainingToRead > 0;
//          
////          final int size = dst.capacity();
//          
////          final int remaining = size - dst.position();
//          
//          // #of bytes to read from this slot (header + data).
//          final int thisCopy = (remainingToRead > slotDataSize ? slotDataSize
//                  : remainingToRead);
//
//          directBuffer.limit( pos + slotHeaderSize + thisCopy );
//
//          dst.limit(dst.position() + thisCopy);
//
//          dst.put(directBuffer);
//          
//      }
//
//      return nextSlot;
//      
//  }

}
