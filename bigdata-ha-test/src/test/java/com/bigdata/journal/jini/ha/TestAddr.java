package com.bigdata.journal.jini.ha;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.rwstore.FixedAllocator;

public class TestAddr extends TestCase {

    private static final Logger log = Logger.getLogger(TestAddr.class);
    
    static final int OFFSET_BITS = 13;

    private int decodeSize(final long addr) {

        final int size = (int) (addr & 0xFFFFFFFF);
        
        log.warn("size=" + size + ", addr=" + addr);
        
        return size;
        
    }

    private int decodeAddr(long addr) {

        addr >>= 32;

        final int latchedAddr = (int) addr;
        
log.warn("latchedAddr="+latchedAddr+", addr="+addr);
        
        return latchedAddr;
        
    }

    /**
     * Return the byte offset in the file.
     * 
     * @param addr
     *            The latched address.
     * 
     * @return The byte offset in the file.
     */
    final private long physicalAddress(final int addr) {

        if (addr >= 0) {

            final long a = addr & 0xFFFFFFE0;

log.warn("physicalAddr="+a+", addr="+addr);
            
            return a;
            
        } else {

            getBlock(addr);
            
            throw new UnsupportedOperationException();
                        
//            // Find the allocator.
//            final FixedAllocator allocator = getBlock(addr);
//
//            // Get the bit index into the allocator.
//            final int offset = getOffset(addr);
//
//            // Translate the bit index into a byte offset on the file.
//            final long laddr = allocator.getPhysicalAddress(offset, nocheck);

//            return laddr;
        }

    }
    

    /**
     * Get the {@link FixedAllocator} for a latched address.
     * 
     * @param addr
     *            The latched address.
     */
    private void getBlock(final int addr) {
        
        // index of the FixedAllocator for that latched address.
        final int index = (-addr) >>> OFFSET_BITS;
log.warn("blockIndex="+index+", addr="+addr);        
//        if (index >= m_allocs.size()) {
//            throw new PhysicalAddressResolutionException(addr);
//        }
//
//        // Return the FixedAllocator for that index.
//        return m_allocs.get(index);
    }

    private void doDecode(final long addr) {
        
        log.warn("externalAddr=" + addr);

        final int size = decodeSize(addr);
        
        final int latchedAddr = decodeAddr(addr);
        
        final long latchedAddr2 = latchedAddr;

        physicalAddress((int) latchedAddr2);

    }
    
    public void test_addrCheckpoint() {

        final long addrCheckpoint = -35205846925092L;

        doDecode(addrCheckpoint);
        
    }
    
    public void test_addrMetadata() {

        final long addrMetadata = -70385924045657L;
        
        doDecode(addrMetadata);
        
    }
    
}
