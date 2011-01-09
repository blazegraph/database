package com.bigdata.journal;

import com.bigdata.rawstore.IAddressManager;

/**
 * 
 * FIXME unit tests.
 */
public class RWAddressManager implements IAddressManager {

    public int getByteCount(final long addr) {
        return (int) (addr & 0xFFFFFFFFL);
    }

    public long getOffset(final long addr) {
        return addr >> 32;
    }

    public long toAddr(final int nbytes, final long offset) {
        return (offset << 32) + nbytes;
    }

    public String toString(final long addr) {
        return "{off=" + getOffset(addr) + ",len=" + getByteCount(addr)
                + "}";
    }
    
}