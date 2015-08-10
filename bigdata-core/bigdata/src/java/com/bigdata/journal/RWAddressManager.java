package com.bigdata.journal;

import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rwstore.RWStore;

/**
 * 
 * FIXME unit tests.
 */
public class RWAddressManager implements IAddressManager {

	RWStore m_store;
	
	public RWAddressManager(final RWStore store) {
		m_store = store;
	}
    public int getByteCount(final long addr) {
        return (int) (addr & 0xFFFFFFFFL);
    }

    public long getOffset(final long addr) {
        return addr >> 32;
        
    }

    public long getPhysicalAddress(final long addr) {
        return m_store.physicalAddress((int) getOffset(addr)) ;   
    }

    public long toAddr(final int nbytes, final long offset) {
        return (offset << 32) + nbytes;
    }

    public String toString(final long addr) {
    	if (m_store == null) {
	        return "{off=NATIVE:" + getOffset(addr) + ",len=" + getByteCount(addr)
	        + "}";
    	} else {
	        return "{off=" + m_store.physicalAddress((int) getOffset(addr)) + ",len=" + getByteCount(addr)
	        + "}";
    	}
    }
    
}