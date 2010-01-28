package com.bigdata.rwstore;

import java.nio.ByteBuffer;
import java.util.Properties;

import com.bigdata.journal.Journal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.sparse.SparseRowStore;

/**
 * The RWJournal ensures it is backed by the RWStore and uses its methods to implement low-level read and write methods
 * 
 * @author mgc
 *
 */
public class RWJournal extends Journal {

	RWStore m_store = null;
	
	public RWJournal(Properties properties) {
		super(properties);
		
		String jrnl = properties.getProperty(Options.FILE); // all we need for now
		
		m_store = new RWStore(jrnl, false);
	}
	
    public long write(ByteBuffer data) {
    	return write(data, 0);
    }
    
    /**
     * must return an encoded address with the size of the allocated data
     */
    public long write(ByteBuffer data, long oldAddr) {
    	return -1;
    }
    
    /**
     * can decode the address and data size and read into direct buffer
     */
    public ByteBuffer read(long addr) {
    	return null;
    }
    
    // just free from store
    public void delete(long addr) {
    	m_store.free(addr);
    }
	
}
