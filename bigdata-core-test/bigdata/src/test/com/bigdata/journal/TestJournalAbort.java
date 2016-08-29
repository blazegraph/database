package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.InnerCause;

/**
 * Test suite for a failure to handle errors inside of abort() by marking the
 * journal as requiring abort().
 * 
 * @see #1021 (Add critical section protection to AbstractJournal.abort() and
 *      BigdataSailConnection.rollback())
 * 
 * @author martyncutcher
 * 
 * TODO Thia should be a proxied test suite. It is RWStore specific.
 */
public class TestJournalAbort extends TestCase2 {

    /**
     * 
     */
    public TestJournalAbort() {
    }

    /**
     * @param name
     */
    public TestJournalAbort(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();
        
    }

    @Override
    public void tearDown() throws Exception {

        TestHelper.checkJournalsClosed(this);
        
        super.tearDown();
    }
    
    @Override
    public Properties getProperties() {
		File file;
		try {
			file = File.createTempFile(getName(), Options.JNL);
			file.deleteOnExit();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		final Properties properties = new Properties();

		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

		properties.setProperty(Options.FILE, file.toString());

        properties.setProperty(Journal.Options.INITIAL_EXTENT, ""
                + Bytes.megabyte * 10);
        
        return properties;
    	
    }
    
    static private class AbortException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		AbortException(String msg) {
    		super(msg);
    	}
    }
    
    /**
     * In this test we want to run through some data inserts, commits and aborts.
     * 
     * The overridden Journal will fail to abort correctly by overriding
     * the discardcommitters method that AbstractJournal calls after calling bufferStragey.reset().
     * 
     * @throws InterruptedException
     */
    public void test_simpleAbortFailure() throws InterruptedException {
    	
    	// Define atomic to control whether abort should succeed or fail
    	final AtomicBoolean succeed = new AtomicBoolean(true);

        final Journal jnl = new Journal(getProperties()) {
            @Override
        	protected void discardCommitters() {

        		if (succeed.get()) {
        			super.discardCommitters();
        		} else {
        			throw new AbortException("Something wrong");
        		}

        	}      	
        };
        
        final RWStrategy strategy = (RWStrategy) jnl.getBufferStrategy();
        final RWStore store = strategy.getStore();
        
        final String btreeName = "TestBTreeAbort";
                
        // 1) Create and commit some data
        // 2) Create more data and Abort success
        // 4) Create and commit more data (should work)
        // 3) Create more data and Abort fail
        // 4) Create and commit more data (should fail)
        
        BTree btree = createBTree(jnl);
        
        jnl.registerIndex(btreeName, btree);
        
        btree.writeCheckpoint();
        jnl.commit();
        
        System.out.println("Start Commit Counter: " + jnl.getCommitRecord().getCommitCounter());
        // 1) Add some data and commit
        addSomeData(btree);
        btree.writeCheckpoint();
        jnl.commit();
        System.out.println("After Data Commit Counter: " + jnl.getCommitRecord().getCommitCounter());
        
        btree.close(); // force re-open
        
        btree = jnl.getIndex(btreeName);
        
        addSomeData(btree);
        btree.writeCheckpoint();
        jnl.commit();
        

        // Show Allocators
        final StringBuilder sb1 = new StringBuilder();
        store.showAllocators(sb1);       
        
        if(log.isInfoEnabled()) log.info(sb1.toString());

        // 2) Add more data and abort
        if(log.isInfoEnabled()) log.info("Pre Abort Commit Counter: " + jnl.getCommitRecord().getCommitCounter());
        btree.close(); // force re-open
        addSomeData(btree);
        btree.writeCheckpoint();
        jnl.abort();
        if(log.isInfoEnabled()) log.info("Post Abort Commit Counter: " + jnl.getCommitRecord().getCommitCounter());
        
        btree.close(); // force re-open after abort
        btree = jnl.getIndex(btreeName);
        
        // Show Allocators again (should be the same visually)
        final StringBuilder sb2 = new StringBuilder();
        store.showAllocators(sb2);
        
        if(log.isInfoEnabled()) log.info("After Abort\n" + sb2.toString());
        
        // 3) More data and commit
        addSomeData(btree);
        btree.writeCheckpoint();
        jnl.commit();
        
        // Show Allocators
        final StringBuilder sb3 = new StringBuilder();
        store.showAllocators(sb3);       
        if(log.isInfoEnabled()) log.info("After More Data\n" + sb3.toString());

        // 4) More data and bad abort
        addSomeData(btree);
        btree.writeCheckpoint();
        succeed.set(false);
        try {
        	jnl.abort();
        	fail();
        } catch (Exception e) {
        	// Check the Abort was Aborted
        	assertTrue(InnerCause.isInnerCause(e, AbortException.class));
        	// good, let's see what state it is in now
       }
        
       btree.close();
        
        // 5) More data and bad commit (after bad abort)
        final BTree btree2 = jnl.getIndex(btreeName); // Note: BTree was marked as invalid. Must be reloaded.
        assertTrue(btree != btree2); // Must be different references.
        btree = btree2;
        try {
	        addSomeData(btree);
	        btree.writeCheckpoint();
	        jnl.commit();
	        fail();
        } catch (Throwable e) {
        	
        	if(log.isInfoEnabled()) log.info("Expected exception", e);
        	
            succeed.set(true);
	        jnl.abort(); // successful abort!
        }
        
        btree = jnl.getIndex(btreeName);

        // 6) More data and good commit (after good abort)
        addSomeData(btree);
        btree.writeCheckpoint();
        jnl.commit();
    }
    
    private void addSomeData(final BTree btree) {
    	
    	final Random r = new Random();
    	
    	for (int n = 0; n < 2000; n++) {
        	final byte[] key = new byte[64];
        	final byte[] value = new byte[256];
	    	r.nextBytes(key);
	    	r.nextBytes(value);
	        btree.insert(key, value);
    	}	
	}

	private BTree createBTree(final Journal store) {
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        return BTree.create(store, metadata);
    }

}
