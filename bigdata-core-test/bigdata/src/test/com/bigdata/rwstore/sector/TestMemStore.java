package com.bigdata.rwstore.sector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.AbstractMRMWTestCase;
import com.bigdata.journal.AbstractMROWTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.journal.TestJournalBasics;
import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.service.AbstractTransactionService;

/**
 * Test suite for {@link MemStore}.
 * 
 * @author thompsonbry
 */
public class TestMemStore extends AbstractJournalTestCase {

	public TestMemStore() {
	}

	public TestMemStore(String name) {
		super(name);
	}

	public Properties getProperties() {

        final Properties properties = super.getProperties();

        return setProperties(properties);

	}
	
	static public Properties setProperties(final Properties properties) {

        properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);

        properties.setProperty(Options.BUFFER_MODE, BufferMode.MemStore
                .toString());
        
        /*
         * Make sure that we are not create a backing file. The CREATE_TEMP_FILE
         * option does not require a file name.  So, make sure there is no file
         * name and make sure that we CREATE_TEMP_FILE is not true.
         */
        {

            assertNull(properties.getProperty(Options.FILE));

            if (Boolean
                    .valueOf(properties.getProperty(Options.CREATE_TEMP_FILE,
                            Options.DEFAULT_CREATE_TEMP_FILE))) {

                properties.setProperty(Options.CREATE_TEMP_FILE, "false");
                
            }

        }

		// properties.setProperty(Options.BUFFER_MODE,
		// BufferMode.TemporaryRW.toString());

		// properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		// properties.setProperty(Options.FILE,
		// "/Volumes/SSDData/TestRW/tmp.rw");

		properties.setProperty(Options.DELETE_ON_EXIT, "true");

		// ensure history retention to force deferredFrees
		// properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,
		// "1"); // Non-zero
		
		// Set OVERWRITE_DELETE
		// properties.setProperty(RWStore.Options.OVERWRITE_DELETE, "true");

		return properties;

	}
	
	static Journal getJournal(final Properties props) {
 		return  new Journal(props);
	}

	public static Test suite() {

        final TestMemStore delegate = new TestMemStore(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "MemStore Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // test suite for the IRawStore api.
        suite.addTestSuite( TestRawStore.class );

        // Note: test suite not used since there is no file channel to be closed by interrupts.
//        suite.addTestSuite( TestInterrupts.class );

        // test suite for MROW correctness.
        suite.addTestSuite( TestMROW.class );

        // test suite for MRMW correctness.
        suite.addTestSuite( TestMRMW.class );

        /*
         * Pickup the basic journal test suite. This is a proxied test suite, so
         * all the tests will run with the configuration specified in this test
         * class and its optional .properties file.
         */
        suite.addTest(TestJournalBasics.suite());

        return suite;

    }

    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Transient}.
     * 
     * @throws IOException
     */
    public void test_create_transient01() throws IOException {

		final IRawStore store = new MemStore(DirectBufferPool.INSTANCE, 1/* nbuffers */);
		try {
			assertFalse("isStable", store.isStable());
			assertTrue("isFullyBuffered", store.isFullyBuffered());
		} finally {
			store.destroy();
		}
    }
    
    /**
     * Test suite integration for {@link AbstractRawStoreTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
      */
    public static class TestRawStore extends AbstractRawStoreTestCase {
        
        public TestRawStore() {
            super();
        }

        public TestRawStore(String name) {
            super(name);
        }
        
		/** Note: Properties are not used. */
		public Properties getProperties() {

			return setProperties(new Properties());

		}

		protected IRawStore getStore() {

			return new MemStore(DirectBufferPool.INSTANCE);

        }

		/**
		 * Can be tested by removing RWStore call to journal.removeCommitRecordEntries
		 * in freeDeferrals.
		 * 
		 * final int commitPointsRemoved = journal.removeCommitRecordEntries(fromKey, toKey);
		 * 
		 * replaced with
		 * 
		 * final int commitPointsRemoved = commitPointsRecycled;
		 * 
		 */
		public void testVerifyCommitRecordIndex() {
            final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "400");

			final Journal store = getJournal(properties);
            try {

            	MemStrategy bs = (MemStrategy) store.getBufferStrategy();
            	      	
	        	for (int r = 0; r < 10; r++) {
		        	ArrayList<Long> addrs = new ArrayList<Long>();
		        	for (int i = 0; i < 100; i++) {
		        		addrs.add(bs.write(randomData(45)));
		        	}
		        	store.commit();
		
		        	for (long addr : addrs) {
		        		bs.delete(addr);
		        	}
		
		           	store.commit();
		           	
	        	}
	        	
	        	// Age the history (of the deletes!)
	        	Thread.currentThread().sleep(400);
	        	
	        	verifyCommitIndex(store, 20);
	        	
	        	store.close();
	        	
           } catch (InterruptedException e) {
			} finally {
            	store.destroy();
            }
		}
		
		private void verifyCommitIndex(final Journal jrnl, int expectedRecords) {
		       // Check if journal is DISKRW
	        if (jrnl.getBufferStrategy().getBufferMode() != BufferMode.MemStore) {            
	            System.err.println("Buffer mode should be MemStore not " + jrnl.getBufferStrategy().getBufferMode());
	            
	            return;
	        }
	        
	        final MemStrategy strategy = (MemStrategy) jrnl.getBufferStrategy();
	        
	        final IIndex commitRecordIndex = jrnl.getReadOnlyCommitRecordIndex();
	        if (commitRecordIndex == null) {
	            System.err.println("Unexpected null commit record index");
	            return;
	        }

	        final IndexMetadata metadata = commitRecordIndex
	                .getIndexMetadata();

	        final byte[] zeroKey = metadata.getTupleSerializer()
	                .serializeKey(0L);

	        final byte[] releaseKey = metadata.getTupleSerializer()
            .serializeKey(System.currentTimeMillis());

	        final int removed = jrnl.removeCommitRecordEntries(zeroKey, releaseKey);
	      
	        assertTrue(removed == expectedRecords);

	        jrnl.commit();
		}

		/**
		 * Tests whether tasks are able to access and modify data safely by
		 * emulating transactions by calling activateTx and deactivateTx
		 * directly.
		 */
		public void test_sessionProtection() {

	        final Properties p = getProperties();

	        // Note: No longer the default. Must be explicitly set.
	        p.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, "0");

			final Journal store = getJournal(p);
			try {
				final IRWStrategy bs = (IRWStrategy) store.getBufferStrategy();
	
				final byte[] buf = new byte[300]; // Just some data
				r.nextBytes(buf);
	
				final ByteBuffer bb = ByteBuffer.wrap(buf);
	
				final long faddr = store.write(bb); // rw.alloc(buf, buf.length);
	
				final IRawTx tx = bs.newTx();
	
				ByteBuffer rdBuf = store.read(faddr);
	
				bb.position(0);
	
				assertEquals(bb, rdBuf);
	
				store.delete(faddr); // deletion protected by session
	
				bb.position(0);
	
				rdBuf = store.read(faddr);
	
				// should be able to successfully read from freed address
				assertEquals(bb, rdBuf);
	
				tx.close();
	
				store.commit();
			} finally {
			    store.destroy();
			}
			
		}

		public void test_stressSessionProtection() {
			// Sequential logic

            final Properties p = getProperties();

            // Note: No longer the default. Must be explicitly set.
            p.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, "0");

            final Journal store = getJournal(p);
			try {
				final IRWStrategy bs = (IRWStrategy) store.getBufferStrategy();

				byte[] buf = new byte[300]; // Just some data
				r.nextBytes(buf);

				ByteBuffer bb = ByteBuffer.wrap(buf);

				IRawTx tx = bs.newTx();
				ArrayList<Long> addrs = new ArrayList<Long>();

				// We just want to stress a single allocator, so do not
				//	make more allocations than a single allocator can
				//	handle since this would prevent predictable
				//	recycling

				for (int i = 0; i < 1000; i++) {
					addrs.add(store.write(bb));
					bb.flip();
				}

				for (int i = 0; i < 1000; i += 2) {
					store.delete(addrs.get(i));
				}

				// now release session to make addrs reavailable
				tx.close();

				for (int i = 0; i < 1000; i += 2) {
					long addr = store.write(bb);
					assertTrue(addr == addrs.get(i));
					bb.flip();
				}

				store.commit();

				bb.position(0);

				for (int i = 0; i < 500; i++) {
					bb.position(0);

					ByteBuffer rdBuf = store.read(addrs.get(i));

					// should be able to
					assertEquals(bb, rdBuf);
				}

				store.commit();
			} finally {
				store.destroy();
			}
		}

		public void test_allocCommitFree() {
			final Journal store = getJournal(getProperties());
			try {

				MemStrategy bs = (MemStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	assertTrue(bs.isCommitted(addr));
            } finally {
            	store.destroy();
            }
		}
		
		public void test_allocCommitFreeWithHistory() {
			final Properties properties = new Properties(getProperties());

			properties.setProperty(
					AbstractTransactionService.Options.MIN_RELEASE_AGE, "100");

			final Journal store = getJournal(properties);
			try {

				MemStrategy bs = (MemStrategy) store.getBufferStrategy();

				final long addr = bs.write(randomData(78));

				store.commit();

				bs.delete(addr);

				assertTrue(bs.isCommitted(addr));
			} finally {
				store.destroy();
			}
		}
		
		ByteBuffer randomData(final int sze) {
			return ByteBuffer.wrap(randomBytes(sze));
		}
		
		byte[] randomBytes(final int sze) {
			byte[] buf = new byte[sze + 4]; // extra for checksum
			r.nextBytes(buf);
			
			return buf;
		}
		public void test_blobDeferredFrees() {

			final Properties properties = new Properties(getProperties());

			properties.setProperty(
					AbstractTransactionService.Options.MIN_RELEASE_AGE, "100");

			final Journal store = getJournal(properties);
            try {

				MemStrategy bs = (MemStrategy) store.getBufferStrategy();
            	
            	ArrayList<Long> addrs = new ArrayList<Long>();
            	for (int i = 0; i < 4000; i++) {
            		addrs.add(bs.write(randomData(45)));
            	}
            	store.commit();

            	for (long addr : addrs) {
            		bs.delete(addr);
            	}
                for (int i = 0; i < 4000; i++) {
                    if(!bs.isCommitted(addrs.get(i))) {
                        fail("i="+i+", addr="+addrs.get(i));
                    }
                }

               	store.commit();
               	
            	// Age the history (of the deletes!)
            	Thread.currentThread().sleep(6000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(220)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	for (int i = 0; i < 4000; i++) {
                   	assertFalse(bs.isCommitted(addrs.get(i)));
            	}
              } catch (InterruptedException e) {
			} finally {
            	store.destroy();
            }
		}
   }

    /**
     * Test suite integration for {@link AbstractMROWTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class TestMROW extends AbstractMROWTestCase {
        
        public TestMROW() {
            super();
        }

        public TestMROW(String name) {
            super(name);
		}

		/** Note: Properties are not used. */
		public Properties getProperties() {

			return new Properties();

		}

		protected IRawStore getStore() {

			return new MemStore(DirectBufferPool.INSTANCE);

		}

    }

    /**
     * Test suite integration for {@link AbstractMRMWTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class TestMRMW extends AbstractMRMWTestCase {
        
        public TestMRMW() {
            super();
        }

        public TestMRMW(String name) {
            super(name);
        }

		/** Note: Properties are not used. */
		public Properties getProperties() {

			return new Properties();

		}

		protected IRawStore getStore() {

			return new MemStore(DirectBufferPool.INSTANCE);

        }

    }

}
