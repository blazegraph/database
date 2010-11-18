/*
 * Created on Jul 10, 2009
 */

package com.bigdata.samples.btree;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.service.AbstractTransactionService;

/**
 * This example illustrates how to safely interact with a zero retention
 * configuration of the RWStore using a mixture of read-only transactions
 * and unisolated index writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: JournalTxExample.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public class JournalReadOnlyTxExample {
	
	private static final Logger log = Logger.getLogger(JournalReadOnlyTxExample.class);

    public static void main(String[] args) throws IOException {

    	// enable logging
    	log.setLevel(Level.INFO);
    	
        final Properties properties = new Properties();

		final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

		if (tmpDir.exists()) {
			tmpDir.mkdirs();
		}

		final File journalFile = new File(tmpDir,
				JournalReadOnlyTxExample.class.getSimpleName() + "jnl");

		if (journalFile.exists()) {
			if (!journalFile.delete()) {
				throw new IOException("Could not delete old file: "
						+ journalFile);
			}
		}

		// this example uses the RWStore, but it also works with the WORM.
		properties.setProperty(Journal.Options.BUFFER_MODE, //
				BufferMode.DiskRW.toString()
//				BufferMode.DiskWORM.toString()
				);

		properties.setProperty(Journal.Options.FILE, journalFile
				.getCanonicalPath());

		/*
		 * Immediate recycling of committed data unless protected by a read-lock
		 * (ZERO milliseconds of retained data associated historical commit
		 * points).
		 * 
		 * Note: this is the default behavior for the RWStore and the bigdata
		 * federation.
		 */
		properties.setProperty(
				AbstractTransactionService.Options.MIN_RELEASE_AGE, "0");

		Journal store = new Journal(properties);

		// The name of the index used in this example.
		final String name = "testIndex";
		
		try {

            /*
             * Register the index. Each store can hold multiple named indices.
             */
        	final long commitTime1;
            {
                
                final IndexMetadata indexMetadata = new IndexMetadata(
                        name, UUID.randomUUID());

                // Note: isolatable indices are NOT required for read-only txs.
                indexMetadata.setIsolatable(false);
                
                // register the index.
                store.registerIndex(indexMetadata);
                
                // commit the store so the B+Tree can be found on restart.
                commitTime1 = store.commit();
                
            }

            /*
             * Write data using the UNISOLATED view of the B+Tree.
             */
            {

                final BTree unisolatedBTree = store.getIndex(name);

                writeSet1(unisolatedBTree);

                verifyWriteSet1(unisolatedBTree);

                /*
                 * Note: The unisolated index has not been committed yet.
                 */
                
            }

			/*
			 * The data are not visible on read-only B+Tree reading from the
			 * most recent commit point.
			 */
			{

				final long tx1 = store.newTx(ITx.READ_COMMITTED);

				try {

					final BTree readOnlyBTree = (BTree) store.getIndex(name,
							tx1);

					verifyWriteSetNotFound(readOnlyBTree);

				} finally {

					store.abort(tx1);

            	}

            }

            // Commit the write set for the UNISOLATED index.
            final long commitTime2 = store.commit();

            // Verify writes are now visible on the UNISOLATED B+Tree.
            {
                
                final BTree unisolatedBTree = store.getIndex(name);

                verifyWriteSet1(unisolatedBTree);

            }
            
			// Show that the changes were restart safe.
			if (true) {

				// close the journal.
				store.close();
				log.info("Store closed.");

				// re-open the journal.
				store = new Journal(properties);
				log.info("Store re-opened.");

				// lookup the B+Tree.
				final BTree unisolatedBTree = store.getIndex(name);

				verifyWriteSet1(unisolatedBTree);

			}

            /*
             * Open a read-only transaction on the last commit time.
             * 
             * Note: If you use store.getLastCommitTime() here instead you will
             * have a read-historical view of the same data, but that view is
             * NOT protected by a read lock. Running the example with this
             * change will cause the RWStore to throw out an exception since the
             * writes will have overwritten the historical data by the time you
             * try to read it.
             */
			// Obtaining a tx here protects against recycling.
            final long tx2 = store.newTx(ITx.READ_COMMITTED);
            // Using a historical read w/o a tx does NOT protect against recycling.
//            final long tx2 = store.getLastCommitTime();
            try {

                // lookup the UNISOLATED B+Tree.
                final BTree unisolatedBTree = store.getIndex(name);

                // First, remove the existing tuples.
                removeWriteSet(unisolatedBTree);

                store.commit();
                
				/*
				 * Write some new records on the unisolated index.
				 */
				writeSet2(unisolatedBTree);

				store.commit();

                /*
                 * Verify that the read-only view has not seen those changes.
                 * 
                 * Note: If you used a historical read rather than a read-only
                 * tx then this is where the RWStore will throw out an exception
                 * because the recycled has reused some of the records
                 * associated with the historical revision of the BTree.
                 */
				{
					final BTree readOnlyBTree = (BTree) store.getIndex(name, tx2);

					verifyWriteSet1(readOnlyBTree);
				}

			} finally {
				// release that read-only transaction.
				store.abort(tx2);
            }
            
			log.info("Done.");
			
        } finally {

            // destroy the backing store.
            store.destroy();

        }

    }

	/**
	 * Write a set of tuples having keys in [0:1000) and values equal to the
	 * keys.
	 */
	private static void writeSet1(BTree btree) {

		log.info("");
		
		for (int i = 0; i < 1000; i++) {

			btree.insert(i, i);

		}

	}

	/**
	 * Write a set of tuples having keys in [0:1000) and values equal to the
	 * twice the numerical value of the keys.
	 * 
	 * @param btree
	 */
	private static void writeSet2(BTree btree) {

		log.info("");

		for (int i = 0; i < 1000; i++) {

			btree.insert(i, i * 2);

		}

	}

	/**
	 * Verify the set of tuples written by {@link #writeSet1(BTree)}.
	 */
	private static void verifyWriteSet1(BTree btree) {

		log.info("");

		for (int i = 0; i < 1000; i++) {

			final Object val = btree.lookup(i);

			if (!Integer.valueOf(i).equals(val)) {

				throw new RuntimeException("Not found: key=" + i+", val="+val);

			}

		}

	}

	/**
	 * Verify that the write set is not found (no keys in [0:1000)).
	 */
	private static void verifyWriteSetNotFound(BTree btree) {

		log.info("");

		for (int i = 0; i < 1000; i++) {

			if (btree.contains(i)) {

				throw new RuntimeException("Not expecting: key=" + i);

			}

		}

	}
	
	/**
	 * Delete the tuples written by {@link #writeSet(BTree)} or
	 * {@link #writeSet2(BTree).
	 */
	private static void removeWriteSet(BTree btree) {

		log.info("");

		for (int i = 0; i < 1000; i++) {

			if (btree.remove(i) == null) {

				throw new RuntimeException("Not found: key=" + i);

			}

		}

	}

}
