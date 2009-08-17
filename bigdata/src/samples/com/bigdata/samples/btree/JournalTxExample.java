/*
 * Created on Jul 10, 2009
 */

package com.bigdata.samples.btree;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;

/**
 * This example show how to create a {@link Journal}, register a {@link BTree},
 * start a transaction, obtain a B+Tree view isolated by that transaction,
 * perform basic operations on the {@link BTree}, and commit the transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JournalTxExample {

    public static void main(String[] args) {

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.FILE, "testJournal.jnl");
        
        Journal store = new Journal(properties);

        try {

            /*
             * Register the index. There are a lot of options for the B+Tree,
             * but you only need to specify the index name and the UUID for the
             * index. Each store can hold multiple named indices.
             */
            {
                
                final IndexMetadata indexMetadata = new IndexMetadata(
                        "testIndex", UUID.randomUUID());

                /*
                 * Note: You MUST explicitly enable transaction processing for a
                 * B+Tree when you register the index. Transaction processing
                 * requires that the index maintain both per-tuple delete
                 * markers and per-tuple version identifiers. While scale-out
                 * indices always always maintain per-tuple delete markers,
                 * neither local nor scale-out indices maintain the per-tuple
                 * version identifiers by default.
                 */
                indexMetadata.setIsolatable(true);
                
                // register the index.
                store.registerIndex(indexMetadata);
                
                // commit the store so the B+Tree can be found on restart.
                store.commit();
                
            }

            // start a read-write transaction.
            final long txid = store.newTx(ITx.UNISOLATED);

            /*
             * Lookup a view of the B+Tree isolated by that transaction. This
             * will be a mutable view since we requested a read-write
             * transaction.
             */
            {
            
                final IIndex isolatedBTree = store.getIndex("testIndex", txid);

                // lookup the tuple (not found).
                System.err.println("tuple: " + isolatedBTree.lookup("hello"));

                // add a tuple
                isolatedBTree.insert("hello", "world");

                // lookup the tuple
                System.err.println("tuple: " + isolatedBTree.lookup("hello"));

                // update the tuple
                isolatedBTree.insert("hello", "again");

                // lookup the new value
                System.err.println("tuple: " + isolatedBTree.lookup("hello"));

            }

            /*
             * The tuple is not visible on unisolated B+Tree until we commit the
             * transaction.
             */
            {

                final BTree unisolatedBTree = store.getIndex("testIndex");

                final Object val = unisolatedBTree.lookup("hello");
                
                // lookup the tuple (not found).
                System.err.println("tuple: " + val);
                
                assert val == null;

            }

            // Commit the transaction.
            store.commit(txid);

            // Verify writes are now visible on the mutable B+Tree.
            {
                
                final BTree mutableBTree = store.getIndex("testIndex");

                final Object val = mutableBTree.lookup("hello");
                
                // lookup the tuple (found).
                System.err.println("tuple: " + val);
                
                assert "again".equals(val);

            }

            // Show that the changes were restart safe.
            {
                
                // close the journal.
                store.close();
                System.out.println("Store closed.");
                
                // re-open the journal.
                store = new Journal(properties);
                System.out.println("Store re-opened.");

                // lookup the B+Tree.
                final BTree mutableBTree = store.getIndex("testIndex");

                // lookup the tuple.
                final Object val = mutableBTree.lookup("hello");
                
                // lookup the tuple (found).
                System.err.println("tuple: " + val);
                
                assert "again".equals(val);
    
            }
            
        } finally {

            // destroy the backing store.
            store.destroy();

        }

    }

}
