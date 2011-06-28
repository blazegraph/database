/*
 * Created on Jul 10, 2009
 */

package com.bigdata.samples.btree;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.journal.Journal;

/**
 * This example shows how to wrap a B+Tree with a skin which makes it thread
 * safe for concurrent readers and writers (the underlying B+Tree implementation
 * is already thread-safe for concurrent readers, but is single-threaded for
 * mutation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadWriteIndexExample {

    public static void main(String[] args) throws InterruptedException,
            ExecutionException {

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.FILE, "testJournal.jnl");
        
        final Journal store = new Journal(properties);

        try {

            /*
             * Register the index. There are a lot of options for the B+Tree,
             * but you only need to specify the index name and the UUID for the
             * index. Each store can hold multiple named indices.
             */
            {
                
                final IndexMetadata indexMetadata = new IndexMetadata(
                        "testIndex", UUID.randomUUID());

                store.registerIndex(indexMetadata);
                
                // commit the store so the index is on record.
                store.commit();
                
            }

            /*
             * Demonstrate that the each view has read/write concurrency against
             * the mutable B+Tree.
             */
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

            final UnisolatedReadWriteIndex btree = new UnisolatedReadWriteIndex(
                    store.getIndex("testIndex"));

            final int nops = 50000;

            tasks.add(new ReadWriteTask(btree, nops));

            tasks.add(new ReadWriteTask(btree, nops));

            tasks.add(new ReadWriteTask(btree, nops));

            // run tasks on the journal's executor service.
            final List<Future<Void>> futures = store.getExecutorService()
                    .invokeAll(tasks);

            for(Future<Void> future : futures) {
                
                // check for errors.
                future.get();
                
            }
            
            // commit the store.
            store.commit();
            
            // show #of operations executed and #of tuples in the B+Tree.
            System.out.println("nops=" + (nops * tasks.size())
                    + ", rangeCount=" + btree.rangeCount());

            System.out.println(new Date().toString());

        } finally {

            // destroy the backing store.
            store.destroy();

        }

    }
    
    /**
     * Task performs random CRUD operations, range counts, and range scans.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class ReadWriteTask implements Callable<Void> {

        final IIndex ndx;
        final int nops;
        final Random r = new Random();
        
        // #of distinct keys for this task.
        final int range = 1000;

        /**
         * 
         * @param ndx
         *            A B+Tree which serializes write operations but permits
         *            concurrent read operations when no write operations are
         *            running.
         * @param nops
         *            The #of operations to execute against that {@link BTree}.
         */
        public ReadWriteTask(final UnisolatedReadWriteIndex ndx, final int nops) {
            
            /*
             * Obtain two views onto the same
             */
            this.ndx = ndx;

            this.nops = nops;

        }
        
        public Void call() throws Exception {
            
            for(int i=0; i<nops; i++) {
             
                switch (r.nextInt(4)) {
                case 0:
                    /*
                     * write on the index, inserting or updating the value for
                     * the key.
                     */
                    ndx.insert("key#" + r.nextInt(range), r.nextLong());
                    break;
                case 1:
                    /* write on the index, removing the key iff found. */
                    ndx.remove("key#" + r.nextInt(range));
                    break;
                case 2:
                    /*
                     * lookup a key in the index.
                     */
                    ndx.lookup("key#"+r.nextInt(range));
                    break;
                case 3:
                    /*
                     * range count the index.
                     */
                    ndx.rangeCount();
                    break;
                case 4: {
                    /*
                     * run a range iterator over the index.
                     */
                    final Iterator<ITuple<?>> itr = ndx.rangeIterator();
                    while(itr.hasNext()) {
                        itr.next();
                    }
                    break;
                    }
                default:
                    throw new AssertionError("case not handled");
                }
                
            }
            
            // done.
            return null;
            
        }
        
    }

}
