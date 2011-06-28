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
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ValidationError;
import com.bigdata.util.InnerCause;

/**
 * This example shows how to wrap a B+Tree with a skin which makes it thread
 * safe for concurrent readers and writers (the underlying B+Tree implementation
 * is already thread-safe for concurrent readers, but is single-threaded for
 * mutation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadWriteIndexTxExample {

    public static void main(String[] args) throws InterruptedException,
            ExecutionException {

        final Properties properties = new Properties();

        properties.setProperty(Journal.Options.FILE, "testJournal.jnl");
        
        final Journal store = new Journal(properties);

        try {

            /*
             * Register the index. Each store can hold multiple named indices.
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
                
                // commit the store so the index is on record.
                store.commit();
                
            }

            /*
             * Run a set of concurrent tasks. Each task executes within its own
             * transaction. Conflicts between the transactions are increasingly
             * likely as the #of transactions or the #of operations per
             * transaction increases. When there is a conflict, the transaction
             * for which the conflict was detected will be aborted when it tries
             * to commit.
             */
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

            final int ntx = 30;
            final int nops = 10;
            
            for(int i=0; i<ntx; i++) {

                tasks.add(new ReadWriteTxTask(store, "testIndex", nops));
                
            }

            // run tasks on the journal's executor service.
            final List<Future<Void>> futures = store.getExecutorService()
                    .invokeAll(tasks);

            int i = 0;
            int nok = 0;
            for (Future<Void> future : futures) {

                // check for errors.
                try {
                    future.get();
                    nok++;
                } catch (ExecutionException ex) {
                    if (InnerCause.isInnerCause(ex, ValidationError.class)) {
                        /*
                         * Normal exception. There was a conflict and one or the
                         * transactions could not be committed.
                         */
                        System.out
                                .println("Note: task["
                                        + i
                                        + "] could not be committed due to a write-write conflict.");
                    } else {
                        // Unexpected exception.
                        throw ex;
                    }
                }
                i++;
                
            }

            /*
             * Show #of transactions which committed successfully and the #of
             * transactions which were executed.
             */
            System.out.println("" + nok + " out of " + tasks.size()
                    + " transactions were committed successfully.");

            /*
             * Show the operations executed and #of tuples in the B+Tree.
             */
            System.out.println("nops=" + (nops * tasks.size())
                    + ", rangeCount="
                    + store.getIndex("testIndex").rangeCount());

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
    static class ReadWriteTxTask implements Callable<Void> {

        final Journal jnl;
        final String indexName;
        final int nops;
        final Random r = new Random();
        
        // #of distinct keys for this task.
        final int range = 1000;

        /**
         * 
         * @param store
         *            The journal.
         * @param indexName
         *            The name of the index.
         * @param nops
         *            The #of operations to execute against that {@link BTree}.
         */
        public ReadWriteTxTask(final Journal jnl, final String indexName,
                final int nops) {

            this.jnl = jnl;
            this.indexName = indexName;
            this.nops = nops;

        }

        /**
         * Starts a read-write transaction, obtains a view of the B+Tree
         * isolated by the transaction, performs a series of operations on the
         * isolated view, and then commits the transaction.
         * <p>
         * Note: When multiple instances of this task are run concurrently it
         * becomes increasingly likely that a write-write conflict will be
         * detected when you attempt to commit the transaction, in which case
         * the commit(txid) will fail and an appropriate error will be thrown
         * out of the task.
         * 
         * @throws ValidationError
         *             if there is a write-write conflict during commit
         *             processing (the transaction write set conflicts with the
         *             write set of a concurrent transaction which has already
         *             successfully committed).
         * @throws Exception
         */
        public Void call() throws Exception {

            // Start a transaction.
            final long txid = jnl.newTx(ITx.UNISOLATED);

            try {

                /*
                 * Obtain a view of the index isolated by the transaction.
                 */
                final IIndex ndx = jnl.getIndex(indexName, txid);

                for (int i = 0; i < nops; i++) {

                    switch (r.nextInt(4)) {
                    case 0:
                        /*
                         * write on the index, inserting or updating the value
                         * for the key.
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
                        ndx.lookup("key#" + r.nextInt(range));
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
                        while (itr.hasNext()) {
                            itr.next();
                        }
                        break;
                    }
                    default:
                        throw new AssertionError("case not handled");
                    }

                }

            } catch (Throwable t) {

                jnl.abort(txid);

                throw new RuntimeException(t);
                
            }

            /*
             * Commit the transaction. if the commit fails, then the transaction
             * is aborted.
             */
            jnl.commit(txid);

            // done.
            return null;
            
        }
        
    }

}
