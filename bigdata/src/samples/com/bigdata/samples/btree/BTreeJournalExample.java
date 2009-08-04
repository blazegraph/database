/*
 * Created on Jul 10, 2009
 */

package com.bigdata.samples.btree;

import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;

/**
 * Sample for for using the {@link BTree} with a {@link Journal}. The
 * {@link Journal} is the right choice when you want a standalone persistence
 * store. Both the {@link TemporaryStore} and the {@link Journal} are
 * thread-safe for concurrent readers and writers. However, the {@link Journal}
 * has a commit protocol and has support for concurrency control. This example
 * shows you how to use the {@link BTree} on the {@link Journal} without any
 * concurrency controls.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTreeJournalExample {

    public static void main(String[] args) {

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
             * Lookup the unisolated B+Tree. This is the mutable B+Tree view.
             * 
             * While the temporary store does not differentiate between mutable
             * and read-only views, the Journal and the scale-out architecture
             * do.
             */
            {
            
                final BTree mutableBTree = store.getIndex("testIndex");

                // lookup the tuple (not found).
                System.err.println("tuple: " + mutableBTree.lookup("hello"));

                // add a tuple
                mutableBTree.insert("hello", "world");

                // lookup the tuple
                System.err.println("tuple: " + mutableBTree.lookup("hello"));

                // update the tuple
                mutableBTree.insert("hello", "again");

                // lookup the new value
                System.err.println("tuple: " + mutableBTree.lookup("hello"));

                /*
                 * The tuple is not visible on the read-committed view until the
                 * next commit.
                 */
                {

                    final ILocalBTreeView readOnlyBTree = store.getIndex(
                            "testIndex", store.getLastCommitTime());

                    // lookup the tuple (not found).
                    System.err.println("tuple: "
                            + readOnlyBTree.lookup("hello"));

                }

                // abort (discard writes since the last commit).
                store.abort();
                
            }
            
            // Verify writes are not visible on the mutable B+Tree.
            {
                
                final BTree mutableBTree = store.getIndex("testIndex");

                // lookup the tuple (not found).
                System.err.println("tuple: " + mutableBTree.lookup("hello"));

            }

            // @todo show writes visible after a commit.

        } finally {

            // destroy the backing store.
            store.destroy();

        }

    }

}
