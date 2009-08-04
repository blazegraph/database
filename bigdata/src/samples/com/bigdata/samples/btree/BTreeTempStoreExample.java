/*
 * Created on Jul 10, 2009
 */

package com.bigdata.samples.btree;

import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryStore;

/**
 * Sample for for using the {@link BTree} with a {@link TemporaryStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTreeTempStoreExample {

    public static void main(String[] args) {

        final TemporaryStore store = new TemporaryStore();

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
            }

            /*
             * Lookup the unisolated B+Tree. This is the mutable B+Tree view.
             * 
             * While the temporary store does not differentiate between mutable
             * and read-only views, the Journal and the scale-out architecture
             * do.
             */
            {
            
                final BTree btree = store.getIndex("testIndex", ITx.UNISOLATED);

                // lookup the tuple (not found).
                System.err.println("tuple: " + btree.lookup("hello"));

                // add a tuple
                btree.insert("hello", "world");

                // lookup the tuple
                System.err.println("tuple: " + btree.lookup("hello"));

                // update the tuple
                btree.insert("hello", "again");

                // lookup the new value
                System.err.println("tuple: " + btree.lookup("hello"));

            }

        } finally {

            // destroy the backing store.
            store.destroy();

        }

    }

}
