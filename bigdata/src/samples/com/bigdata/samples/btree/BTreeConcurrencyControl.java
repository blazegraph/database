/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jul 10, 2009
 */

package com.bigdata.samples.btree;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.Journal;

/**
 * The {@link BTree} is single-threaded for writers, but you have several
 * concurreny control. You can:
 * <ol>
 * <li>Manage concurrency control yourself. </li>
 * <li>Wrap a {@link BTree} using an {@link UnisolatedReadWriteIndex}, which
 * will manage concurrency for you using a {@link ReadWriteLock}. </li>
 * <li>Use the {@link ConcurrencyManager}, which lets you execute
 * {@link IIndexProcedure}s while holding locks on one or more indices.</li>
 * </ol>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo do another example which shows how to use range counts and iterators.
 * 
 * @todo do another example which shows how to use the scale-out indices
 *       efficiently with {@link IIndexProcedure}s. this approach is necessary
 *       when you need to do complex logic which would otherwise result in a
 *       large volume of point tests against the scale-out index. Instead you
 *       map a procedure against the index and it is executed locally on each
 *       index partition within some key-range.
 */
public class BTreeConcurrencyControl {

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
             * Obtain two views onto the same
             */
            final UnisolatedReadWriteIndex view1 = new UnisolatedReadWriteIndex(
                    store.getIndex("testIndex"));

            final UnisolatedReadWriteIndex view2 = new UnisolatedReadWriteIndex(
                    store.getIndex("testIndex"));
            
            /*
             * @todo demonstrate that the two views have read/write concurrency
             * against the mutable B+Tree.
             */
            new Thread() {
                public void run() {
                    
                }
            };
            
            throw new UnsupportedOperationException();
            
        } finally {

            // destroy the backing store.
            store.destroy();

        }

    }

}
