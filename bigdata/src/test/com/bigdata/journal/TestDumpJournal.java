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
 * Created on Nov 3, 2008
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KV;
import com.bigdata.htree.HTree;

/**
 * Test suite for {@link DumpJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Test historical journal artifacts for compatibility.
 * 
 *          TODO Test command line utility.
 * 
 *          FIXME GIST : Test other types of indices (HTree (one test exists
 *          now), Stream (no tests yet)).
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
 *      </a>
 */
public class TestDumpJournal extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestDumpJournal() {
    }

    /**
     * @param name
     */
    public TestDumpJournal(String name) {
        super(name);
    }

    /**
     * Dump an empty journal.
     */
    public void test_emptyJournal() throws IOException, InterruptedException,
            ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, true/* showTuples */);

        } finally {

            src.destroy();

        }
        
    }
    
    /**
     * Dump a journal with a single named index.
     */
    public void test_journal_oneIndexNoData() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, true/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
     * Test with a journal on which a single index has been registered with
     * random data on the index.
     */
    public void test_journal_oneIndexRandomData() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";
            
            src.registerIndex(new IndexMetadata(NAME, UUID.randomUUID()));
            
            {
              
                final BTree ndx = src.getIndex(NAME);
                
                final KV[] a = AbstractBTreeTestCase
                        .getRandomKeyValues(1000/* ntuples */);
                
                for (KV kv : a) {
                
                    ndx.insert(kv.key, kv.val);
                    
                }
                
            }

            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
     * Test with an HTree.
     */
    public void test_journal_oneIndex_HTree_RandomData() throws IOException,
            InterruptedException, ExecutionException {

        final Journal src = getStore(getProperties());

        try {

            // register an index and commit the journal.
            final String NAME = "testIndex";

            src.registerIndex(new HTreeIndexMetadata(NAME, UUID.randomUUID()));

            {

                final HTree ndx = (HTree) src.getUnisolatedIndex(NAME);

                final KV[] a = AbstractBTreeTestCase
                        .getRandomKeyValues(1000/* ntuples */);

                for (KV kv : a) {

                    ndx.insert(kv.key, kv.val);

                }

            }

            src.commit();

            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

        } finally {

            src.destroy();

        }

    }

    /**
     * Test with a journal on which many indices have been registered and
     * populated with random data.
     */
    public void test_journal_manyIndicesRandomData() throws IOException,
            InterruptedException, ExecutionException {

        final String PREFIX = "testIndex#";
        final int NUM_INDICES = 4;

        Journal src = getStore(getProperties());

        try {

            for (int i = 0; i < NUM_INDICES; i++) {

                // register an index
                final String name = PREFIX + i;

                src.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
                {

                    // lookup the index.
                    final BTree ndx = src.getIndex(name);

                    // #of tuples to write.
                    final int ntuples = r.nextInt(1000);

                    // generate random data.
                    final KV[] a = AbstractBTreeTestCase
                            .getRandomKeyValues(ntuples);

                    // write tuples (in random order)
                    for (KV kv : a) {

                        ndx.insert(kv.key, kv.val);

                        if (r.nextInt(100) < 10) {

                            // randomly increment the counter (10% of the time).
                            ndx.getCounter().incrementAndGet();

                        }

                    }

                }

            }

            // commit the journal (!)
            src.commit();

            new DumpJournal(src)
                    .dumpJournal(false/* dumpHistory */, true/* dumpPages */,
                            false/* dumpIndices */, false/* showTuples */);
            
            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, true/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

            // test again w/o dumpPages
            new DumpJournal(src)
                    .dumpJournal(true/* dumpHistory */, false/* dumpPages */,
                            true/* dumpIndices */, false/* showTuples */);

            /*
             * Now write some more data, going through a series of commit
             * points. This let's us check access to historical commit points.
             */
            for (int j = 0; j < 10; j++) {
                
                for (int i = 0; i < NUM_INDICES; i++) {

                    // register an index
                    final String name = PREFIX + i;

                    // lookup the index.
                    final BTree ndx = src.getIndex(name);

                    // #of tuples to write.
                    final int ntuples = r.nextInt(1000);

                    // generate random data.
                    final KV[] a = AbstractBTreeTestCase
                            .getRandomKeyValues(ntuples);

                    // write tuples (in random order)
                    for (KV kv : a) {

                        ndx.insert(kv.key, kv.val);

                        if (r.nextInt(100) < 10) {

                            // randomly increment the counter (10% of the time).
                            ndx.getCounter().incrementAndGet();

                        }

                    }

                }

                src.commit();

                new DumpJournal(src)
                        .dumpJournal(false/* dumpHistory */,
                                true/* dumpPages */, false/* dumpIndices */,
                                false/* showTuples */);

                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                true/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

                // test again w/o dumpPages
                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                false/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);
            }

            if (src.isStable()) {

                src = reopenStore(src);

                new DumpJournal(src)
                        .dumpJournal(false/* dumpHistory */,
                                true/* dumpPages */, false/* dumpIndices */,
                                false/* showTuples */);

                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                true/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

                // test again w/o dumpPages
                new DumpJournal(src)
                        .dumpJournal(true/* dumpHistory */,
                                false/* dumpPages */, true/* dumpIndices */,
                                false/* showTuples */);

            }
            
            
        } finally {

            src.destroy();

        }

    }

}
