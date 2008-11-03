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

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KV;

/**
 * Test suite for {@link Journal#compact(java.io.File)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCompactJournal extends AbstractJournalTestCase {

    /**
     * 
     */
    public TestCompactJournal() {
    }

    /**
     * @param name
     */
    public TestCompactJournal(String name) {
        super(name);
    }

    /**
     * Verifies exception if there are no commits on the journal (the
     * lastCommitTime will be zero which does not identify a valid commit
     * point).
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_emptyJournal() throws IOException, InterruptedException,
            ExecutionException {

        final File out = File.createTempFile(getName(), Options.JNL);

        out.deleteOnExit();
        
        final Journal src = getStore(getProperties());
        
        try {
            
            try {
                // create task.
                Future<Journal> f = src.compact(out);
                // obtain new journal (expected to fail).
                final Journal newJournal = f.get();
                try {
                    // destroy new journal if succeeded (clean up).
                    newJournal.destroy();
                } finally {
                    // notify test error.
                    fail("Expecting " + IllegalArgumentException.class);
                }
            } catch(IllegalArgumentException ex) {
                // log expected exception.
                log.info("Ignoring expected exception: "+ex);
            }
            
        } finally {
            
            src.destroy();
            
            out.delete();
            
        }
        
    }
    
    /**
     * Test of a journal on which a single index has been register (and the
     * journal committed) but no data was written onto the index.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_oneIndexNoData() throws IOException,
            InterruptedException, ExecutionException {

        final File out = File.createTempFile(getName(), Options.JNL);

        out.deleteOnExit();

        final Journal src = getStore(getProperties());

        // register an index and commit the journal.
        final String NAME = "testIndex";
        src.registerIndex(new IndexMetadata(NAME,UUID.randomUUID()));
        src.commit();
        
        try {

            Future<Journal> f = src.compact(out);

            Journal newJournal = f.get();

            // verify state
            try {

                // verify index exists.
                assertNotNull(newJournal.getIndex(NAME));

                // verify data is the same.
                AbstractBTreeTestCase.assertSameBTree(src.getIndex(NAME), newJournal.getIndex(NAME));
                
            } finally {
                newJournal.destroy();
            }

        } finally {

            src.destroy();

            out.delete();

        }

    }

    /**
     * Test with a journal on which a single index has been registered with
     * random data on the index.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_oneIndexRandomData() throws IOException,
            InterruptedException, ExecutionException {

        final File out = File.createTempFile(getName(), Options.JNL);

        out.deleteOnExit();

        final Journal src = getStore(getProperties());

        // register an index and commit the journal.
        final String NAME = "testIndex";
        src.registerIndex(new IndexMetadata(NAME,UUID.randomUUID()));
        {
            BTree ndx = src.getIndex(NAME);
            KV[] a = AbstractBTreeTestCase.getRandomKeyValues(1000/* ntuples */);
            for (KV kv : a) {
               ndx.insert(kv.key, kv.val);
           }
        }
        src.commit();
        
        try {

            Future<Journal> f = src.compact(out);

            Journal newJournal = f.get();

            // verify state
            try {

                // verify index exists.
                assertNotNull(newJournal.getIndex(NAME));

                // verify data is the same.
                AbstractBTreeTestCase.assertSameBTree(src.getIndex(NAME), newJournal.getIndex(NAME));
                
            } finally {
                newJournal.destroy();
            }

        } finally {

            src.destroy();

            out.delete();

        }

    }

    /**
     * Test with a journal on which many indies have been registered and
     * populated with random data.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_journal_manyIndicesRandomData() throws IOException,
            InterruptedException, ExecutionException {

        final File out = File.createTempFile(getName(), Options.JNL);

        out.deleteOnExit();

        final Journal src = getStore(getProperties());

        final String PREFIX = "testIndex#";
        final int NUM_INDICES = 20;

        for(int i=0; i<NUM_INDICES; i++) {

            // register an index
            final String name = PREFIX + i;
            
            src.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
            {
                
                // lookup the index.
                final BTree ndx = src.getIndex(name);
                
                // #of tuples to write.
                final int ntuples = r.nextInt(10000);
                
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
        
        try {

            Future<Journal> f = src.compact(out);

            Journal newJournal = f.get();

            // verify state
            try {

                for (int i = 0; i < NUM_INDICES; i++) {

                    final String name = PREFIX + i;

                    // verify index exists.
                    assertNotNull(newJournal.getIndex(name));

                    // verify data is the same.
                    AbstractBTreeTestCase.assertSameBTree(src.getIndex(name),
                            newJournal.getIndex(name));

                    // and verify the counter was correctly propagated.
                    assertEquals(src.getIndex(name).getCounter().get(),
                            newJournal.getIndex(name).getCounter().get());
                    
                }
                
            } finally {
                
                newJournal.destroy();
                
            }

        } finally {

            src.destroy();

            out.delete();

        }

    }

}
