/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 15, 2007
 */

package com.bigdata.journal;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;

/**
 * Correctness test suite for unisolated writes on one or more indices. The
 * tests in this suite validate that the unisolated writes resulting in a commit
 * and that the committed data may be read back by an unisolated read task. Some
 * tests write on more than one index in order to verify that the writes and
 * reads are performed against the expected index. The stress test additionally
 * verifies that dirty indices are retained by {@link Name2Addr} on its commit
 * list and that writes are NOT lost in large commit groups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnisolatedWriteTasks extends ProxyTestCase {

    /**
     * 
     */
    public TestUnisolatedWriteTasks() {
        super();
    }

    /**
     * @param name
     */
    public TestUnisolatedWriteTasks(String name) {
        super(name);
    }

    /**
     * Test creates a named index and writes a set of entries on that index
     * using an unisolated write task. We verify that a commit occured and
     * then verify that the written data may be read back using an unisolated
     * read task.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_writeOneIndex() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final UUID indexUUID = UUID.randomUUID(); 

        /*
         * Setup batch insert.
         */
        final int ninserts = 100;
        final KeyBuilder keyBuilder = new KeyBuilder(4);
        final byte[][] keys = new byte[ninserts][];
        final byte[][] vals = new byte[ninserts][];
        
        for(int i=0; i<ninserts; i++) {

            keys[i] = keyBuilder.reset().append(i).getKey();
            
            vals[i] = keyBuilder.getKey(); 
            
            assertNotNull(keys[i]);
            assertNotNull(vals[i]);
            assertTrue(keys[i]!=vals[i]);
            
        }

        // submit task to create index and do batch insert on that index.
        journal.submit(SequenceTask.newSequence(new AbstractTask[]{
                
                new RegisterIndexTask(journal, //
                                resource[0],//
                                new IndexMetadata(resource[0], indexUUID)),
                
                new AbstractTask(journal,ITx.UNISOLATED,resource) {

                    protected Object doTask() throws Exception {
                        
                        IIndex ndx = getIndex(getOnlyResource());

                        assertEquals("indexUUID",indexUUID,ndx.getIndexMetadata().getIndexUUID());
                        
//                        // Note: clone values since replaced with old values by the batch op.
//                        ndx.insert(new BatchInsert(ninserts, keys, vals.clone()));
                        
                        for (int i = 0; i < keys.length; i++) {
                            
                            ndx.insert(keys[i], vals[i]);
                            
                        }
                        
                        return null;
                        
                    }
                    
                }
                
        })).get();

        // verify a commit was performed.
        assertEquals("commitCounter", commitCounterBefore + 1, journal
                .getRootBlockView().getCommitCounter());

        /*
         * Submit task to validate the committed unisolated write on the named
         * index.
         */
        journal.submit(new AbstractTask(journal, ITx.READ_COMMITTED,
                resource) {

                    protected Object doTask() throws Exception {

                        IIndex ndx = getIndex(getOnlyResource());

                        assertEquals("indexUUID", indexUUID, ndx
                                .getIndexMetadata().getIndexUUID());

                        // verify write is disallowed on the index.
                        try {
                            ndx.insert(new byte[]{}, new byte[]{});
                            fail("Expecting: "+UnsupportedOperationException.class);
                        } catch(UnsupportedOperationException ex) {
                            System.err.println("Ignoring expected exception: "+ex);
                        }
                        
                        /*
                         * verify the written data from the prior task.
                         */
                        
//                        byte[][] actualValues = new byte[ninserts][];
                        
//                        BatchLookup op = new BatchLookup(ninserts,keys,actualValues);
//                        
//                        ndx.lookup(op);
                        
                        for (int i = 0; i < ninserts; i++) {
                            
                            assertNotNull(keys[i]);

                            assertNotNull(vals[i]);

                            byte[] actualValue = (byte[])ndx.lookup(keys[i]);
                            
                            assertEquals("i="+i, vals[i], actualValue);
                            
                        }
                        
                        return null;
                        
                    }
        }).get();
        
        // verify commit was NOT performed.
        assertEquals("commitCounter", commitCounterBefore + 1, journal
                .getRootBlockView().getCommitCounter());

        journal.shutdown();
        
        journal.delete();
        
    }
    
    /**
     * Test creates two named indices and writes a set of distinct entries on
     * each index using a single unisolated write task to write on both indices.
     * We verify that a commit occured and then verify that the written data may
     * be read back using a single unisolated read task to read from both
     * indices.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_writeTwoIndex() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo","bar"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final UUID indexUUID1 = UUID.randomUUID(); 
        final UUID indexUUID2 = UUID.randomUUID(); 

        /*
         * Setup batch insert.
         */
        final int ninserts = 100;
        final KeyBuilder keyBuilder = new KeyBuilder(4);
        final byte[][] keys1 = new byte[ninserts][];
        final byte[][] vals1 = new byte[ninserts][];
        final byte[][] keys2 = new byte[ninserts][];
        final byte[][] vals2 = new byte[ninserts][];
        
        for(int i=0; i<ninserts; i++) {

            keys1[i] = keyBuilder.reset().append(i).getKey();
            
            vals1[i] = keyBuilder.getKey(); 
            
            // use -i to have different keys and values in the 2nd index.
            keys2[i] = keyBuilder.reset().append(-i).getKey();
            
            vals2[i] = keyBuilder.getKey(); 
            
        }

        // submit task to create index and do batch insert on that index.
        journal.submit(SequenceTask.newSequence(new AbstractTask[]{
                
                new RegisterIndexTask(journal, resource[0], //
                        new IndexMetadata(resource[0], indexUUID1)),

                        new RegisterIndexTask(journal, resource[1], //
                                new IndexMetadata(resource[1], indexUUID2)),
                
                new AbstractTask(journal, ITx.UNISOLATED,resource) {

                    protected Object doTask() throws Exception {

                        {

                            IIndex ndx = getIndex("foo");

                            assertEquals("indexUUID", indexUUID1, ndx
                                    .getIndexMetadata().getIndexUUID());

//                            // Note: clone values since replaced with
//                            // old values by the batch op.
//                            ndx.insert(new BatchInsert(ninserts, keys1,
//                                    vals1.clone()));

                            for (int i = 0; i < keys1.length; i++) {
                                
                                ndx.insert(keys1[i], vals1[i]);
                                
                            }

                        }
                        
                        {

                            IIndex ndx = getIndex("bar");

                            assertEquals("indexUUID", indexUUID2, ndx
                                    .getIndexMetadata().getIndexUUID());

//                            // Note: clone values since replaced with
//                            // old values by the batch op.
//                            ndx.insert(new BatchInsert(ninserts, keys2,
//                                    vals2.clone()));
                            
                            for (int i = 0; i < keys2.length; i++) {
                                
                                ndx.insert(keys2[i], vals2[i]);
                                
                            }

                        }
                        
                        return null;
                        
                    }
                    
                }
                
        })).get();

        // verify a commit was performed.
        assertEquals("commitCounter", commitCounterBefore + 1, journal
                .getRootBlockView().getCommitCounter());

        /*
         * Submit task to validate the committed unisolated write on the named
         * index.
         */
        journal.submit(new AbstractTask(journal, ITx.READ_COMMITTED, resource) {

                    protected Object doTask() throws Exception {

                        {
                            
                            IIndex ndx = getIndex("foo");

                            assertEquals("indexUUID", indexUUID1, ndx
                                    .getIndexMetadata().getIndexUUID());

                            // verify write is disallowed on the index.
                            try {
                                ndx.insert(new byte[] {}, new byte[] {});
                                fail("Expecting: "
                                        + UnsupportedOperationException.class);
                            } catch (UnsupportedOperationException ex) {
                                System.err
                                        .println("Ignoring expected exception: "
                                                + ex);
                            }

                            /*
                             * verify the written data from the prior task.
                             */

//                            byte[][] actualValues = new byte[ninserts][];
//
//                            BatchLookup op = new BatchLookup(ninserts, keys1,
//                                    actualValues);
//
//                            ndx.lookup(op);

                            for (int i = 0; i < ninserts; i++) {

                                assertNotNull(keys1[i]);
                                
                                assertNotNull(vals1[i]);

                                byte[] actualValue = (byte[]) ndx.lookup(keys1[i]);
                                
                                assertEquals("i=" + i, vals1[i], actualValue);

                            }
                            
                        }
                        
                        {
                         
                            IIndex ndx = getIndex("bar");

                            assertEquals("indexUUID", indexUUID2, ndx
                                    .getIndexMetadata().getIndexUUID());

                            // verify write is disallowed on the index.
                            try {
                                ndx.insert(new byte[] {}, new byte[] {});
                                fail("Expecting: "
                                        + UnsupportedOperationException.class);
                            } catch (UnsupportedOperationException ex) {
                                System.err
                                        .println("Ignoring expected exception: "
                                                + ex);
                            }

                            /*
                             * verify the written data from the prior task.
                             */
//
//                            byte[][] actualValues = new byte[ninserts][];
//
//                            BatchLookup op = new BatchLookup(ninserts, keys2,
//                                    actualValues);
//
//                            ndx.lookup(op);

                            for (int i = 0; i < ninserts; i++) {

                                assertNotNull(keys2[i]);
                                
                                assertNotNull(vals2[i]);

                                byte[] actualValue = (byte[]) ndx.lookup(keys2[i]);
                                
                                assertEquals("i=" + i, vals2[i], actualValue);

                            }
                            
                        }
                        
                        return null;
                        
                    }
        }).get();
        
        // verify commit was NOT performed.
        assertEquals("commitCounter", commitCounterBefore + 1, journal
                .getRootBlockView().getCommitCounter());

        journal.shutdown();
        
        journal.delete();
        
    }
   
    /**
     * This is an N index stress test designed to verify that all indices
     * receive their writes in a large commit group. N SHOULD be choosen to be
     * larger than the #of indices in the LRU cache maintained by
     * {@link Name2Addr}.
     * <p>
     * Note: This does all writes in a single task in order to simulate a large
     * commit group and stress the commit list mechanism used by
     * {@link Name2Addr}. However, it does not matter whether the data are
     * validated by a single read task or by multiple read tasks.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_writeManyIndices() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);

        /*
         * Note: This SHOULD be larger than the capacity of the LRUCache in
         * Name2Addr.
         */
        final int nindices = 200;
        
        System.err.println("Stress test is using "+nindices+" indices");

        final Random r = new Random();
        
        final KeyBuilder keyBuilder = new KeyBuilder(4);
        
        // per-index stuff.
        class IndexStuff {
            final String name;
            final UUID indexUUID = UUID.randomUUID();
            final int ninserts = r.nextInt(100)+100;
            final byte[][] keys;
            final byte[][] vals;
            
            IndexStuff(String name) {
            
                this.name = name;
                
                keys = new byte[ninserts][];
                vals = new byte[ninserts][];
                
                for(int i=0; i<ninserts; i++) {

                    keys[i] = keyBuilder.reset().append(r.nextInt()).getKey();
                    
                    vals[i] = keyBuilder.getKey(); 
                    
                }
                // sort the keys (the values will no longer be pairs to the same
                // key, but who cares for this test?
                Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);
            }
        }

        /*
         * Setup resources.
         */

        final String[] resource = new String[nindices];
        final IndexStuff[] indices = new IndexStuff[nindices];
        for (int i = 0; i < nindices; i++) {
           
            indices[i] = new IndexStuff("index#" + i);
            resource[i] = indices[i].name;
            
        }
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        // submit task to create indices and do batch inserts those indices.
        journal.submit(SequenceTask.newSequence(new AbstractTask[]{
                
                // register all indices.
                new AbstractTask(journal, ITx.UNISOLATED,resource) {

                    protected Object doTask() throws Exception {
                        
                        for(int i=0; i<nindices; i++) {
                            
                            getJournal().registerIndex(//
                                    indices[i].name,//
                                    BTree.create(//
                                            getJournal(),//
                                            new IndexMetadata(indices[i].name,indices[i].indexUUID)
                                    ));
                            
                        }
                        
                        return null;
                        
                    }
                    
                },

                // write on all indices.
                new AbstractTask(journal, ITx.UNISOLATED,resource) {

                    protected Object doTask() throws Exception {

                        for(int i=0; i<nindices; i++) {
                            
                            IIndex ndx = getIndex(indices[i].name);

                            assertEquals("indexUUID",
                                            indices[i].indexUUID, ndx
                                                    .getIndexMetadata()
                                                    .getIndexUUID());
                            
//                            // Note: clone values since replaced with old values
//                            // by the batch op.
//                            ndx.insert(new BatchInsert(
//                                    indices[i].ninserts,
//                                    indices[i].keys, indices[i].vals
//                                    .clone()));
                            
                            for (int j = 0; j < indices[i].keys.length; j++) {
                                
                                ndx.insert(indices[i].keys[j], indices[i].vals[j]);
                                
                            }

                        }
                        
                        return null;
                        
                    }
                    
                }
                
        })).get();

        // verify a commit was performed.
        assertEquals("commitCounter", commitCounterBefore + 1, journal
                .getRootBlockView().getCommitCounter());

        /*
         * Submit task to validate the committed unisolated write on the named
         * index.
         */
        journal.submit(
                new AbstractTask(journal, ITx.READ_COMMITTED, resource) {

                    // verify the writes on each index.
                    protected Object doTask() throws Exception {

                        for(int i=0; i<nindices; i++) {
                         
                            IndexStuff stuff = indices[i];
                            
                            IIndex ndx = getIndex(stuff.name);

                            assertEquals("indexUUID", stuff.indexUUID, ndx
                                    .getIndexMetadata().getIndexUUID());

                            /*
                             * verify the written data from the prior task.
                             */

//                            byte[][] actualValues = new byte[stuff.ninserts][];
//
//                            BatchLookup op = new BatchLookup(stuff.ninserts, stuff.keys,
//                                    actualValues);
//
//                            ndx.lookup(op);

                            for (int j = 0; j < stuff.ninserts; j++) {

                                byte[] actualValue = (byte[])ndx.lookup(stuff.keys[j]);
                                
                                assertEquals("j=" + j, stuff.vals[j], actualValue);

                            }

                        }

                        return null;

                    }
                }).get();

        // verify commit was NOT performed.
        assertEquals("commitCounter", commitCounterBefore + 1, journal
                .getRootBlockView().getCommitCounter());

        journal.shutdown();

        journal.delete();

    }

}
