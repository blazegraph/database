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
 * Created on Dec 26, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.ITx;

/**
 * Unit tests of local (all writes are on a single data service) and distributed
 * abort and commit protocols for an {@link IBigdataFederation} using the
 * {@link DistributedTransactionService}.
 * 
 * @todo the easiest way to set this up is to place different indices onto
 *       different data services, using just 2 data services to make setup
 *       easier.
 *       <p>
 *       An alternative setup would pre-partition a single scale-out index so
 *       that it was on both data services.
 * 
 * @todo There should be explicit tests of distributed transactions that involve
 *       different indices, including the case where the MDS is participating in
 *       the tx. This should really be no different, but the tests should cover
 *       the case anyway.  In point, perhaps the easiest thing to do is to register
 *       different indices on each data service since the commit protocol is
 *       entirely in terms of the local index resources (it pays no attention
 *       to scale-out indices, so this is best really).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDistributedTransactionService extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestDistributedTransactionService() {
    }

    /**
     * @param arg0
     */
    public TestDistributedTransactionService(String arg0) {
        super(arg0);
    }

//    /**
//     * Writes a key/val pair on a named index on the specified
//     * {@link IDataService}.
//     * 
//     * @param tx
//     *            The timestamp or transaction identifer.
//     * @param ds
//     *            The {@link IDataService}.
//     * @param name
//     *            The index name.
//     *            
//     * @throws InterruptedException
//     * @throws ExecutionException
//     * @throws IOException
//     */
//    protected void doWrite(long tx, IDataService ds, String name)
//            throws InterruptedException, ExecutionException, IOException {
//
//        ds.submit(tx, name, new IIndexProcedure() {
//
//            public Object apply(IIndex ndx) {
//                
//                // write on the index.
//                ndx.insert(new byte[]{1}, new byte[]{1});
//                
//                return null;
//            }
//
//            /** read-write. */
//            public boolean isReadOnly() {
//                return false;
//            }});
//    
//    }
    
    /**
     * Unit test of abort of a read-write tx that writes on a single data
     * service.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    public void test_localTxAbort() throws IOException, InterruptedException, ExecutionException {
        
        final String name1 = "ndx1";
        
        {
            final IndexMetadata md = new IndexMetadata(name1, UUID
                    .randomUUID());
            
            md.setIsolatable(true);
            
            dataService1.registerIndex(name1, md);
        }
        
        final long tx = fed.getTransactionService().newTx(ITx.UNISOLATED);
        
        // submit write operation to the ds.
        dataService1.submit(tx, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                
                // write on the index.
                ndx.insert(new byte[]{1}, new byte[]{1});
                
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();
        
        // verify write not visible to unisolated operation.
        dataService1.submit(ITx.UNISOLATED, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                
                // verify not in the index.
                assertFalse(ndx.contains(new byte[]{1}));
                
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();

        // abort the tx.
        fed.getTransactionService().abort(tx);

        // verify write still not visible.
        dataService1.submit(ITx.UNISOLATED, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                
                // verify not in the index.
                assertFalse(ndx.contains(new byte[]{1}));
                
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();

        // verify operation rejected for aborted read-write tx.
        try {
        dataService1.submit(tx, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                // NOP
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();
        fail("Expecting exception");
        } catch(Throwable t) {
            log.info("Ignoring expected error: "+t);
        }
        
    }
    
    /**
     * unit test of commit of a read-write tx that writes on a single data
     *       service.
     * 
     * @throws IOException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    public void test_localTxCommit() throws InterruptedException,
            ExecutionException, IOException {
        
        final String name1 = "ndx1";
        
        {
            final IndexMetadata md = new IndexMetadata(name1, UUID
                    .randomUUID());
            
            md.setIsolatable(true);
            
            dataService1.registerIndex(name1, md);
        }
        
        final long tx = fed.getTransactionService().newTx(ITx.UNISOLATED);
        
        // submit write operation to the ds.
        dataService1.submit(tx, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                
                // write on the index.
                ndx.insert(new byte[]{1}, new byte[]{1});
                
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();
        
        // verify write not visible to unisolated operation.
        dataService1.submit(ITx.UNISOLATED, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                
                // verify not in the index.
                assertFalse(ndx.contains(new byte[]{1}));
                
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();

        // commit the tx.
        final long commitTime = fed.getTransactionService().commit(tx);

        // verify write now visible as of that commit time.
        dataService1.submit(commitTime, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                
                // verify in the index.
                assertTrue(ndx.contains(new byte[]{1}));
                
                return null;
            }

            public boolean isReadOnly() {
                return true;// read-only.
            }}).get();

        // verify operation rejected for committed read-write tx.
        try {
        dataService1.submit(tx, name1, new IIndexProcedure(){

            public Object apply(IIndex ndx) {
                // NOP
                return null;
            }

            public boolean isReadOnly() {
                return false;// read-write.
            }}).get();
        fail("Expecting exception");
        } catch(Throwable t) {
            log.info("Ignoring expected error: "+t);
        }
        
    }

    /**
     * @todo unit test of abort of a read-write tx that writes on a more than
     *       one data service.
     */
    public void test_distTxAbort() {
        
        fail("write test");
        
    }
    
    /**
     * @todo unit test of commit of a read-write tx that writes on a more than
     *       one data service.
     */
    public void test_distTxCommit() {
        
        fail("write test");
        
    }

}
