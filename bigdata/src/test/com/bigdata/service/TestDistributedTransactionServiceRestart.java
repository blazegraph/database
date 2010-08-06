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
 * Created on Dec 23, 2008
 */

package com.bigdata.service;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

/**
 * Unit tests of the setReleaseTime, snapshot and restart aspects of the
 * {@link DistributedTransactionService} (all having to do with the maintenance
 * of the commit time index, including across restart).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDistributedTransactionServiceRestart extends TestCase2 {

    /**
     * 
     */
    public TestDistributedTransactionServiceRestart() {
    }

    /**
     * @param arg0
     */
    public TestDistributedTransactionServiceRestart(String arg0) {
        super(arg0);
    }

    protected static class MockDistributedTransactionService extends DistributedTransactionService {
        
        public MockDistributedTransactionService(
                Properties properties) {
            
            super(properties);
            
        }

        @Override
        public AbstractFederation getFederation() {

            throw new UnsupportedOperationException();

        }

        @Override
        protected void setReleaseTime(final long releaseTime) {
            
            lock.lock();
            
            try {
                
                super.setReleaseTime(releaseTime);
                
            } finally {
                
                lock.unlock();
                
            }
            
        }
        
        /**
         * Note: The scheduled tasks are disabled for the unit test since we do
         * not have a federation for this test.
         */
        @Override
        protected void addScheduledTasks() {

            // NOP.

        }

        @Override
        public MockDistributedTransactionService start() {

            super.start();

            return this;
            
        }

		/**
		 * This is overridden to be a NOP for this test suite. The unit tests in
		 * this suite depend on the ability to inject specific commit times into
		 * the transaction service without having them "release" based on the
		 * actual system clock.
		 */
        @Override
        protected void updateReleaseTimeForBareCommit(final long commitTime) {
        	
        	return;
        }
        
        /**
         * Exposed to the unit tests.
         */
        CommitTimeIndex getCommitTimeIndex() {
            
            return commitTimeIndex;
            
        }
        
    }
    
    /**
     * Return an array containing the ordered keys in the
     * {@link CommitTimeIndex}.
     * 
     * @param ndx
     * 
     * @return The array.
     */
    long[] toArray(final CommitTimeIndex ndx) {
        
        synchronized(ndx) {
            
            final long[] a = new long[ndx.getEntryCount()];
            
            final ITupleIterator<?> itr = ndx.rangeIterator();
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                final ITuple<?> tuple = itr.next();
                
                a[i] = ndx.decodeKey(tuple.getKey());
                
                i++;
                
            }
            
            return a;
            
        }
        
    }
    
    /**
     * Unit tests verifies that the head of the commit time index is truncated
     * when the release time is advanced and that it is still possible to obtain
     * a read-only tx as of the timestamp immediately after the current release
     * time.
     */
    public void test_setReleaseTime() {

        final Properties properties = new Properties();

        properties.setProperty(DistributedTransactionService.Options.DATA_DIR,
                getName());

        properties.setProperty(DistributedTransactionService.Options.MIN_RELEASE_AGE,
                "10");

        final MockDistributedTransactionService service = new MockDistributedTransactionService(
                properties).start();

        try {

            /*
             * populate the commit index.
             */
            service.notifyCommit(10L);

            service.notifyCommit(20L);

            // verify the commit index.
            {

                final CommitTimeIndex ndx = service.getCommitTimeIndex();

                synchronized (ndx) {

                    assertEquals(2, ndx.getEntryCount());

                    assertEquals(new long[] { 10, 20 }, toArray(ndx));

                }

            }

            // verify that we can obtain a read-only tx.
            service.abort(service.newTx(10L));

            // verify that we can obtain a read-only tx.
            service.abort(service.newTx(20L));

            service.setReleaseTime(20L - 1);
            {

                final CommitTimeIndex ndx = service.getCommitTimeIndex();

                synchronized (ndx) {
                    
                    assertEquals(1, ndx.getEntryCount());
                    
                    assertEquals(new long[] { 20 }, toArray(ndx));
                    
                }

            }
            
            // verify that we can still obtain a read-only tx for that commit time.
            service.abort(service.newTx(20L));

            /*
             * Verify that we can not release the lastCommitTime.
             */
            assertEquals(20L,service.getLastCommitTime());
            service.setReleaseTime(20L);
            {

                final CommitTimeIndex ndx = service.getCommitTimeIndex();

                synchronized (ndx) {
                    
                    assertEquals(1, ndx.getEntryCount());
                    
                    assertEquals(new long[] { 20 }, toArray(ndx));
                    
                }

            }
            

        }

        finally {

            service.destroy();

        }

    }

    /**
     * Unit test of the ability to snapshot the commit index. The test setups up
     * the expected state of the commit index, verifies that state, shuts down
     * the service, and verifies that a snapshot was written during shutdown.
     * The test then restarts the service, and verifies that the commit index
     * has the expected values (read from the snapshot). Another entry is then
     * added to the commit index and the release time is advanced, which should
     * cause the head of the commit index to be truncated. The test then
     * verifies the expected state of the commit index, shuts down the service,
     * and verifies that another snapshot file was written. Finally, we restart
     * the service one more time and verify that the commit index has the data
     * that we would expect if it had read the 2nd snapshot.
     */
    public void test_snapshotCommitIndex() {

        final Properties properties = new Properties();

        properties.setProperty(DistributedTransactionService.Options.DATA_DIR,
                getName());

        MockDistributedTransactionService service = new MockDistributedTransactionService(
                properties).start();

        final File file0 = new File(service.dataDir,
                DistributedTransactionService.BASENAME + "0"
                        + DistributedTransactionService.EXT);

        final File file1 = new File(service.dataDir,
                DistributedTransactionService.BASENAME + "1"
                        + DistributedTransactionService.EXT);

        try {

            /*
             * populate the commit log.
             */
            service.notifyCommit(10L);

            service.notifyCommit(20L);

            {
                
                final CommitTimeIndex ndx = service.getCommitTimeIndex();
                
                synchronized(ndx) {
                    
                    assertEquals(2, ndx.getEntryCount());

                    assertEquals(new long[] { 10, 20 }, toArray(ndx));
                    
                }
                
            }

            // should do a snapshot.
            service.shutdown();

            assertTrue(file0.exists());
            assertFalse(file1.exists());

            // restart the service.
            service = new MockDistributedTransactionService(properties).start();

            // verify the commit time index.
            {
                
                final CommitTimeIndex ndx = service.getCommitTimeIndex();
                
                synchronized(ndx) {
                    
                    assertEquals(2, ndx.getEntryCount());

                    assertEquals(new long[] { 10, 20 }, toArray(ndx));
                    
                }
                
            }

            service.setReleaseTime(20L - 1);
            {
                
                final CommitTimeIndex ndx = service.getCommitTimeIndex();

                synchronized (ndx) {
                    assertEquals(1, ndx.getEntryCount());
                    assertEquals(new long[] { 20 }, toArray(ndx));
                }

            }

            // should do a snapshot.
            service.shutdown();

//            System.err.println("file0: "+file0.lastModified());
//            System.err.println("file1: "+file1.lastModified());
            assertTrue(file0.exists());
            assertTrue(file1.exists());

            // restart the service.
            service = new MockDistributedTransactionService(properties).start();
            
//          verify the commit time index.
            {
                
                final CommitTimeIndex ndx = service.getCommitTimeIndex();
                
                synchronized(ndx) {
                    
                    assertEquals(1, ndx.getEntryCount());

                    assertEquals(new long[] { 20 }, toArray(ndx));
                    
                }
                
            }

        } finally {

            service.destroy();

        }

    }

}
