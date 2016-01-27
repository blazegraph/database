/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.File;
import java.io.IOException;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.service.DistributedTransactionService.SnapshotHelper;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link SnapshotHelper}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSnapshotHelper extends TestCase2 {

    /**
     * 
     */
    public TestSnapshotHelper() {
    }

    /**
     * @param arg0
     */
    public TestSnapshotHelper(String arg0) {
        super(arg0);
    }

    public void test_snapshots() throws IOException {

        final File testFile = File.createTempFile(getName(), ".snapshot");
        
        try {
        
            if (!testFile.delete()) {

                fail("Could not delete test file: " + testFile);
                
            }
            
            // test empty snapshot.
            {
                // populate and write.
                {
                    
                    final CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    SnapshotHelper.write(ndx, testFile);
                }

                // read and verify.
                {

                    final CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    SnapshotHelper.read(ndx, testFile);
                    
                    assertEquals(new long[]{},toArray(ndx));
                    
                }
                
                // delete snapshot file.
                testFile.delete();

            }

            {
                
                // populate and write.
                {
                    final CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    ndx.add(10L);

                    ndx.add(20L);
                    
                    final long nwritten = SnapshotHelper.write(ndx, testFile);
                    
                    assertEquals(2L, nwritten);
                    
                }

                // read and verify.
                {

                    final CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    final long nread = SnapshotHelper.read(ndx, testFile);

                    assertEquals(2L, nread);
                    
                    assertEquals(new long[] { 10, 20 }, toArray(ndx));
                    
                }

            }

        } finally {

            testFile.delete();
            
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
            
        	final long entryCount = ndx.getEntryCount();
        	
			if (entryCount > Integer.MAX_VALUE) {
				// The test depends on materializing the data in an array.
				throw new RuntimeException();
			}

			final long[] a = new long[(int) entryCount];

            final ITupleIterator itr = ndx.rangeIterator();
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();
                
                a[i] = ndx.decodeKey(tuple.getKey());
                
                i++;
                
            }
            
            return a;
            
        }
        
    }
    
}
