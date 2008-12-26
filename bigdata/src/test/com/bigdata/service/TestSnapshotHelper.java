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
        
        if (!testFile.delete()) {

            fail("Could not delete test file: " + testFile);
            
        }
        
        try {
        
            // test empty snapshot.
            {
                // populate and write.
                {
                    CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    SnapshotHelper.write(ndx, testFile);
                }

                // read and verify.
                {

                    CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    SnapshotHelper.read(ndx, testFile);
                    
                    assertEquals(new long[]{},toArray(ndx));
                    
                }
                
                // delete snapshot file.
                testFile.delete();

            }

            {
                
                // populate and write.
                {
                    CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    ndx.add(10L);

                    ndx.add(20L);
                    
                    SnapshotHelper.write(ndx, testFile);
                }

                // read and verify.
                {

                    CommitTimeIndex ndx = CommitTimeIndex.createTransient();

                    SnapshotHelper.read(ndx, testFile);
                    
                    assertEquals(new long[]{10,20},toArray(ndx));
                    
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
    long[] toArray(CommitTimeIndex ndx) {
        
        synchronized(ndx) {
            
            long[] a = new long[ndx.getEntryCount()];
            
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
