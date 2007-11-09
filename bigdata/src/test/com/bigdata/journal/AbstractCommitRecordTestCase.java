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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.util.Random;

import junit.framework.TestCase;

/**
 * Defines some helper methods for testing {@link ICommitRecord}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractCommitRecordTestCase extends TestCase {

    public AbstractCommitRecordTestCase() {
    }

    public AbstractCommitRecordTestCase(String name) {
        super(name);
    }

    Random r = new Random();

    /**
     * Compare two {@link ICommitRecord}s for equality in their data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(ICommitRecord expected, ICommitRecord actual) {
        
        assertEquals("timestamp", expected.getTimestamp(), actual.getTimestamp());

        assertEquals("#roots", expected.getRootAddrCount(), actual.getRootAddrCount());
        
        final int n = expected.getRootAddrCount();
        
        for(int i=0; i<n; i++) {
        
            assertEquals("rootAddrs", expected.getRootAddr(i), actual.getRootAddr(i));
            
        }
        
    }

    public ICommitRecord getRandomCommitRecord() {

        final long timestamp = System.currentTimeMillis();

        // using the clock for this as well so that it is an ascending value.
        final long commitCounter = System.currentTimeMillis();

        final int n = ICommitRecord.MAX_ROOT_ADDRS;
        
        long[] roots = new long[n];
        
        for(int i=0; i<n; i++) {

            boolean empty = r.nextInt(100)<30;
        
            roots[i] = empty ? 0L : r.nextInt(Integer.MAX_VALUE);
            
        }

        return new CommitRecord(timestamp,commitCounter,roots);
        
    }
    

}
