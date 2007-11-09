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
 * Created on Oct 26, 2006
 */

package com.bigdata.util;

import junit.framework.TestCase;

import com.bigdata.journal.RootBlockView;

/**
 * Test suite for {@link NanosecondTimestampFactory}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNanosecondTimestampFactory extends TestCase {

    public TestNanosecondTimestampFactory() {
        
    }

    public TestNanosecondTimestampFactory(String arg0) {

        super(arg0);
        
    }

    /**
     * Test determines whether nano times are always distinct from the last
     * generated nanos time (as assigned by {@link System#nanoTime()}.
     * <p>
     * Note: This test is NOT designed to pass/fail but simply to test determine
     * a characteristic of the platform on which it is executing.
     */
    public void test_nextNanoTime() {

        final int limit = 1000000;
        
        long lastNanoTime = System.nanoTime();
        long nanoTime;
        long minDiff = Long.MAX_VALUE;
        
        for( int i=0; i<limit; i++ ) {

            nanoTime = System.nanoTime();
            
            if (nanoTime == lastNanoTime) {

                System.err
                        .println("This platform can generate identical timestamps with nanosecond resolution");
            
                return;
                
            }

            long diff = nanoTime - lastNanoTime;
            
            if( diff < 0 ) diff = -diff;
            
            if( diff < minDiff ) minDiff = diff;
            
            lastNanoTime = nanoTime;
            
        }

        System.err.println("Nano times appear to be distinct on this platorm.");

        System.err.println("Minimum difference in nanos is " + minDiff
                + " over " + limit + " trials");
        
    }
    
    /**
     * Test verifies that nano times are always distinct from the last generated
     * nanos time (as assigned by {@link NanosecondTimestampFactory#nextNanoTime()}.
     */
    public void test_nextNanoTime2() {

        final int limit = 1000000;
        
        long lastNanoTime = System.nanoTime() - 1;
        long nanoTime;
        long minDiff = Long.MAX_VALUE;
        
        for( int i=0; i<limit; i++ ) {

            nanoTime = NanosecondTimestampFactory.nextNanoTime();
            
            if( nanoTime == lastNanoTime ) fail("Same nano time?");

            long diff = nanoTime - lastNanoTime;
            
            if( diff < 0 ) diff = -diff;
            
            if( diff < minDiff ) minDiff = diff;
            
            lastNanoTime = nanoTime;
            
        }
        
        System.err.println("Minimum difference in nanos is " + minDiff
                + " over " + limit + " trials");
        
    }
    
}
