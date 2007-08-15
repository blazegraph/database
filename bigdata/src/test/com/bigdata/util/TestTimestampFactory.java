/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 26, 2006
 */

package com.bigdata.util;

import junit.framework.TestCase;

import com.bigdata.journal.RootBlockView;

/**
 * Test suite for {@link TimestampFactory}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTimestampFactory extends TestCase {

    public TestTimestampFactory() {
        
    }

    public TestTimestampFactory(String arg0) {

        super(arg0);
        
    }

    /**
     * Test verifies that nano times are always distinct from the last generated
     * nanos time (as assigned by {@link System#nanoTime()}.  If this test passes
     * then it shows that nanos can not be assigned quickly enough to result in
     * duplicate values.
     */
    public void test_nextNanoTime() {

        final int limit = 1000000;
        
        long lastNanoTime = System.nanoTime();
        long nanoTime;
        long minDiff = Long.MAX_VALUE;
        
        for( int i=0; i<limit; i++ ) {

            nanoTime = System.nanoTime();
            
            if( nanoTime == lastNanoTime ) fail("Same nano time?");

            long diff = nanoTime - lastNanoTime;
            
            if( diff < 0 ) diff = -diff;
            
            if( diff < minDiff ) minDiff = diff;
            
            lastNanoTime = nanoTime;
            
        }
        
        System.err.println("Minimum difference in nanos is " + minDiff
                + " over " + limit + " trials");
        
    }
    
    /**
     * Test verifies that nano times are always distinct from the last generated
     * nanos time (as assigned by {@link RootBlockView#nextNanoTime()}.
     */
    public void test_nextNanoTime2() {

        final int limit = 1000000;
        
        long lastNanoTime = System.nanoTime() - 1;
        long nanoTime;
        long minDiff = Long.MAX_VALUE;
        
        for( int i=0; i<limit; i++ ) {

            nanoTime = TimestampFactory.nextNanoTime();
            
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
