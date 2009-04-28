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
 * Created on Sep 10, 2008
 */

package com.bigdata.relation.accesspath;

import junit.framework.TestCase2;

/**
 * Test suite for {@link BlockingBuffer} and its {@link IAsynchronousIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo this should have a more extensive test suite since we use it all over.
 */
public class TestBlockingBufferWithChunks extends TestCase2 {

    /**
     * 
     */
    public TestBlockingBufferWithChunks() {
    }

    /**
     * @param arg0
     */
    public TestBlockingBufferWithChunks(String arg0) {
        super(arg0);
    }

//    /**
//     * Basic test of the ability to add to a buffer with a fixed capacity queue
//     * and to drain the elements from the queue.  
//     *
//     */
//    public void test_blockingBuffer() {
//
//        final BlockingBuffer<Object> buffer = new BlockingBuffer<Object>(3/* capacity */); 
//        
//        
//        
//    }
//    
    public void test_chunkCombinerNoticesCloseWithInfiniteTimeout() {
        
        fail("write tests");
        
    }
    
}
