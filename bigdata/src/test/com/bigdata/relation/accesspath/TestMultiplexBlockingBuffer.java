/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Oct 8, 2010
 */

package com.bigdata.relation.accesspath;

import junit.framework.TestCase2;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Test suite for {@link MultiplexBlockingBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMultiplexBlockingBuffer extends TestCase2 {

    /**
     * 
     */
    public TestMultiplexBlockingBuffer() {

    }

    /**
     * @param name
     */
    public TestMultiplexBlockingBuffer(String name) {
        super(name);

    }

    public void test_multiplex() {
        
        final IBlockingBuffer<String> buffer = new BlockingBuffer<String>();
        
        final MultiplexBlockingBuffer<String> multiplex = new MultiplexBlockingBuffer<String>(buffer);

        // buffer is open and empty.
        assertTrue(buffer.isOpen());
        assertTrue(buffer.isEmpty());
        
        // multiplex is open.
        assertTrue(multiplex.isOpen());
        
        final IBlockingBuffer<String> skin1 = multiplex.newInstance();

        final IBlockingBuffer<String> skin2 = multiplex.newInstance();

        // buffer is open and empty.
        assertTrue(buffer.isOpen());
        assertTrue(buffer.isEmpty());
        
        // multiplex is open.
        assertTrue(multiplex.isOpen());

        skin1.add("a");
        skin1.flush();
        skin1.close();
        try {
            skin1.add("a2");
            fail("Expecting: " + BufferClosedException.class);
        } catch (BufferClosedException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // buffer is open but no longer empty.
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());

        // multiplex is open.
        assertTrue(multiplex.isOpen());

        skin2.add("b");
        skin2.add("c");
        skin2.flush();

        // buffer is open but not empty.
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());

        // multiplex is open.
        assertTrue(multiplex.isOpen());

        // close the last open skin.
        skin2.close();
        
        // buffer is closed but not empty.
        assertFalse(buffer.isOpen());
        assertFalse(buffer.isEmpty());

        // multiplex closed.
        assertFalse(multiplex.isOpen());

        // verify the data.
        assertSameIterator(new String[]{"a","b","c"}, buffer.iterator());
        
    }

}
