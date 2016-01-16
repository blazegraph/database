/**

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
 * Created on Aug 17, 2011
 */

package com.bigdata.rwstore.sector;

import com.bigdata.io.DirectBufferPool;

import junit.framework.TestCase2;

/**
 * Some additional tests for the {@link MemoryManager} which look at the effect
 * of the #of buffers it is allowed to allocate.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMemoryManager2 extends TestCase2 {

    /**
     * 
     */
    public TestMemoryManager2() {
    }

    /**
     * @param name
     */
    public TestMemoryManager2(String name) {
        super(name);
    }

    /**
     * Verify that nsectors must be positive.
     */
    public void test_maxBuffers0() {

        MemoryManager mmgr = null;
        try {

            mmgr = new MemoryManager(DirectBufferPool.INSTANCE, 0/* nsectors */);

            fail("Expecting : " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        } finally {

            if (mmgr != null)
                mmgr.clear();

        }

    }
    
    /**
     * Verify that nsectors must be positive.
     */
    public void test_maxBuffersNegative() {
        
        MemoryManager mmgr = null;
        try {

            mmgr = new MemoryManager(DirectBufferPool.INSTANCE, -1/* nsectors */);

            fail("Expecting : " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        } finally {

            if (mmgr != null)
                mmgr.clear();

        }
        
    }
 
    /**
     * Verify that pool must not be <code>null</code>.
     */
    public void test_pool_is_null() {

        MemoryManager mmgr = null;
        try {

            mmgr = new MemoryManager(null/* pool */, 1/* nsectors */);

            fail("Expecting : " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        } finally {

            if (mmgr != null)
                mmgr.clear();

        }

    }
    
}
